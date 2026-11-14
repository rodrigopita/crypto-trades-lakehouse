import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.settings import ProcessingSettings
from processing.silver import LAYER, REJECTS_TABLE, TRADES_TABLE
from reconciliation.bronze import get_bronze_counts
from reconciliation.checks import ReconciliationError
from shared.logs import configure_logging

logger = logging.getLogger(__name__)


def get_silver_counts(spark: SparkSession, settings: ProcessingSettings) -> dict:
    trades = spark.read.format("delta").load(settings.table_path(LAYER, TRADES_TABLE))

    trade_counts = trades.agg(
        F.count("*").alias("trade_count"),
        F.count_distinct("trade_id").alias("distinct_trade_count"),
    ).first()
    return trade_counts.asDict()


def get_reject_counts(spark: SparkSession, settings: ProcessingSettings) -> dict:
    rejects = spark.read.format("delta").load(settings.table_path(LAYER, REJECTS_TABLE))

    by_reason = rejects.groupBy("reject_reason").count().collect()
    return {row["reject_reason"]: row["count"] for row in by_reason}


def count_dropped(bronze_counts: dict, silver_counts: dict, reject_counts: dict) -> int:
    return bronze_counts["trade_count"] - silver_counts["trade_count"] - sum(reject_counts.values())


def compare_counts(bronze_counts: dict, silver_counts: dict, dropped: int) -> list:
    bronze_duplicates = bronze_counts["trade_id_count"] - bronze_counts["distinct_trade_count"]
    checks = [
        ("dropped vs duplicates in bronze", dropped, bronze_duplicates),
        (
            "silver trades vs distinct silver ids",
            silver_counts["trade_count"],
            silver_counts["distinct_trade_count"],
        ),
    ]
    failed = []
    for name, left, right in checks:
        matches = left == right
        logger.info(f"{name}: {left} vs {right}, matches: {matches}.")
        if not matches:
            failed.append(name)
    return failed


def log_reject_metrics(reject_counts: dict, dropped: int) -> None:
    for reason, count in sorted(reject_counts.items()):
        logger.info(f"Rejected, {reason}: {count}.")
    logger.info(f"Duplicates dropped: {dropped}.")


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("SilverReconciliation").getOrCreate()
    bronze_counts = get_bronze_counts(spark, settings)
    silver_counts = get_silver_counts(spark, settings)
    reject_counts = get_reject_counts(spark, settings)
    dropped = count_dropped(bronze_counts, silver_counts, reject_counts)

    failed = compare_counts(bronze_counts, silver_counts, dropped)
    log_reject_metrics(reject_counts, dropped)
    if failed:
        raise ReconciliationError(f"Mismatch on {failed}.")


if __name__ == "__main__":
    main()
