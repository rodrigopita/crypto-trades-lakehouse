import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.bronze import LAYER, TABLE
from processing.settings import ProcessingSettings
from reconciliation.checks import ReconciliationError
from shared.logs import configure_logging

logger = logging.getLogger(__name__)

RECORD_SCHEMA = (
    "opened_at STRING, "
    "written_at STRING, "
    "filename STRING, "
    "message_count INT, "
    "trade_count INT, "
    "min_trade_id BIGINT, "
    "max_trade_id BIGINT"
)


def get_bronze_counts(spark: SparkSession, settings: ProcessingSettings) -> dict:
    bronze_df = spark.read.format("delta").load(settings.table_path(LAYER, TABLE))
    trades = bronze_df.select(F.explode("data").alias("trade")).select(
        F.col("trade.trade_id").try_cast("bigint").alias("trade_id")
    )

    trade_counts = trades.agg(
        F.count("*").alias("trade_count"),
        F.count_distinct("trade_id").alias("distinct_trade_count"),
        F.count("trade_id").alias("trade_id_count"),
        F.min("trade_id").alias("min_trade_id"),
        F.max("trade_id").alias("max_trade_id"),
    ).first()
    return {"message_count": bronze_df.count(), **trade_counts.asDict()}


def get_record_log_counts(spark: SparkSession, settings: ProcessingSettings) -> dict:
    record_log_df = spark.read.format("json").schema(RECORD_SCHEMA).load(settings.record_path())

    totals = record_log_df.agg(
        F.sum("message_count").alias("message_count"),
        F.sum("trade_count").alias("trade_count"),
        F.min("min_trade_id").alias("min_trade_id"),
        F.max("max_trade_id").alias("max_trade_id"),
    ).first()
    return totals.asDict()


def compare_counts(bronze_counts: dict, record_log_counts: dict) -> list:
    keys_with_diff = []
    for key, value in record_log_counts.items():
        matches = bronze_counts[key] == value
        logger.info(f"{key}: bronze {bronze_counts[key]}, record {value}, matches: {matches}.")
        if not matches:
            keys_with_diff.append(key)
    return keys_with_diff


def log_trade_id_coverage(bronze_counts: dict) -> None:
    if bronze_counts["min_trade_id"] is None:
        logger.info("Bronze has no trades; no trade id coverage to report.")
        return
    distinct = bronze_counts["distinct_trade_count"]
    repeated = bronze_counts["trade_id_count"] - distinct
    id_range = bronze_counts["max_trade_id"] - bronze_counts["min_trade_id"] + 1
    missing = id_range - distinct
    logger.info(f"Distinct trade ids: {distinct}, repeated: {repeated} (snapshot replays).")
    logger.info(f"Missing trade ids: {missing} of {id_range} in range (event gaps).")


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("BronzeReconciliation").getOrCreate()
    bronze_counts = get_bronze_counts(spark, settings)
    record_log_counts = get_record_log_counts(spark, settings)

    keys_with_diff = compare_counts(bronze_counts, record_log_counts)
    log_trade_id_coverage(bronze_counts)
    if keys_with_diff:
        raise ReconciliationError(f"Mismatch on {keys_with_diff}.")


if __name__ == "__main__":
    main()
