import logging
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from processing import gold, silver
from processing.settings import ProcessingSettings
from shared.jsonl import append_jsonl
from shared.logs import configure_logging

logger = logging.getLogger(__name__)

TABLES = [
    (silver.LAYER, silver.TRADES_TABLE),
    (gold.LAYER, gold.CANDLES_TABLE),
]


def get_file_counts(spark: SparkSession, table_path: str) -> dict:
    snapshot_counts = DeltaTable.forPath(spark, table_path).detail().first()
    files_on_disk = len(list(Path(table_path).glob("*.parquet")))
    return {
        "num_files": snapshot_counts["numFiles"],
        "size_in_bytes": snapshot_counts["sizeInBytes"],
        "files_on_disk": files_on_disk,
    }


def compact(spark: SparkSession, table_path: str) -> dict:
    metrics = DeltaTable.forPath(spark, table_path).optimize().executeCompaction().first().metrics
    return {"files_removed": metrics.numFilesRemoved, "files_added": metrics.numFilesAdded}


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("OptimizeFlow").getOrCreate()

    ran_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    for layer, table in TABLES:
        path = settings.table_path(layer, table)
        before = get_file_counts(spark, path)
        compaction = compact(spark, path)
        after = get_file_counts(spark, path)
        logger.info(
            f"{layer}/{table}: {before['num_files']} -> {after['num_files']} files in the snapshot "
            f"({before['files_on_disk']} -> {after['files_on_disk']} on disk), "
            f"{compaction['files_removed']} removed, {compaction['files_added']} added, "
            f"{before['size_in_bytes']} -> {after['size_in_bytes']} bytes."
        )
        append_jsonl(
            settings.optimize_metrics_path(),
            {"ran_at": ran_at, "table": f"{layer}/{table}", "before": before, "after": after}
            | compaction,
        )
    logger.info(f"Results appended to {settings.optimize_metrics_path()}.")


if __name__ == "__main__":
    main()
