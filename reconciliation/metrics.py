import json
import logging
import math
import os
import statistics
from collections import defaultdict
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing import bronze, gold, silver
from processing.settings import ProcessingSettings
from reconciliation.bronze import get_bronze_counts
from reconciliation.checks import ReconciliationError
from shared.logs import configure_logging

logger = logging.getLogger(__name__)

EPOCH = "1970-01-01T00:00:00.000Z"
TABLES = [
    (bronze.LAYER, bronze.TABLE),
    (silver.LAYER, silver.TRADES_TABLE),
    (silver.LAYER, silver.REJECTS_TABLE),
    (gold.LAYER, gold.CANDLES_TABLE),
]


def read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def parse_time(text: str) -> datetime:
    return datetime.fromisoformat(text)


def connection_summary(events: list[dict]) -> dict:
    """Reconnects, failed attempts and downtime: each disconnect to the next connect."""
    downtimes = []
    open_since = None
    for event in sorted(events, key=lambda e: e["time"]):
        if event["event"] == "disconnected" and open_since is None:
            open_since = parse_time(event["time"])
        elif event["event"] == "connected" and open_since is not None:
            downtimes.append((parse_time(event["time"]) - open_since).total_seconds())
            open_since = None
    return {
        "reconnects": sum(event["event"] == "disconnected" for event in events),
        "failed_attempts": sum(event["event"] == "connect_failed" for event in events),
        "downtime_total_seconds": round(sum(downtimes), 1),
        "downtime_max_seconds": round(max(downtimes, default=0), 1),
        "ended_disconnected": open_since is not None,
    }


def reports_by_query(progress: list[dict]) -> dict[str, list[dict]]:
    by_query = defaultdict(list)
    for report in progress:
        by_query[report["name"]].append(report)
    return {
        name: sorted(reports, key=lambda r: r["timestamp"]) for name, reports in by_query.items()
    }


def input_rows(reports: list[dict]) -> int:
    return sum(report["numInputRows"] for report in reports)


def operator_totals(reports: list[dict], operator_name: str) -> dict:
    totals = {"updated": 0, "dropped_by_watermark": 0, "dropped_duplicates": 0}
    for report in reports:
        for operator in report["stateOperators"]:
            if operator["operatorName"] != operator_name:
                continue
            totals["updated"] += operator.get("numRowsUpdated", 0)
            totals["dropped_by_watermark"] += operator.get("numRowsDroppedByWatermark", 0)
            totals["dropped_duplicates"] += operator.get("customMetrics", {}).get(
                "numDroppedDuplicateRows", 0
            )
    return totals


def rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def batch_latencies(reports: list[dict]) -> list[int]:
    """triggerExecution of batches with input rows, leaving out each run's first batch, whose
    time includes the run's startup."""
    first_of_run = {}
    for report in reports:
        first_of_run.setdefault(report["runId"], report["batchId"])
    return [
        report["durationMs"]["triggerExecution"]
        for report in reports
        if report["numInputRows"] and report["batchId"] != first_of_run[report["runId"]]
    ]


def watermark_lags(reports: list[dict]) -> list[float]:
    """Seconds from the watermark to the batch's start, for batches that have a watermark."""
    lags = []
    for report in reports:
        watermark = report.get("eventTime", {}).get("watermark")
        if not watermark or watermark == EPOCH:
            continue
        lags.append((parse_time(report["timestamp"]) - parse_time(watermark)).total_seconds())
    return lags


def summarize(values: list[float]) -> dict | None:
    """Count, median, 95th percentile (nearest rank) and max, or None with no values."""
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "median": statistics.median(ordered),
        "p95": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "max": ordered[-1],
    }


def get_output_rows(spark: SparkSession, table_path: str) -> int:
    """Rows the streaming writes added, from Delta's history. The empty-table WRITE at startup
    is left out."""
    total = (
        DeltaTable.forPath(spark, table_path)
        .history()
        .where(F.col("operation") == "STREAMING UPDATE")
        .agg(F.sum(F.col("operationMetrics")["numOutputRows"].cast("long")))
        .first()[0]
    )
    return total or 0


def missing_trade_ids(bronze_counts: dict) -> int:
    if bronze_counts["min_trade_id"] is None:
        return 0
    id_range = bronze_counts["max_trade_id"] - bronze_counts["min_trade_id"] + 1
    return id_range - bronze_counts["distinct_trade_count"]


def build_summary(
    progress: list[dict], connections: list[dict], output_rows: dict[str, int], missing_ids: int
) -> dict:
    by_query = reports_by_query(progress)
    dedup = operator_totals(by_query.get("silver_trades", []), "dedupeWithinWatermark")
    candles_state = operator_totals(by_query.get("gold_candles", []), "stateStoreSave")
    reached_dedup = dedup["updated"] + dedup["dropped_duplicates"] + dedup["dropped_by_watermark"]
    gold_input = input_rows(by_query.get("gold_candles", []))
    return {
        "connections": connection_summary(connections),
        "event_gaps": {"missing_trade_ids": missing_ids},
        "duplicates": {
            "dropped": dedup["dropped_duplicates"],
            "reached_dedup": reached_dedup,
            "rate": rate(dedup["dropped_duplicates"], reached_dedup),
        },
        "late_events": {
            "silver_dropped": dedup["dropped_by_watermark"],
            "silver_rate": rate(dedup["dropped_by_watermark"], reached_dedup),
            "gold_dropped": candles_state["dropped_by_watermark"],
            "gold_rate": rate(candles_state["dropped_by_watermark"], gold_input),
        },
        "dropped_by_quality_rules": output_rows.get(f"{silver.LAYER}/{silver.REJECTS_TABLE}", 0),
        "watermark_lag_seconds": {
            name: summarize(watermark_lags(by_query.get(name, [])))
            for name in ("silver_trades", "gold_candles")
        },
        "batch_latency_ms": {
            name: summarize(batch_latencies(reports)) for name, reports in by_query.items()
        },
        "input_rows": {name: input_rows(reports) for name, reports in by_query.items()},
        "output_rows": output_rows,
    }


def percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2%}"


def spread(stats: dict | None, unit: str) -> str:
    if stats is None:
        return "no batches"
    return (
        f"median {stats['median']:.1f}{unit}, p95 {stats['p95']:.1f}{unit}, "
        f"max {stats['max']:.1f}{unit} over {stats['count']} batches"
    )


def log_report(summary: dict) -> None:
    connections = summary["connections"]
    logger.info(
        f"Websocket reconnects: {connections['reconnects']} "
        f"({connections['failed_attempts']} failed attempts, "
        f"{connections['downtime_total_seconds']} s down, "
        f"longest {connections['downtime_max_seconds']} s)."
    )
    logger.info(f"Event gaps: {summary['event_gaps']['missing_trade_ids']} missing trade ids.")
    duplicates = summary["duplicates"]
    logger.info(
        f"Duplicate rate: {duplicates['dropped']} of {duplicates['reached_dedup']} trades "
        f"({percent(duplicates['rate'])})."
    )
    late = summary["late_events"]
    logger.info(
        f"Late-event rate: Silver {late['silver_dropped']} ({percent(late['silver_rate'])}), "
        f"Gold {late['gold_dropped']} ({percent(late['gold_rate'])})."
    )
    logger.info(f"Records dropped by quality rules: {summary['dropped_by_quality_rules']}.")
    for name, stats in summary["watermark_lag_seconds"].items():
        logger.info(f"Watermark lag, {name}: {spread(stats, ' s')}.")
    for name, stats in summary["batch_latency_ms"].items():
        logger.info(f"Micro-batch latency, {name}: {spread(stats, ' ms')}.")
    for name, count in summary["input_rows"].items():
        logger.info(f"Input rows, {name}: {count}.")
    for table, count in summary["output_rows"].items():
        logger.info(f"Output rows, {table}: {count}.")


def main():
    configure_logging()
    settings = ProcessingSettings()
    progress_metrics = read_jsonl(settings.progress_metrics_path())
    connections_metrics = read_jsonl(settings.connections_metrics_path())

    if not progress_metrics:
        raise ReconciliationError(
            f"No progress reports in {settings.progress_metrics_path()}; run a session first."
        )

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("MetricsReconciliation").getOrCreate()
    output_rows = {
        f"{layer}/{table}": get_output_rows(spark, settings.table_path(layer, table))
        for layer, table in TABLES
    }
    missing_ids = missing_trade_ids(get_bronze_counts(spark, settings))

    summary = build_summary(progress_metrics, connections_metrics, output_rows, missing_ids)
    log_report(summary)
    with open(settings.summary_metrics_path(), "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Summary written to {settings.summary_metrics_path()}.")


if __name__ == "__main__":
    main()
