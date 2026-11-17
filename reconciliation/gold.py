import logging
from datetime import UTC, datetime

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.gold import CANDLES_TABLE, LAYER
from processing.settings import ProcessingSettings
from reconciliation import silver
from reconciliation.checks import ReconciliationError
from shared.logs import configure_logging

logger = logging.getLogger(__name__)

BUCKETS = ("unchecked", "partial", "gap", "too_late", "impossible", "checked")
CANDLE_FIELDS = {  # field: (index in Kraken's row, tolerance)
    "open": (1, 0),
    "high": (2, 0),
    "low": (3, 0),
    "close": (4, 0),
    "vwap": (5, 0.1),
    "volume": (6, 1e-9),
}


def get_gold_candles_per_minute(spark: SparkSession, settings: ProcessingSettings) -> dict:
    candles = spark.read.format("delta").load(settings.table_path(LAYER, CANDLES_TABLE))

    rows = candles.select(
        F.col("window.start").cast("long").alias("minute"),
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "volume",
        "trade_count",
    ).collect()
    return {row["minute"]: row.asDict() for row in rows}


def get_silver_counts_per_minute(spark: SparkSession, settings: ProcessingSettings) -> dict:
    trades = spark.read.format("delta").load(settings.table_path(silver.LAYER, silver.TRADES_TABLE))

    rows = (
        trades.groupBy(F.window("event_time", "1 minute"))
        .agg(F.count("*").alias("trade_count"))
        .select(F.col("window.start").cast("long").alias("minute"), "trade_count")
        .collect()
    )

    return {row["minute"]: row["trade_count"] for row in rows}


def get_kraken_ohlc(settings: ProcessingSettings) -> dict:
    url = f"{settings.kraken_base_url}/public/OHLC"
    params = {"pair": settings.asset_pair}

    response = requests.get(url, params=params, timeout=settings.request_timeout)
    response.raise_for_status()
    response_json = response.json()
    if response_json["error"]:
        raise requests.HTTPError(response_json["error"])
    result = response_json["result"]
    kraken_candles = {
        row[0]: row for row in result[settings.asset_pair] if row[0] <= result["last"]
    }
    return kraken_candles


def classify_minutes(silver_counts: dict, gold_candles: dict, kraken_ohlc: dict) -> dict:
    buckets = {name: [] for name in BUCKETS}
    min_silver_counts = min(silver_counts)

    for minute in sorted(gold_candles.keys()):
        if minute not in kraken_ohlc:
            buckets["unchecked"].append(minute)
            continue

        silver_count = silver_counts[minute]
        gold_count = gold_candles[minute]["trade_count"]
        kraken_count = kraken_ohlc[minute][-1]

        if kraken_count < silver_count or gold_count > silver_count:
            buckets["impossible"].append(minute)
        elif minute == min_silver_counts:
            buckets["partial"].append(minute)
        elif kraken_count > silver_count:
            buckets["gap"].append(minute)
        elif silver_count > gold_count:
            buckets["too_late"].append(minute)
        else:
            buckets["checked"].append(minute)

    return buckets


def compare_checked_minutes(minutes: list, gold_candles: dict, kraken_ohlc: dict) -> list:
    failed = []
    for minute in minutes:
        gold_candle = gold_candles[minute]
        kraken_candle = kraken_ohlc[minute]

        for field, value in CANDLE_FIELDS.items():
            index = value[0]
            tolerance = value[1]
            gold_value = gold_candle[field]
            kraken_value = float(kraken_candle[index])
            if abs(gold_value - kraken_value) > tolerance:
                failed.append((minute, field, gold_value, kraken_value))
    return failed


def format_minute(minute: int, pattern: str = "%H:%M") -> str:
    return datetime.fromtimestamp(minute, UTC).strftime(pattern)


def log_buckets(
    buckets: dict, silver_counts: dict, gold_candles: dict, kraken_ohlc: dict, failed: list
):
    logger.info(
        f"Gold candles: {len(gold_candles)}, from "
        f"{format_minute(min(gold_candles), '%Y-%m-%d %H:%M')} to "
        f"{format_minute(max(gold_candles), '%Y-%m-%d %H:%M')} UTC."
    )
    logger.info(
        f"Reference candles: {len(kraken_ohlc)} finished, from "
        f"{format_minute(min(kraken_ohlc), '%Y-%m-%d %H:%M')} to "
        f"{format_minute(max(kraken_ohlc), '%Y-%m-%d %H:%M')} UTC."
    )
    logger.info(f"Unchecked minutes (no finished reference candle): {len(buckets['unchecked'])}.")
    for minute in buckets["partial"]:
        logger.info(f"Partial minute (first of the session): {format_minute(minute)}.")

    missing = 0
    for minute in buckets["gap"]:
        kraken_count, silver_count = kraken_ohlc[minute][-1], silver_counts[minute]
        missing += kraken_count - silver_count
        logger.info(
            f"Gap {format_minute(minute)}: reference {kraken_count} trades, "
            f"silver {silver_count}, missing {kraken_count - silver_count}."
        )
    logger.info(f"Gap minutes: {len(buckets['gap'])}, trades missing: {missing}.")

    too_late = 0
    for minute in buckets["too_late"]:
        silver_count, gold_count = silver_counts[minute], gold_candles[minute]["trade_count"]
        too_late += silver_count - gold_count
        logger.info(
            f"Too late {format_minute(minute)}: silver {silver_count} trades, "
            f"gold {gold_count}, too late {silver_count - gold_count}."
        )
    logger.info(f"Too late minutes: {len(buckets['too_late'])}, trades too late: {too_late}.")

    for minute in buckets["impossible"]:
        logger.info(
            f"Impossible {format_minute(minute)}: reference {kraken_ohlc[minute][-1]} trades, "
            f"silver {silver_counts[minute]}, gold {gold_candles[minute]['trade_count']}."
        )
    logger.info(f"Impossible minutes: {len(buckets['impossible'])}.")

    mismatched = {minute for minute, *_ in failed}
    checked = len(buckets["checked"])
    logger.info(f"Checked minutes: {checked}, within tolerance: {checked - len(mismatched)}.")
    for minute, field, gold_value, kraken_value in failed:
        logger.info(
            f"Mismatch {format_minute(minute)} {field}: gold {gold_value}, "
            f"reference {kraken_value}, tolerance {CANDLE_FIELDS[field][1]}."
        )


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("GoldReconciliation").getOrCreate()
    silver_counts = get_silver_counts_per_minute(spark, settings)
    gold_candles = get_gold_candles_per_minute(spark, settings)
    if not gold_candles:
        logger.info("Gold has no final candles; nothing to compare with the reference candles.")
        return

    kraken_ohlc = get_kraken_ohlc(settings)

    buckets = classify_minutes(silver_counts, gold_candles, kraken_ohlc)
    failed = compare_checked_minutes(buckets["checked"], gold_candles, kraken_ohlc)
    log_buckets(buckets, silver_counts, gold_candles, kraken_ohlc, failed)

    if buckets["impossible"] or not buckets["checked"] or failed:
        raise ReconciliationError(
            f"Impossible minutes: {buckets['impossible']}, "
            f"checked minutes: {len(buckets["checked"])}, mismatches: {failed}."
        )


if __name__ == "__main__":
    main()
