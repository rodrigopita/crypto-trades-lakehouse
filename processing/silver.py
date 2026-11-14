from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import bronze
from .settings import ProcessingSettings

LAYER = "silver"
TRADES_TABLE = "trades"
REJECTS_TABLE = "trade_rejects"


def start_silver_queries(spark: SparkSession, settings: ProcessingSettings):
    trades_table_path = settings.table_path(LAYER, TRADES_TABLE)
    rejects_table_path = settings.table_path(LAYER, REJECTS_TABLE)
    trades_query_checkpoint_path = settings.query_checkpoint_path(LAYER, TRADES_TABLE)
    rejects_query_checkpoint_path = settings.query_checkpoint_path(LAYER, REJECTS_TABLE)

    bronze_stream = spark.readStream.format("delta").load(
        settings.table_path(bronze.LAYER, bronze.TABLE)
    )

    trades = bronze_stream.select(F.explode("data").alias("trade"), "received_time").select(
        "trade",
        F.col("received_time").try_cast("timestamp").alias("received_time"),
        F.col("trade.symbol"),
        F.col("trade.side"),
        F.col("trade.price").try_cast("double").alias("price"),
        F.col("trade.qty").try_cast("double").alias("qty"),
        F.col("trade.ord_type"),
        F.col("trade.trade_id").try_cast("bigint").alias("trade_id"),
        F.col("trade.timestamp").try_cast("timestamp").alias("event_time"),
    )

    reject_reason = (
        F.when(F.col("trade.trade_id").isNull(), "missing trade_id")
        .when(F.col("trade_id").isNull(), "malformed trade_id")
        .when(F.col("trade.price").isNull(), "missing price")
        .when(F.col("price").isNull(), "malformed price")
        .when(F.col("price").isNaN() | (F.col("price") == float("inf")), "malformed price")
        .when(F.col("price") <= 0, "price not positive")
        .when(F.col("trade.qty").isNull(), "missing qty")
        .when(F.col("qty").isNull(), "malformed qty")
        .when(F.col("qty").isNaN() | (F.col("qty") == float("inf")), "malformed qty")
        .when(F.col("qty") <= 0, "qty not positive")
        .when(F.col("trade.timestamp").isNull(), "missing timestamp")
        .when(F.col("event_time").isNull(), "malformed timestamp")
        .when(
            F.col("event_time") > F.col("received_time") + F.expr("INTERVAL 1 MINUTE"),
            "timestamp in the future",
        )
    )
    checked = trades.withColumn("reject_reason", reject_reason)
    deduplicated = (
        checked.filter(F.col("reject_reason").isNull())
        .withWatermark("event_time", settings.watermark_threshold)
        .dropDuplicatesWithinWatermark(["trade_id"])
    )

    deduplicated.select(
        "symbol",
        "side",
        "price",
        "qty",
        "ord_type",
        "trade_id",
        "event_time",
        "received_time",
    ).writeStream.outputMode("append").format("delta").option(
        "checkpointLocation", trades_query_checkpoint_path
    ).start(trades_table_path)

    checked.filter(F.col("reject_reason").isNotNull()).select(
        "trade",
        "received_time",
        "reject_reason",
    ).writeStream.outputMode("append").format("delta").option(
        "checkpointLocation", rejects_query_checkpoint_path
    ).start(rejects_table_path)
