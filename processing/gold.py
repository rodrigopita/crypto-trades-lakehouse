from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import silver
from .settings import ProcessingSettings

LAYER = "gold"
CANDLES_TABLE = "candles"


def start_gold_query(spark: SparkSession, settings: ProcessingSettings):
    candles_table_path = settings.table_path(LAYER, CANDLES_TABLE)
    candles_query_checkpoint_path = settings.query_checkpoint_path(LAYER, CANDLES_TABLE)

    silver_stream = spark.readStream.format("delta").load(
        settings.table_path(silver.LAYER, silver.TRADES_TABLE)
    )

    candles = (
        silver_stream.withWatermark("event_time", settings.gold_watermark_threshold)
        .groupBy(F.window("event_time", "1 minute"), "symbol")
        .agg(
            F.min_by("price", F.struct("event_time", "trade_id")).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", F.struct("event_time", "trade_id")).alias("close"),
            (F.sum(F.col("price") * F.col("qty")) / F.sum("qty")).alias("vwap"),
            F.sum("qty").alias("volume"),
        )
    )

    candles.writeStream.outputMode("append").format("delta").option(
        "checkpointLocation", candles_query_checkpoint_path
    ).start(candles_table_path)
