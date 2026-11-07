import logging

from pyspark.sql import SparkSession, StreamingQuery

from .settings import Settings

logger = logging.getLogger(__name__)


LAYER = "bronze"
TABLE = "trade_messages"
TRADE_SCHEMA = (
    "STRUCT<symbol STRING, "
    "side STRING, "
    "price DOUBLE, "
    "qty DOUBLE, "
    "ord_type STRING, "
    "trade_id BIGINT, "
    "timestamp STRING>"
)
LANDING_SCHEMA = f"channel STRING, type STRING, data ARRAY<{TRADE_SCHEMA}>, received_time STRING"


def start_bronze_query(spark: SparkSession, settings: Settings) -> StreamingQuery:
    landing_folder = settings.landing_path()
    table_path = settings.table_path(LAYER, TABLE)
    checkpoint_folder = settings.checkpoint_path(LAYER, TABLE)

    landing_stream = spark.readStream.format("json").schema(LANDING_SCHEMA).load(landing_folder)
    query = (
        landing_stream.writeStream.outputMode("append")
        .format("delta")
        .option("checkpointLocation", checkpoint_folder)
        .start(table_path)
    )
    return query
