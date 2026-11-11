import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from .settings import ProcessingSettings

logger = logging.getLogger(__name__)


LAYER = "bronze"
TABLE = "trade_messages"
TRADE_SCHEMA = (
    "STRUCT<symbol STRING, "
    "side STRING, "
    "price STRING, "
    "qty STRING, "
    "ord_type STRING, "
    "trade_id STRING, "
    "timestamp STRING>"
)
LANDING_SCHEMA = f"channel STRING, type STRING, data ARRAY<{TRADE_SCHEMA}>, received_time STRING"


def start_bronze_query(spark: SparkSession, settings: ProcessingSettings) -> StreamingQuery:
    landing_folder = settings.landing_folder()
    table_path = settings.table_path(LAYER, TABLE)
    checkpoint_folder = settings.checkpoint_path(LAYER, TABLE)

    os.makedirs(landing_folder, exist_ok=True)
    spark.createDataFrame([], LANDING_SCHEMA).write.format("delta").mode("ignore").save(table_path)

    landing_stream = spark.readStream.format("json").schema(LANDING_SCHEMA).load(landing_folder)
    query = (
        landing_stream.writeStream.outputMode("append")
        .format("delta")
        .option("checkpointLocation", checkpoint_folder)
        .start(table_path)
    )
    return query
