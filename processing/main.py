import logging

from pyspark.sql import SparkSession

from shared.logs import configure_logging

from .bronze import start_bronze_query
from .settings import ProcessingSettings
from .silver import start_silver_queries

logger = logging.getLogger(__name__)


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("CryptoTradesLakehouse").getOrCreate()

    start_bronze_query(spark, settings)
    start_silver_queries(spark, settings)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
