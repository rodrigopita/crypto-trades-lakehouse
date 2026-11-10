import logging

from pyspark.sql import SparkSession

from shared.logs import configure_logging

from .bronze import start_bronze_query
from .settings import ProcessingSettings

logger = logging.getLogger(__name__)


def main():
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("CryptoTradesLakehouse").getOrCreate()

    bronze_query = start_bronze_query(spark, settings)

    bronze_query.awaitTermination()


if __name__ == "__main__":
    main()
