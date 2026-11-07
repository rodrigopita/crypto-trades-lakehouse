import logging

from pyspark.sql import SparkSession

from .bronze import start_bronze_query
from .settings import Settings

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    settings = Settings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("CryptoTradesLakehouse").getOrCreate()

    bronze_query = start_bronze_query(spark, settings)

    bronze_query.awaitTermination()


if __name__ == "__main__":
    main()
