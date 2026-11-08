import logging

from pyspark.sql import SparkSession

from processing.bronze import LAYER, TABLE
from processing.settings import ProcessingSettings

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = SparkSession.builder.appName("CryptoTradesLakehouse").getOrCreate()


if __name__ == "__main__":
    main()
