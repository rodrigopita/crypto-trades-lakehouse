import logging
import signal
import threading

from pyspark.sql import SparkSession

from shared.logs import configure_logging

from .bronze import start_bronze_query
from .gold import start_gold_query
from .settings import ProcessingSettings
from .silver import start_silver_queries

logger = logging.getLogger(__name__)

stop_requested = threading.Event()


def _stop(signum, frame):
    stop_requested.set()


def main():
    signal.signal(signal.SIGTERM, _stop)
    configure_logging()
    settings = ProcessingSettings()

    logger.info("Starting Spark session...")
    spark = (
        SparkSession.builder.appName("CryptoTradesLakehouse")
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .getOrCreate()
    )

    start_bronze_query(spark, settings)
    start_silver_queries(spark, settings)
    start_gold_query(spark, settings)

    while True:
        result = spark.streams.awaitAnyTermination(timeout=settings.await_termination_timeout)
        if stop_requested.is_set():
            logger.info("SIGTERM received. stopping...")
            break
        if result:
            logger.info(
                "A query stopped without an error. "
                f"Active queries: {[query.name for query in spark.streams.active]}"
            )
            break

    logger.info("Stopping all active queries...")
    for query in spark.streams.active:
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
