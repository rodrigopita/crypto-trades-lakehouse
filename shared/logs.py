import logging
import time

FORMAT = "%(levelname)s [%(asctime)s] %(name)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging() -> None:
    """Once per process, at the top of each entry point's main().

    Log times are UTC, like the containers' clock and the timestamps in landing files, also when
    a script runs on the host.
    """
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(format=FORMAT, datefmt=DATE_FORMAT, level=logging.INFO)
