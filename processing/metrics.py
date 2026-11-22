import logging
import os

from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger(__name__)


class Listener(StreamingQueryListener):
    def __init__(self, metrics_path: str):
        super().__init__()
        self.name_by_id = {}
        self.metrics_path = metrics_path

        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)

    def onQueryStarted(self, event):
        self.name_by_id[event.id] = event.name
        logger.info(f"Query started: {event.name}")

    def onQueryProgress(self, event):
        with open(self.metrics_path, "a") as f:
            f.write(event.progress.json + "\n")

    def onQueryTerminated(self, event):
        if not event.exception:
            logger.info(f"Query terminated: {self.name_by_id[event.id]}")
        else:
            logger.warning(
                f"Query terminated: {self.name_by_id[event.id]}, event exception: {event.exception}"
            )
