import json
import logging

from pyspark.sql.streaming import StreamingQueryListener

from shared.jsonl import append_jsonl

logger = logging.getLogger(__name__)


class Listener(StreamingQueryListener):
    def __init__(self, metrics_path: str):
        super().__init__()
        self.name_by_id = {}
        self.metrics_path = metrics_path

    def onQueryStarted(self, event):
        self.name_by_id[event.id] = event.name
        logger.info(f"Query started: {event.name}")

    def onQueryProgress(self, event):
        append_jsonl(self.metrics_path, json.loads(event.progress.json))

    def onQueryTerminated(self, event):
        if not event.exception:
            logger.info(f"Query terminated: {self.name_by_id[event.id]}")
        else:
            logger.warning(
                f"Query terminated: {self.name_by_id[event.id]}, event exception: {event.exception}"
            )
