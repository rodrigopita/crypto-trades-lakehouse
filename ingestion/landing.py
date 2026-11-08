import json
import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

Clock = Callable[[], datetime]
logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(moment: datetime) -> str:
    return moment.isoformat(timespec="microseconds").replace("+00:00", "Z")


class TradesBatch:
    def __init__(
        self,
        landing_folder: str,
        tmp_folder: str,
        record_path: str,
        max_age_seconds: int = 60,
        max_messages: int = 1000,
        clock: Clock = utc_now,
    ):
        self.landing_folder = landing_folder
        self.tmp_folder = tmp_folder
        self.record_path = record_path
        self.max_age = timedelta(seconds=max_age_seconds)
        self.max_messages = max_messages
        self.messages = []
        self._clock = clock

        self.opened_at = None

        os.makedirs(self.tmp_folder, exist_ok=True)
        os.makedirs(self.landing_folder, exist_ok=True)
        os.makedirs(os.path.dirname(self.record_path), exist_ok=True)

    def add(self, message: dict):
        if not self.messages:
            self.opened_at = self._clock()
            logger.info(f"Opened new batch at {iso_utc(self.opened_at)}.")
        self.messages.append(message)
        logger.debug(f"Added message to batch. Total messages: {len(self.messages)}.")

    def is_due(self) -> bool:
        if self.messages:
            return (
                len(self.messages) >= self.max_messages
                or self._clock() - self.opened_at >= self.max_age
            )
        return False

    def flush(self):
        if not self.messages:
            return

        ref_opened_at = self.opened_at.strftime("%Y%m%dT%H%M%S%fZ")
        tmp_file = save_to_temp(ref_opened_at, self.messages, self.tmp_folder)
        landed_file = batch_file_path(ref_opened_at, self.landing_folder)
        os.replace(tmp_file, landed_file)
        logger.debug(f"Flushed batch to landing file: {landed_file}.")

        written_record = log_written_record(
            iso_utc(self.opened_at),
            iso_utc(self._clock()),
            os.path.basename(landed_file),
            self.messages,
            self.record_path,
        )
        self.messages = []
        logger.info(
            f"Cleared batch after flushing at {written_record['written_at']} with"
            f" {written_record['message_count']} messages and {written_record['trade_count']}"
            " trades."
        )


def batch_file_path(ref_opened_at: str, dst_folder: str) -> str:
    return f"{dst_folder}/{ref_opened_at}-trades.jsonl"


def save_to_temp(ref_opened_at: str, trade_messages: list[dict], tmp_folder: str) -> str:
    filename = batch_file_path(ref_opened_at, tmp_folder)
    with open(filename, "w") as f:
        for trade_message in trade_messages:
            message = json.dumps(trade_message) + "\n"
            f.write(message)
    logger.debug(f"Saved {len(trade_messages)} messages to temporary file: {filename}.")
    return filename


def log_written_record(
    ref_opened_at: str,
    ref_written_at: str,
    filename: str,
    trade_messages: list[dict],
    record_path: str,
) -> dict:
    trade_ids = [trade["trade_id"] for msg in trade_messages for trade in msg.get("data", [])]
    written_record = {
        "opened_at": ref_opened_at,
        "written_at": ref_written_at,
        "filename": filename,
        "message_count": len(trade_messages),
        "trade_count": len(trade_ids),
        "min_trade_id": min(trade_ids, default=None),
        "max_trade_id": max(trade_ids, default=None),
    }
    with open(record_path, "a") as f:
        f.write(json.dumps(written_record) + "\n")
    logger.debug(f"Logged written record to {record_path}: {written_record}.")
    return written_record
