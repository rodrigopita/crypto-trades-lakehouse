import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(moment: datetime) -> str:
    return moment.isoformat(timespec="microseconds").replace("+00:00", "Z")


class TradesBatch:
    def __init__(
        self,
        landing_folder: str,
        tmp_folder: str,
        record_folder: str,
        max_age_seconds: int = 60,
        max_messages: int = 1000,
        clock: Clock = utc_now,
    ):
        self.landing_folder = landing_folder
        self.tmp_folder = tmp_folder
        self.record_folder = record_folder
        self.max_age = timedelta(seconds=max_age_seconds)
        self.max_messages = max_messages
        self.messages = []
        self._clock = clock

        self.opened_at = None

        os.makedirs(self.tmp_folder, exist_ok=True)
        os.makedirs(self.landing_folder, exist_ok=True)
        os.makedirs(self.record_folder, exist_ok=True)

    def add(self, message: dict):
        if not self.messages:
            self.opened_at = self._clock()
        self.messages.append(message)

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
        landed_file = landing_path(ref_opened_at, self.landing_folder)
        os.replace(tmp_file, landed_file)
        log_written_record(
            iso_utc(self.opened_at),
            iso_utc(self._clock()),
            os.path.basename(landed_file),
            self.messages,
            self.record_folder,
        )
        self.messages = []


def landing_path(ref_opened_at: str, dst_folder: str) -> str:
    return f"{dst_folder}/{ref_opened_at}-trades.jsonl"


def save_to_temp(ref_opened_at: str, trade_messages: list[dict], tmp_folder: str) -> str:
    filename = landing_path(ref_opened_at, tmp_folder)
    with open(filename, "w") as f:
        for trade_message in trade_messages:
            message = json.dumps(trade_message) + "\n"
            f.write(message)
    return filename


def log_written_record(
    ref_opened_at: str,
    ref_written_at: str,
    filename: str,
    trade_messages: list[dict],
    record_folder: str,
):
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
    record_path = f"{record_folder}/written.jsonl"
    with open(record_path, "a") as f:
        f.write(json.dumps(written_record) + "\n")
