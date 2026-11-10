"""Land one file of bad trades for the cp2 session.

    uv run python -m scripts.inject_bad_trades

Run it from the repository root while the ingester is running. It builds three trades from the
newest landed trade, one per message:

- a negative price, with a trade_id no real trade will arrive with,
- a missing trade_id,
- a duplicate: the newest trade copied as it is.

The file goes through the ingester's own TradesBatch, so it lands like any other batch and gets
its line in the written-record log, and Bronze still reconciles.
"""

import json
import logging
import os
import sys
from datetime import datetime

from ingestion.landing import TradesBatch, iso_utc, utc_now
from shared.layout import DataLayout
from shared.logs import configure_logging

logger = logging.getLogger(__name__)


def lowest_recorded_trade_id(record_path: str) -> int:
    """The negative-price trade takes the id just below this one. Kraken used that id before the
    first recorded batch, so no real trade will arrive with it, and the id range gets no new gap.
    """
    with open(record_path) as f:
        records = [json.loads(line) for line in f]
    return min(record["min_trade_id"] for record in records if record["min_trade_id"] is not None)


def newest_trade(landing_folder: str) -> dict:
    landing_files = sorted(name for name in os.listdir(landing_folder) if name.endswith(".jsonl"))
    if not landing_files:
        sys.exit(f"No landing files in {landing_folder}; start the ingester first.")
    with open(os.path.join(landing_folder, landing_files[-1])) as f:
        last_message = json.loads(f.readlines()[-1])
    return last_message["data"][-1]


def trade_message(trade: dict) -> dict:
    return {
        "channel": "trade",
        "type": "update",
        "data": [trade],
        "received_time": iso_utc(utc_now()),
    }


def main():
    configure_logging()
    logging.getLogger("ingestion.landing").setLevel(logging.DEBUG)
    layout = DataLayout()

    source = newest_trade(layout.landing_folder())
    age = utc_now() - datetime.fromisoformat(source["timestamp"])
    now = iso_utc(utc_now())

    negative_price = source | {
        "price": -abs(source["price"]),
        "trade_id": lowest_recorded_trade_id(layout.record_path()) - 1,
        "timestamp": now,
    }
    missing_trade_id = {key: value for key, value in source.items() if key != "trade_id"}
    missing_trade_id["timestamp"] = now
    duplicate = dict(source)

    batch = TradesBatch(layout.landing_folder(), layout.tmp_folder(), layout.record_path())
    for trade in (negative_price, missing_trade_id, duplicate):
        batch.add(trade_message(trade))
    batch.flush()

    logger.info(f"Negative price: trade_id {negative_price['trade_id']}, timestamp {now}.")
    logger.info(f"Missing trade_id: timestamp {now}.")
    logger.info(
        f"Duplicate: trade_id {duplicate['trade_id']}, timestamp {duplicate['timestamp']}"
        f" ({age.total_seconds():.0f} s before the injection)."
    )


if __name__ == "__main__":
    main()
