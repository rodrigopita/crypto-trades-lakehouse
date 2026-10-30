import json
import logging
from datetime import UTC, datetime

from websockets.sync.client import connect

logger = logging.getLogger(__name__)

URI = "wss://ws.kraken.com/v2"


class KrakenError(Exception):
    """Base class for failures while streaming trades from Kraken."""


class KrakenRetryableError(KrakenError):
    """A dropped connection is retryable."""


class SubscriptionPermanentError(KrakenError):
    """A rejected subscription is permanent. Reconnecting sends the same
    request and gets the same answer.
    """


def get_trade_message(message: str, received_time: str) -> dict | None:
    parsed_message = json.loads(message)
    if parsed_message.get("channel") == "trade":
        parsed_message["received_time"] = received_time
        return parsed_message
    elif parsed_message.get("method") == "subscribe" and not parsed_message["success"]:
        raise SubscriptionPermanentError(parsed_message.get("error"))
    return None


def main():
    with connect(uri=URI) as websocket:
        print(f"Connected to {websocket}.")
        payload = json.dumps(
            {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": ["BTC/USD"],
                    "snapshot": True,
                },
            }
        )
        websocket.send(payload)
        for message in websocket:
            received_time = (
                datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
            )
            trade_message = get_trade_message(message, received_time)
            if not trade_message:
                continue
            print(json.dumps(trade_message, indent=4))


if __name__ == "__main__":
    main()
