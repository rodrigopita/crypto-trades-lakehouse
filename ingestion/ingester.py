import json
import logging
import signal

from websockets.sync.client import connect

from .landing import TradesBatch, iso_utc, utc_now
from .settings import Settings

logger = logging.getLogger(__name__)


class KrakenError(Exception):
    """Base class for failures while streaming trades from Kraken."""


class KrakenRetryableError(KrakenError):
    """A dropped connection is retryable."""


class SubscriptionPermanentError(KrakenError):
    """A rejected subscription is permanent. Reconnecting sends the same
    request and gets the same answer.
    """


def _stop(signum, frame):
    raise SystemExit(0)


def get_trade_message(message: str, received_time: str) -> dict | None:
    parsed_message = json.loads(message)
    if parsed_message.get("channel") == "trade":
        parsed_message["received_time"] = received_time
        return parsed_message
    elif parsed_message.get("method") == "subscribe" and not parsed_message["success"]:
        raise SubscriptionPermanentError(parsed_message.get("error"))
    return None


def main():
    signal.signal(signal.SIGTERM, _stop)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    settings = Settings()
    trades_batch = TradesBatch(
        settings.landing_folder,
        settings.tmp_folder,
        settings.record_folder,
    )

    with connect(uri=settings.uri) as websocket:
        logger.info(f"Connected to {settings.uri}.")
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
        try:
            for message in websocket:
                received_time = iso_utc(utc_now())
                trade_message = get_trade_message(message, received_time)
                if trades_batch.is_due():
                    logger.info("Batch is due. Flushing messages...")
                    trades_batch.flush()
                if not trade_message:
                    continue
                trades_batch.add(trade_message)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received.")
        finally:
            logger.info("Stopping, flushing the open batch...")
            trades_batch.flush()


if __name__ == "__main__":
    main()
