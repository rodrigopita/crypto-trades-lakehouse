import json
import logging
import signal
import time
from collections.abc import Callable

from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.sync.client import ClientConnection, connect

from .landing import TradesBatch, iso_utc, utc_now
from .settings import IngesterSettings

logger = logging.getLogger(__name__)


class KrakenError(Exception):
    """Base class for failures while streaming trades from Kraken."""


class SubscriptionPermanentError(KrakenError):
    """A rejected subscription is permanent. Reconnecting sends the same
    request and gets the same answer.
    """


class Backoff:
    def __init__(self, cap_seconds: int = 60, sleeper: Callable[[float], None] = time.sleep):
        self.attempt = 0
        self.cap_seconds = cap_seconds
        self._sleep = sleeper

    def reset(self) -> None:
        self.attempt = 0

    def wait(self) -> None:
        self._sleep(min(2**self.attempt, self.cap_seconds))
        self.attempt += 1


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


def receive_trades(
    websocket: ClientConnection, trades_batch: TradesBatch, stall_timeout: int, backoff: Backoff
):
    while True:
        message = websocket.recv(timeout=stall_timeout)
        received_time = iso_utc(utc_now())
        trade_message = get_trade_message(message, received_time)
        if trades_batch.is_due():
            logger.info("Batch is due. Flushing messages...")
            trades_batch.flush()
        if not trade_message:
            continue
        backoff.reset()
        trades_batch.add(trade_message)


def ingest_with_reconnects(settings: IngesterSettings, trades_batch: TradesBatch):
    backoff = Backoff()
    while True:
        try:
            with connect(uri=settings.uri, close_timeout=settings.close_timeout) as websocket:
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
                receive_trades(websocket, trades_batch, settings.stall_timeout, backoff)
        except (TimeoutError, ConnectionClosed, InvalidHandshake, OSError) as exc:
            logger.warning("%s: %s. Reconnecting...", type(exc).__name__, exc)
            trades_batch.flush()
            backoff.wait()


def main():
    signal.signal(signal.SIGTERM, _stop)
    logging.basicConfig(
        format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    settings = IngesterSettings()
    trades_batch = TradesBatch(
        settings.landing_folder(),
        settings.tmp_folder(),
        settings.record_path(),
    )

    try:
        ingest_with_reconnects(settings, trades_batch)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        logger.info("Stopping, flushing the open batch...")
        trades_batch.flush()


if __name__ == "__main__":
    main()
