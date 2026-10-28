# Crypto Trades Lakehouse

The language of a local lakehouse that turns Kraken's BTC/USD trade stream into one-minute candles. These terms are used in code, test names, docs and conversation. Synonyms this project does not use are listed under _Avoid_.

## Trades and the connection

**Trade**:
One execution on Kraken. Each one carries `symbol`, `side`, `price`, `qty`, `ord_type`, `trade_id` and `timestamp`. The `trade_id` identifies it within the pair.
_Avoid_: event, fill, tick

**Message**:
One JSON frame received over the WebSocket. Its `channel` field says what it is.

**Heartbeat**:
The automatic message from the WebSocket service to verify the connection is alive (Kraken's `{"channel": "heartbeat"}`). Not the ping frames built into the WebSocket protocol itself.
_Avoid_: ping, keepalive

**Stall**:
The connection stays open, but no messages arrive: no trades and no heartbeats. Unlike a disconnect, nothing tells the ingester the connection is dead, so it has to notice the silence. cp1 simulates one by pausing the container or blocking traffic.
_Avoid_: hang, freeze, timeout (the timeout is how a stall is detected, not the stall)

**Reconnect**:
The action to perform after the WebSocket service has disconnected or the ingester needs to drop the connection and start a new one (in case of a stall). A subscription lives on a connection, so after reconnecting, the ingester has to subscribe again.

## Time

**Event time**:
The timestamp of the actual event, when the trade happened. ("timestamp" in the Kraken response)
_Avoid_: trade time

**Received time**:
When the ingester received the message carrying a trade, by the ingester's clock. It is written into the landing file next to the trade.
_Avoid_: arrival time, ingest time

**Processing time**:
When a streaming query processes a trade, by Spark's clock. Bronze, Silver and Gold each process the same trade at a different processing time.
_Avoid_: real-time

## Arrival

**Out of order**:
A trade that reaches a query after a trade with a later event time. It was observed that Kraken sends trades in `trade_id` order, so out-of-order trades come from the pipeline itself, for example from several landing files read in one micro-batch.

**Late**:
A trade whose event time is behind the newest event time the query saw in an earlier micro-batch. How far behind is its lateness.
_Avoid_: delayed (received time minus event time is the ingest delay, a different measure)

**Too late**:
A late trade that arrives after its candle is final. Spark does not guarantee whether it is counted: the docs say it "may or may not get aggregated".
_Avoid_: dropped (not guaranteed)

**Duplicate**:
The same `trade_id` arriving twice.

**Gap**:
A missing `trade_id` in the sequence of ids. Easily spotted if a distance between subsequent ids is greater than 1.

**Watermark**:
A point in event time that trails the newest event time a query has seen by a fixed threshold. A candle whose minute ends before the watermark stops accepting trades. It moves only when newer trades arrive, not with the clock.
It tells which late trades are certain to be counted, not which ones are certain to be dropped.
_Avoid_: cutoff, deadline

## The pipeline

**Landing file**:
The file that has been written to `data/landing/` by the ingester (with heartbeats dropped and a field to record the received time) where it waits to be processed.

**Archived file**:
A landing file that has already been read by the Bronze query and moved to the `archive/` directory.

**Batch**:
The trades the ingester writes into one landing file.
_Avoid_: chunk; micro-batch (Spark's term, see below)

**Micro-batch**:
One round of work a streaming query runs: it reads the new input, processes it and commits the result.
_Avoid_: batch

**Milestone**:
One of the project's four stages, cp1 to cp4, each closed by an annotated tag (`cp1-ingest`, `cp2-quality`, `cp3-candles`, `cp4-operability`).
_Avoid_: checkpoint

**Query checkpoint**:
The folder under `data/checkpoints/<layer>/<table>/` where a streaming query records which input it has processed and the state it keeps, so a restart resumes where it stopped. Deleting a table means deleting its query checkpoint too.
_Avoid_: checkpoint

**Delta log checkpoint**:
A file Delta writes inside a table's `_delta_log/` that summarizes the log up to one version, so readers don't replay every commit.
_Avoid_: checkpoint

**Reject**:
A trade that didn't make it into Silver.

## Candles

**Candle**:
One row in Gold for one pair and one minute of event time: open, high, low, close, volume and VWAP of the trades in that minute. Open and close are the first and last trades by event time, with `trade_id` breaking ties.
_Avoid_: candlestick, bar, kline

**Final candle**:
A candle whose minute ends before the watermark, so the pipeline will no longer update it. What happens to a too-late trade for its minute is the late-data policy, decided at cp3.
_Avoid_: closed candle, complete candle

**VWAP**:
Stands for Volume Weighted Average Price. It is the sum of price × quantity over the minute's trades, divided by the sum of quantity.

## Reconciliation

**Reference candles**:
Kraken's REST candles (`/0/public/OHLC`) to check against Gold's candles (Kraken only serves the last 720 minutes).
_Avoid_: oracle, source of truth

**Tolerance**:
How far a Gold candle can be from its reference candle, for each field, and still count as matching.

**Reconciliation**:
Checking a layer against an independent record of the same data: at cp1, Bronze counts against the ingester's written-record log; at cp3, Gold candles against the reference candles.
_Avoid_: validation
