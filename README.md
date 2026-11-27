# crypto-trades-lakehouse

A local lakehouse that turns Kraken's live BTC/USD trades into one-minute candles, with Spark Structured Streaming and Delta Lake in Docker. It is a case study in event-time processing: late, duplicate and out-of-order trades, and when a candle counts as final.

## Pipeline

```
Kraken WebSocket v2, trade channel
  -> ingester (Python)      data/landing/*.jsonl, one file a minute
  -> Bronze                 bronze/trade_messages
  -> Silver                 silver/trades, silver/trade_rejects
  -> Gold                   gold/candles
```

| layer  | holds                                                                                                                                                                           |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bronze | every message as received, fields as strings                                                                                                                                    |
| Silver | typed trades. Rejects, with the reason, a missing or malformed `trade_id`, price, qty or timestamp, a price or qty that is not positive, and a timestamp more than 1 minute ahead of arrival. Drops duplicates by `trade_id` |
| Gold   | one row per minute: open, high, low, close, VWAP, volume, trade count                                                                                                           |

One Spark process runs the four streaming queries. The ingester is plain Python.

## Run

```sh
docker compose up -d            # the first candle is final about 2.5 minutes later
docker compose stop ingester    # then wait about 30 s, so Bronze reads its last file
docker compose stop processor
for r in bronze silver gold metrics; do
    docker compose run --rm --no-deps processor python -m reconciliation.$r
done
docker compose run --rm --no-deps processor python -m processing.optimize
```

- Containers run as uid 1000, so the host user that owns `data/` must be uid 1000.
- Start each session from an empty `data/`, keeping `data/.gitkeep`. The checks assume one session per `data/`.
- Run the Gold reconciliation within 12 hours of the session. Kraken's REST API serves only the last 720 one-minute candles.
- To test failures, `scripts/inject_failure.sh reset|stall <seconds>` breaks the ingester's connection, and `uv run python -m scripts.inject_bad_trades` lands two invalid trades and a duplicate.

## Event time and late data

Candles use each trade's own timestamp, not the time it arrived.

- **A candle is final once the watermark passes the end of its minute.** Gold groups trades into one-minute windows under a 1-minute watermark, which is the newest event time seen minus 1 minute. It writes each candle once, when final, and never changes it. In the cp4 session that came a median 90 s after the minute ended, 147 s at the 95th percentile.
- **A trade that arrives after its candle is final is too late,** and that candle never counts it. No session has had one, in 8,566 trades in cp2, 3,265 in cp3 and 6,139 in cp4.
- **Order inside a minute comes from the data.** Open and close are the first and last trades by event time, then trade id, whatever order they arrived in.
- **A reconnect replays up to 50 trades** in Kraken's subscribe snapshot. Silver drops the ones it already has, by `trade_id`, under a 5-minute watermark.
- **The cost is the end of a session.** Once the ingester stops, no newer trade moves the watermark, so the last one or two minutes never become final.

Alternatives considered:

| alternative                                                   | why not                                                                            |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| update output mode, rewriting a candle when a late trade arrives | Delta's streaming sink refuses it (`DELTA_UNSUPPORTED_OUTPUT_MODE`)            |
| `foreachBatch` with `MERGE`, correcting final candles         | it works, but no session has had a late trade to correct                         |
| a longer watermark, such as 5 minutes                         | every candle waits 4 more minutes, against lateness never observed               |
| backfilling gaps from Kraken's REST trades                    | backfilled trades reach Gold after their candles are final. Gaps are reported instead |

## Checks

| module                   | compares                                                                                       | cp4 session                                         |
| ------------------------ | ---------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `reconciliation.bronze`  | Bronze against the ingester's written-record log                                               | 6,148 trades in both; 90 trade ids missing, from the reset |
| `reconciliation.silver`  | Silver against Bronze, through rejects and duplicates                                          | 6,139 = 6,148 − 2 rejects − 7 duplicates             |
| `reconciliation.gold`    | Gold against Kraken's REST candles. OHLC exact, volume within 1e-9, VWAP within 0.1, since Kraken cuts VWAP to one decimal | 31 checked minutes, all within tolerance; 1 partial and 2 gap minutes |
| `reconciliation.metrics` | the metrics below, from query progress, connection events and Delta's history                  | written to `data/metrics/summary.json`              |

None of them writes to a table, so a second run prints the same lines. Gold's volume is compared with Kraken's in every checked minute; no other volume comparison, such as buy against sell volume, is built.

## Metrics

From the cp4 session, 35 minutes with a 60 s reset, a 20 s stall, a processor restart and injected bad trades.

| metric                   | value                                                   |
| ------------------------ | ------------------------------------------------------- |
| WebSocket reconnects     | 2, 76.9 s down                                          |
| event gaps               | 90 missing trade ids                                    |
| duplicate rate           | 7 of 6,146 trades, 0.11%                                |
| late-event rate          | 0 trades behind Silver's watermark; 0 Gold windows with too-late trades |
| dropped by quality rules | 2                                                       |
| watermark lag, Gold      | median 73.6 s, 95th percentile 133.8 s                  |
| candle finality delay    | median 90.3 s, 95th percentile 147.3 s                  |
| micro-batch latency      | median 1.5 to 2.7 s per query                           |
| input and output         | 2,765 messages in Bronze, 6,139 trades in Silver, 34 candles in Gold |

Watermark lag is a micro-batch's start time minus its watermark. Candle finality delay runs from a minute's end to the end of the micro-batch whose watermark passed it. Latency is a micro-batch's `triggerExecution`, over micro-batches with input, leaving out each run's first one, which includes startup. Gold's late-event count is per window, not per trade, because Spark counts too-late input after partial aggregation.

## Small files

Bronze archives each landing file once the next micro-batch with new data commits, with the file source's `cleanSource` option, so landing keeps the newest file it read. cp3 left 40 files in landing. cp4 left 1 and archived 34.

For the Delta tables, `processing.optimize` ran one `OPTIMIZE` pass after the cp4 session:

| table           | files in the snapshot | files on disk | bytes             |
| --------------- | --------------------: | ------------: | ----------------: |
| `silver/trades` |                68 → 1 |     104 → 105 | 321,143 → 140,646 |
| `gold/candles`  |                32 → 1 |     108 → 109 |    96,545 → 4,933 |

Readers open the snapshot's files. The disk starts above the snapshot because it also holds empty Parquet files, 36 in Silver and 76 in Gold, written by tasks with no rows and never added to a commit. The count on disk rises because Delta keeps the replaced files until a `VACUUM`, which this project does not run. The pass does not compare query time, since at this size it is noise. The 1,698 checkpoint files are a separate problem, left as they are.

## Versions

Python 3.14.9, Java 21 (Temurin 21.0.12.1), PySpark 4.2.0, delta-spark 4.4.1 and uv 0.10.9, pinned in `Dockerfile`, `pyproject.toml` and `uv.lock`.

## Evidence

`docs/scope.md` is the spec. Each milestone has a proof doc with its session's numbers: `docs/2026-11-09-cp1-reconciliation.md`, `docs/2026-11-12-cp2-silver-rejects.md`, `docs/2026-11-17-cp3-candles.md` and `docs/2026-11-27-cp4-operability.md`.
