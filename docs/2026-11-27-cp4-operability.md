# cp4: System is operable and reproducible

Written 2026-11-27 from one session run from a fresh clone of `e1650c9`, with a 60-second scripted reset, one run of the bad-trade injector, the processor stopped and restarted while the ingester kept landing, and a 20-second stall. After it ran the three reconciliations and the metrics report, each twice, `python -m processing.optimize` twice, and the reconciliations once more on the compacted tables. It records the cp4 done-criterion, which asks that a fresh clone go from `docker compose up` to live candles and that the small-files numbers be measured. The quoted files are under `data/`, which git ignores; the session's whole `data/` is in `~/crypto-trades-lakehouse-data-cp4-proof.tgz`.

## What the numbers say

- **A fresh clone reaches live candles 142 s after `docker compose up`.** The build without cache took 133 s, and the code inside both images matches the commit file for file.
- **Compaction took Silver from 68 files to 1 and Gold from 32 to 1.** It changed no row. The reconciliations and the report print the same lines before and after it. Landing kept 1 file of the session's 35, and the other 34 are in the archive.
- **Every stop was clean.** The ingester stopped in 1.3 s and the processor in 2.2 s and 2.6 s, all with exit 0. Stopping the processor mid-session while the ingester kept landing lost nothing and read nothing twice.
- **The metrics report agrees with the reconciliations.** 2 reconnects, 90 missing trade ids, 7 duplicates, 2 rejects and no late trade, counted from the queries' progress, the connection log and Delta's history, match what the reconciliations count from the tables.
- **Gold still reconciles with Kraken.** 31 of its 34 final candles are checked minutes within tolerance; the partial first minute and the two minutes of the reset explain the other three. No trade was too late.
- **Every check is idempotent** (safe to rerun: a second run of the same input changes nothing). Each printed the same lines twice, and a second compaction removed and added nothing.

## Fresh clone to live candles

```sh
git clone git@github.com:rodrigopita/crypto-trades-lakehouse.git   # e1650c9
docker compose build --no-cache                                     # 133 s, exit 0
docker compose up -d                                                # 02:13:53
```

The clone holds only `data/.gitkeep` under `data/`; the ingester and Bronze create their folders, and Spark creates the archive folder on its first move. Containers run as uid 1000, which owns the clone here; a host user with another uid could not write to the bind-mounted `data/`.

| after `up` | what was final                                        |
| ---------: | ----------------------------------------------------- |
|      142 s | the first candle, 02:13, partial                      |
|      197 s | the first full-minute candle, 02:14                   |

A candle is final once the watermark, the newest event time minus 1 minute, passes the end of its minute, so the first full minute cannot be final before about 2 minutes after it starts.

## Small files

The landing folder and the Delta tables are separate small-files problems, as `docs/scope.md` sets out.

### Landing

Bronze's file source archives each landing file one micro-batch late, when the next micro-batch with new data commits. Its `cleanSource` option is set to `archive`, and `sourceArchiveDir` points to `data/archive`. So landing keeps the newest file Bronze has read.

| session | minutes | landing files left | archived |
| ------- | ------: | -----------------: | -------: |
| cp3     |      41 |                 40 |        0 |
| cp4     |      35 |                  1 |       34 |

The written-record log lists 35 files. All 35 are in landing or the archive, none in both. Spark keeps each file's source path under the archive, so they sit in `data/archive/app/data/landing/`.

### Delta tables

`processing.optimize` runs Delta's compaction once on Silver's trades and Gold's candles, with the processor stopped, and appends each table's counts to `data/metrics/optimize.jsonl`.

```
INFO [2026-11-27 02:52:47] __main__ - silver/trades: 68 -> 1 files in the snapshot (104 -> 105 on disk), 68 removed, 1 added, 321143 -> 140646 bytes.
INFO [2026-11-27 02:52:50] __main__ - gold/candles: 32 -> 1 files in the snapshot (108 -> 109 on disk), 32 removed, 1 added, 96545 -> 4933 bytes.
```

| table         | files in the snapshot | files on disk | bytes in the snapshot | rows  |
| ------------- | --------------------: | ------------: | --------------------: | ----: |
| silver/trades |               68 → 1 |     104 → 105 |     321,143 → 140,646 | 6,139 |
| gold/candles  |               32 → 1 |     108 → 109 |         96,545 → 4,933 |    34 |

- **The snapshot is what a reader opens.** Before compaction, Gold held 34 candles in 32 files of about 3 KB each, a file a minute. Every small Parquet file carries its own footer and column metadata, which is why one file of 34 candles takes 5% of the bytes.
- **The disk count goes up.** Compaction adds one file and marks the old ones removed in a new version, but leaves them on disk, so earlier versions stay readable until a `VACUUM`. The project runs none.
- **The disk also holds files no commit ever added.** 36 in Silver and 76 in Gold, each 0 rows and about 1 KB, written by tasks that had no rows to write. Compaction does not touch them.
- **Compaction is a no-op the second time.** It removed and added nothing at 02:53:07 and 02:53:10.
- **Compacting Silver does not disturb Gold's stream.** A probe on 2026-11-25 compacted a Delta source, then restarted the stream that reads it. The stream read only the rows appended afterwards, with no error and no duplicate.

`data/checkpoints/` holds 1,698 files, a small-files problem of its own that compaction does not reach. The pass does not time queries before and after, because at these sizes the difference would be noise.

## The metrics

`reconciliation.metrics` reads the queries' progress reports from `data/metrics/progress.jsonl`, written by a `StreamingQueryListener` in the processor, one line per micro-batch per query; the ingester's connection events from `data/metrics/connections.jsonl`; each table's Delta history; and Bronze's trade ids. It logs the spec's metrics and writes them to `data/metrics/summary.json`.

```
INFO [2026-11-27 02:51:08] __main__ - Websocket reconnects: 2 (6 failed attempts, 76.9 s down, longest 63.6 s).
INFO [2026-11-27 02:51:08] __main__ - Event gaps: 90 missing trade ids.
INFO [2026-11-27 02:51:08] __main__ - Duplicate rate: 7 of 6146 trades (0.11%).
INFO [2026-11-27 02:51:08] __main__ - Late-event rate: Silver 0 (0.00%), Gold 0 (0.00%).
INFO [2026-11-27 02:51:08] __main__ - Records dropped by quality rules: 2.
INFO [2026-11-27 02:51:08] __main__ - Watermark lag, silver_trades: median 327.2 s, p95 373.4 s, max 431.5 s over 67 batches.
INFO [2026-11-27 02:51:08] __main__ - Watermark lag, gold_candles: median 73.6 s, p95 133.8 s, max 195.1 s over 88 batches.
INFO [2026-11-27 02:51:08] __main__ - Micro-batch latency, gold_candles: median 2684.0 ms, p95 6110.0 ms, max 8410.0 ms over 33 batches.
INFO [2026-11-27 02:51:08] __main__ - Micro-batch latency, silver_rejects: median 2622.0 ms, p95 8302.0 ms, max 9365.0 ms over 34 batches.
INFO [2026-11-27 02:51:08] __main__ - Micro-batch latency, silver_trades: median 2541.5 ms, p95 5158.0 ms, max 6228.0 ms over 34 batches.
INFO [2026-11-27 02:51:08] __main__ - Micro-batch latency, bronze_trade_messages: median 1487.0 ms, p95 5671.0 ms, max 5967.0 ms over 33 batches.
INFO [2026-11-27 02:51:08] __main__ - Input rows, gold_candles: 6139.
INFO [2026-11-27 02:51:08] __main__ - Input rows, silver_rejects: 2765.
INFO [2026-11-27 02:51:08] __main__ - Input rows, silver_trades: 2765.
INFO [2026-11-27 02:51:08] __main__ - Input rows, bronze_trade_messages: 2765.
INFO [2026-11-27 02:51:08] __main__ - Output rows, bronze/trade_messages: 2765.
INFO [2026-11-27 02:51:08] __main__ - Output rows, silver/trades: 6139.
INFO [2026-11-27 02:51:08] __main__ - Output rows, silver/trade_rejects: 2.
INFO [2026-11-27 02:51:08] __main__ - Output rows, gold/candles: 34.
```

| metric                | how it is counted                                                                                                                             |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| reconnects            | `disconnected` events: a connection that was open and dropped. Attempts that never connected are `connect_failed`. Downtime runs from each `disconnected` to the next `connected` |
| event gaps            | trade ids missing from Bronze's range: highest − lowest + 1 − distinct                                                                         |
| duplicate rate        | duplicates Silver's dedup dropped, over the trades that reached it: kept + duplicates + dropped by the watermark                                |
| late-event rate       | input behind each stateful operator's watermark. Silver's dedup counts trades, over the trades reaching it. Gold's aggregation counts windows, not trades, so this session's Gold rate mixed units; see "Fixed after the review" |
| dropped by quality rules | rows written to `silver/trade_rejects`                                                                                                     |
| watermark lag         | a micro-batch's start time minus the watermark it ran with, for Silver and Gold. For Gold it is a lower bound on how long a candle waits to be final; see "Fixed after the review" |
| micro-batch latency   | `triggerExecution` of micro-batches with input rows, leaving out each run's first micro-batch, whose time includes startup                     |
| input and output rows | input from each query's progress; output summed from the `numOutputRows` of each table's streaming commits                                     |

Silver's queries count Bronze messages as their input rows, not trades, 2,765 each against 6,148 trades. That is why duplicates come from the dedup operator and rejects from the rejects table. Delta's streaming sink reports `-1` output rows in the progress, so output rows come from the table's history instead.

| metric           | report | reconciliations                                   | injected or scripted          |
| ---------------- | -----: | ------------------------------------------------- | ----------------------------- |
| reconnects       |      2 | not counted                                       | the reset and the stall       |
| event gaps       |     90 | Bronze 90; Gold's gap minutes 15 + 75             | the reset                     |
| duplicates       |      7 | Bronze 7; Silver dropped 7                        | 1, plus 6 from the stall's snapshot |
| rejects          |      2 | Silver: 1 missing `trade_id`, 1 price not positive | 2                             |
| late trades      |      0 | Gold: 0 too late                                  | none                          |
| Silver output    |  6,139 | Silver 6,139                                      |                               |
| Gold output      |     34 | Gold 34 candles                                   |                               |

## Stopping and restarting

The processor's SIGTERM handler sets a flag; its main loop waits for terminations in 2-second slices, checks the flag between them, then stops each query and the Spark session.

| stop                                    | took     | exit |
| --------------------------------------- | -------: | ---: |
| processor, with the ingester running    | 2,234 ms |    0 |
| ingester                                | 1,296 ms |    0 |
| processor, 45 s after the ingester      | 2,639 ms |    0 |

In cp1 to cp3 the processor exited 137 every session, killed after Compose's 10 s grace period. The mid-session stop came at 02:31:53 and the restart at 02:32:56. The ingester landed a file at 02:32:11, while the processor was down, and Bronze read it after the restart. No minute around the restart is a gap or too late. 02:30 to 02:34 are all checked minutes.

## How it was run

The session ran from a script on a scratch clone named `cp4-proof`, so Compose kept its containers, images and `data/` apart from the working checkout. Its steps, in order:

```sh
git clone git@github.com:rodrigopita/crypto-trades-lakehouse.git cp4-proof && cd cp4-proof   # 02:11:38
docker compose build --no-cache                                                              # 02:13:51
docker compose up -d                                                                         # 02:13:53
scripts/inject_failure.sh reset 60                                                           # 02:18:53
~/projects/crypto-trades-lakehouse/.venv/bin/python -m scripts.inject_bad_trades             # 02:25:53, the checkout's venv
docker compose stop processor                                                                # 02:31:53
docker compose start processor                                                               # 02:32:56
scripts/inject_failure.sh stall 20                                                           # 02:38:53
docker compose stop ingester                                                                 # 02:48:53
docker compose stop processor                                                                # 02:49:40
docker compose logs --no-color > data/session/compose.log
for run in 1 2; do
    for r in bronze silver gold metrics; do
        docker compose run --rm --no-deps processor python -m reconciliation.$r
    done
done                                                                                         # 02:49:44 to 02:52:25
docker compose run --rm --no-deps processor python -m processing.optimize                   # 02:52:26, and again at 02:52:51
for r in silver gold metrics; do
    docker compose run --rm --no-deps processor python -m reconciliation.$r
done                                                                                         # 02:53:11 to 02:54:09
```

- **Code at `e1650c9`.** The Python files inside both images match that commit, 5 in the ingester image and 14 in the processor image.
- **An empty start.** The processor created the empty Bronze table at 02:14:09 and the empty Silver trade and reject tables at 02:14:11 and 02:14:12. Bronze's first commit with data came at 02:14:57, after the ingester's first flush.
- **Ingester first.** Its SIGTERM handler flushed the open batch at 02:48:55. Bronze committed that file at 02:48:56, Silver at 02:49:01 and Gold at 02:49:04, before the processor stopped.
- **Checks after the processor stopped, each run twice.** They only read the tables, the written-record log, the metrics files and Kraken's REST API. The second runs printed the same lines as the first, apart from the reference window's start and end, which move with the clock.
- **Kraken's answer, kept.** `data/session/reference-ohlc.json` is the OHLC response fetched at 02:49:43, with 720 finished candles from 14:49 the day before to 02:48. Kraken serves only the last 720 minutes, so from about 14:13 on 2026-11-27 it starts dropping this session's minutes.
- **Logs clean.** `compose.log` holds no traceback and no Spark ERROR line. The processor peaked at 2.1 GiB in `docker stats`, a figure that includes page cache; the ingester at 37 MiB.

## Timeline

UTC, 2026-11-27.

| time        | what happened                                                                          |
| ----------- | -------------------------------------------------------------------------------------- |
| 02:11:38    | Fresh clone at `e1650c9`.                                                              |
| 02:13:51    | Build without cache done, 133 s.                                                       |
| 02:13:53    | Both containers start.                                                                 |
| 02:13:54.96 | `connected`; first snapshot, 50 trades from 02:13:35.                                  |
| 02:14:09    | The processor creates the empty Bronze table, version 0.                               |
| 02:14:22    | First Silver and Gold commits, from the empty Bronze.                                  |
| 02:14:57    | Bronze's first commit with data.                                                       |
| 02:16:15    | First final candle, 02:13.                                                             |
| 02:18:53    | Reset on.                                                                              |
| 02:18:54.97 | `disconnected`: `ConnectionClosedError`.                                               |
| 02:18:56    | First of five refused attempts; the last is at 02:19:26.                               |
| 02:19:54    | Reset off.                                                                             |
| 02:19:58.56 | `connected`; snapshot of 50 trades from 02:19:28, none already received.               |
| 02:25:53    | The injector lands one file with 3 trades.                                             |
| 02:31:56    | Processor stopped with the ingester running, exit 0.                                   |
| 02:32:56    | Processor started again.                                                               |
| 02:38:53    | Stall on.                                                                              |
| 02:39:03.87 | `disconnected`: `TimeoutError: timed out in 5.0s`.                                     |
| 02:39:14    | Stall off; an attempt started during it times out, `connect_failed`.                   |
| 02:39:17.20 | `connected`; snapshot of 50 trades from 02:38:45, 6 already received.                  |
| 02:48:55    | Ingester stopped; it flushes the open batch and exits 0.                               |
| 02:49:04    | Last Gold commit; the last final candle is 02:46.                                      |
| 02:49:43    | Processor stopped, exit 0.                                                             |
| 02:50:02    | Bronze reconciliation.                                                                 |
| 02:50:27    | Silver reconciliation.                                                                 |
| 02:50:48    | Gold reconciliation.                                                                   |
| 02:51:08    | Metrics report.                                                                        |
| 02:52:42    | Silver's compaction commit, version 70; Gold's follows at 02:52:49, version 91.        |

## The reset and the stall

`scripts/inject_failure.sh reset 60` held a TCP-reset rule on port 443 in the ingester's network for 60 s; `stall 20` dropped the port's packets both ways for 20 s.

```
02:13:54.96 connected
02:18:54.97 disconnected    ConnectionClosedError: no close frame received or sent
02:18:56.00 connect_failed  ConnectionRefusedError: [Errno 111] Connection refused
02:18:58.02 connect_failed  ConnectionRefusedError: [Errno 111] Connection refused
02:19:02.04 connect_failed  ConnectionRefusedError: [Errno 111] Connection refused
02:19:10.05 connect_failed  ConnectionRefusedError: [Errno 111] Connection refused
02:19:26.07 connect_failed  ConnectionRefusedError: [Errno 111] Connection refused
02:19:58.56 connected
02:39:03.87 disconnected    TimeoutError: timed out in 5.0s
02:39:14.93 connect_failed  TimeoutError: timed out
02:39:17.20 connected
```

- **The reset kept the connection down 63.6 s, longer than its rule.** The backoff waits 1, 2, 4, 8, 16 and 32 s, so the attempt after 02:19:26 came at 02:19:58, 4.5 s after the rule went away. The snapshot on reconnect reached back 29 s, to 02:19:28, and the 90 trades between the drop and that point were lost, 15 in 02:18 and 75 in 02:19.
- **The stall dropped an open connection.** With the packets dropped, the ingester's 5 s receive timeout ran out at 02:39:03.87, 10.9 s after the rule went in, and it gave up the connection, which `connections.jsonl` records as `disconnected`. The next attempt could not open while the packets were dropped and timed out as `connect_failed`. The connection was down 13.3 s, and the snapshot reached back to 02:38:45, past the start of the stall, so nothing was lost. 6 of its trades were ones the ingester already had, and Silver dropped them.

| minute |           | trades |    open |    high |     low |   close |     VWAP |     volume |
| ------ | --------- | -----: | ------: | ------: | ------: | ------: | -------: | ---------: |
| 02:18  | Gold      |    103 | 61172.8 | 61216.1 | 61168.8 | 61205.0 | 61190.08 | 0.07810240 |
|        | reference |    118 | 61172.8 | 61216.1 | 61168.8 | 61190.7 |  61202.3 | 0.57922625 |
| 02:19  | Gold      |     55 | 61213.3 | 61213.3 | 61149.5 | 61149.5 | 61185.58 | 0.02900451 |
|        | reference |    130 | 61190.8 | 61215.1 | 61145.4 | 61149.5 |  61180.2 | 0.06643636 |

The minutes on either side, 02:17 and 02:20, match their reference candles exactly, apart from VWAP's last decimal.

## The injection

```
INFO [2026-11-27 02:25:53] __main__ - Negative price: trade_id 118175063, timestamp 2026-11-27T02:25:53.704323Z.
INFO [2026-11-27 02:25:53] __main__ - Missing trade_id: timestamp 2026-11-27T02:25:53.704323Z.
INFO [2026-11-27 02:25:53] __main__ - Duplicate: trade_id 118176832, timestamp 2026-11-27T02:25:05.720995Z (48 s before the injection).
```

| injected trade            | Bronze | Silver | `silver/trade_rejects` | Gold                   |
| ------------------------- | -----: | -----: | ---------------------- | ---------------------- |
| negative price, 118175063 |      1 |      0 | `price not positive`   | not counted            |
| no `trade_id`             |      1 |      0 | `missing trade_id`     | not counted            |
| duplicate of 118176832    |      2 |      1 | none; dropped by dedup | counted once, in 02:25 |

02:25 and 02:26 are checked minutes, with 280 and 768 trades, the same as Kraken's.

## The reconciliations

```
INFO [2026-11-27 02:50:02] __main__ - message_count: bronze 2765, record 2765, matches: True.
INFO [2026-11-27 02:50:02] __main__ - trade_count: bronze 6148, record 6148, matches: True.
INFO [2026-11-27 02:50:02] __main__ - min_trade_id: bronze 118175063, record 118175063, matches: True.
INFO [2026-11-27 02:50:02] __main__ - max_trade_id: bronze 118181292, record 118181292, matches: True.
INFO [2026-11-27 02:50:02] __main__ - Distinct trade ids: 6140, duplicates: 7.
INFO [2026-11-27 02:50:02] __main__ - Missing trade ids: 90 of 6230 in range (gaps).
```

```
INFO [2026-11-27 02:50:27] __main__ - dropped vs duplicates in bronze: 7 vs 7, matches: True.
INFO [2026-11-27 02:50:27] __main__ - silver trades vs distinct silver ids: 6139 vs 6139, matches: True.
INFO [2026-11-27 02:50:27] __main__ - Rejected, missing trade_id: 1.
INFO [2026-11-27 02:50:27] __main__ - Rejected, price not positive: 1.
INFO [2026-11-27 02:50:27] __main__ - Duplicates dropped: 7.
```

```
INFO [2026-11-27 02:50:48] __main__ - Gold candles: 34, from 2026-11-27 02:13 to 2026-11-27 02:46 UTC.
INFO [2026-11-27 02:50:48] __main__ - Reference candles: 720 finished, from 2026-11-26 14:50 to 2026-11-27 02:49 UTC.
INFO [2026-11-27 02:50:48] __main__ - Unchecked minutes (no finished reference candle): 0.
INFO [2026-11-27 02:50:48] __main__ - Partial minute (first of the session): 02:13.
INFO [2026-11-27 02:50:48] __main__ - Gap 02:18: reference 118 trades, silver 103, missing 15.
INFO [2026-11-27 02:50:48] __main__ - Gap 02:19: reference 130 trades, silver 55, missing 75.
INFO [2026-11-27 02:50:48] __main__ - Gap minutes: 2, trades missing: 90.
INFO [2026-11-27 02:50:48] __main__ - Too late minutes: 0, trades too late: 0.
INFO [2026-11-27 02:50:48] __main__ - Impossible minutes: 0.
INFO [2026-11-27 02:50:48] __main__ - Checked minutes: 31, within tolerance: 31.
```

The tolerance is cp3's: open, high, low and close exact, volume within 1e-9, VWAP within 0.1. In the 31 checked minutes, OHLC and the trade counts are exact, volume is at most 6.4e-14 off, and VWAP from 0.0005 to 0.0875, always with Gold above, because Kraken cuts VWAP to one decimal.

| measure                                                 | count |
| ------------------------------------------------------- | ----: |
| Bronze trades                                           | 6,148 |
| Silver trades                                           | 6,139 |
| rejected                                                |     2 |
| dropped: 6,148 − 6,139 − 2                              |     7 |
| duplicates in Bronze: 6,147 with an id − 6,140 distinct |     7 |

| Silver trades, by what Gold did with their minute | trades |
| ------------------------------------------------- | -----: |
| partial, 02:13, 64 of Kraken's 138                |     64 |
| checked, 31 minutes                               |  5,773 |
| gap, 02:18 and 02:19                              |    158 |
| never final, 02:47 and 02:48                      |    144 |
| total                                             |  6,139 |

The late-data policy is cp3's, unchanged: a 1-minute watermark, each candle appended once when final. Again no trade reached Gold after its candle was final. The Gold reconciliation counts 0 too-late trades, and the report 0 trades behind Silver's watermark.

## Fixed after the review

The two-axis review from `cp3-candles` found two misleading numbers in the report and a fragile write in the ingester. `2a862cf`, `acd830d` and `7ca47c6` fix them.

- **Gold's late-event rate mixed units.** Gold's aggregation counts its too-late input after partial aggregation, once per window and partition, however many trades that window had. A probe sent 5 too-late trades into one window, and Spark reported `numRowsDroppedByWatermark` 1. The report now gives Gold's figure as windows; the Gold reconciliation's too-late bucket counts trades. cp3's note that this progress field "would count them directly" was wrong for the same reason.
- **Watermark lag understated the finality delay.** Its 73.6 s median ran over all 88 of Gold's micro-batches, including ones with no data, and a micro-batch starts before the commit that makes a candle final. The report now measures each candle from its minute's end to the end of the micro-batch whose watermark passed it. That gives a median of 90.3 s, within 0.1 s of the delay measured from Gold's commit times.
- **A failed connection-event write could stop the ingester.** The write ran inside the reconnect loop's `try`, so an `OSError` from it read as a connection failure, and the second write, in the `except`, ended the loop. Every JSON-line file now goes through one helper, `shared/jsonl.py`, and a failed connection-event write logs a warning. With `data/metrics` replaced by a plain file, the ingester logged the warnings through a 10 s reset and kept landing trades.

The report rerun on this session's data, with `7ca47c6`:

```
Late-event rate: Silver 0 of 6146 trades behind its watermark (0.00%). Gold had too-late trades in 0 windows, a count of windows, not trades.
Candle finality delay: median 90.3 s, p95 147.3 s, max 192.2 s over 34 candles.
```

Every other line printed the same numbers, with "micro-batches" in place of "batches".

## Findings

- **Candles became final a median 90 s after their minute.** Measured from Gold's commit times, the delay ran from 74.2 s to 192.1 s, with a 95th percentile of 147.2 s. The longest was 02:17's, which waited through the reset, when no newer trade could move the watermark.
- **A 63.6 s outage lost 90 trades; a 13.3 s one lost none.** A snapshot is always 50 trades, which reached back 29 and 30 s after the two outages at this session's pace. The reconnect count alone does not tell the two apart; the gap count does.
- **Micro-batches take a few seconds.** The median is 1.5 s for Bronze and 2.7 s for Gold. The 95th percentiles run from 5.2 s to 8.3 s, and the longest micro-batch took 9.4 s, in the rejects query.
- **The disk holds two kinds of file the snapshot does not.** The files compaction replaced, kept for time travel, and the empty files no commit added. Only a `VACUUM` removes either.

## Left for later

- **`VACUUM`.** Not run. The spec asks for one `OPTIMIZE` pass and no scheduled compaction; the disk keeps every replaced and empty file.
- **Checkpoint files.** 1,698 in `data/checkpoints/`, untouched by compaction.
- **Gap recovery.** As in cp3, a backfill from Kraken's REST trades would reach Gold too late for candles already final.
- **Volume comparisons.** `docs/scope.md` asks for Gold metrics "such as VWAP and volume comparisons"; volume is checked against Kraken's every minute, and no other volume metric is built.
- **The checks' timing.** They assume the processor has stopped and every query has read the same Bronze. Nothing checks it.
- **A failed query's shutdown.** When a query fails, the processor exits with its error without stopping the other queries or the Spark session first. Only a SIGTERM or a query that ends without an error takes the clean path.
- **One session per `data/`.** As in cp3; this session started from an empty `data/`.
- **Tests.** `tests/` still holds no file.
