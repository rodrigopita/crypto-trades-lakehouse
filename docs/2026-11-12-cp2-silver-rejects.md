# cp2: Silver rejects garbage

Written 2026-11-12 from one session of the ingester, the Bronze query and the two Silver queries, with one scripted reset and one run of the bad-trade injector, and from the reports `python -m reconciliation.bronze` and `python -m reconciliation.silver` write. It is the record of the cp2 done-criterion: an injected negative price, a missing `trade_id` and a duplicate all show up in the reject metrics instead of Silver. The quoted files are under `data/`, which git ignores.

## What the numbers say

- **The three injected trades reached the reject metrics, and none reached Silver.** The negative price and the missing `trade_id` are the only two rows in `silver/trade_rejects`, each with its reason. The duplicate is in Bronze twice and in Silver once, and it is one of the 53 duplicates the Silver reconciliation counts as dropped.
- **Every Bronze trade is accounted for.** Bronze holds 8,621 trades: 8,566 in Silver, 2 rejected and 53 dropped. The 53 equal the duplicates Bronze shows on its own, and Silver holds each of its 8,566 trade ids once.
- **Bronze still reconciles with the ingester.** 4,095 messages and 8,621 trades on both sides, the same lowest and highest `trade_id`, and no gap in the ids.

## How it was run

```sh
tar czf ~/crypto-trades-lakehouse-data-cp1.tgz data   # cp1's data, kept outside the repo
rm -rf data/*                                         # keeps data/.gitkeep
mkdir data/session
docker compose build
docker compose up -d                                                              # 14:41:53
scripts/inject_failure.sh reset 3 | tee -a data/session/failures.log              # 14:47:09
uv run python -m scripts.inject_bad_trades 2>&1 | tee data/session/injection.log  # 14:54:47
docker compose stop ingester                                                      # 15:05:39
docker compose stop processor                                                     # killed 15:08:33
docker compose logs --no-color > data/session/compose.log
docker compose run --rm processor python -m reconciliation.bronze 2>&1 \
    | tee data/session/reconciliation-bronze.log
docker compose run --rm processor python -m reconciliation.silver 2>&1 \
    | tee data/session/reconciliation-silver.log
```

- **Code at `d076351`.** The Python files inside both images match that commit file for file, 5 in the ingester image and 9 in the processor image. Docker reused a cached ingester image, built on 2026-11-10 from the same code.
- **An empty start.** The processor created the landing folder and an empty Bronze table (version 0, 14:42:09) before the first landing file existed. Before `8b11ca9`, that start crashed Silver with `DELTA_SCHEMA_NOT_SET`.
- **Ingester first.** Its SIGTERM handler flushed the open batch at 15:05:39. Bronze committed that file at 15:05:40 and Silver at 15:05:52, before the processor stopped.
- **Reconciliations after the processor stopped, each run twice.** The host has no Java, so both run in the processor image. They only read Bronze, Silver and the written-record log, so they are idempotent (safe to rerun: a second run of the same input changes nothing). A second run of each, read-only on the same `data/`, printed the same lines as the session's logs.

## Timeline

UTC, 2026-11-12.

| time     | what happened                                                                   |
| -------- | ------------------------------------------------------------------------------- |
| 14:41:53 | Both containers start.                                                          |
| 14:41:55 | `Connected`; first snapshot, 50 trades over 8 s of event time.                  |
| 14:42:09 | The processor creates the empty Bronze table, version 0.                        |
| 14:42:56 | First landing file in Bronze, version 1, 138 messages.                          |
| 14:47:09 | Reset on.                                                                       |
| 14:47:10 | `ConnectionClosedError`; the open batch is flushed.                             |
| 14:47:11 | Reconnect refused.                                                              |
| 14:47:12 | Reset off.                                                                      |
| 14:47:14 | `Connected`; snapshot of 50 trades over 4 s, 2 of them already received.        |
| 14:54:47 | The injector lands one file with 3 trades.                                      |
| 15:02:48 | Unscripted `ConnectionClosedError`; the open batch is flushed.                  |
| 15:02:50 | `Connected`; snapshot of 50 trades over 15 s, all 50 already received.          |
| 15:05:39 | Ingester stopped, exit 0.                                                       |
| 15:05:40 | Last Bronze commit, version 26.                                                 |
| 15:05:52 | Last Silver commit.                                                             |
| 15:08:33 | Processor killed after the 10 s grace period, exit 137.                         |

## The injection

```
INFO [2026-11-12 14:54:47] __main__ - Negative price: trade_id 115448300, timestamp 2026-11-12T14:54:47.224573Z.
INFO [2026-11-12 14:54:47] __main__ - Missing trade_id: timestamp 2026-11-12T14:54:47.224573Z.
INFO [2026-11-12 14:54:47] __main__ - Duplicate: trade_id 115452847, timestamp 2026-11-12T14:54:19.057655Z (28 s before the injection).
```

`scripts/inject_bad_trades.py` built the three trades from the newest landed trade and wrote them through the ingester's own `TradesBatch` as `20261112T145447224735Z-trades.jsonl`, so the file has its line in the written-record log like any other. Where each one ended up:

| injected trade            | Bronze | Silver | `silver/trade_rejects`                           |
| ------------------------- | -----: | -----: | ------------------------------------------------ |
| negative price, 115448300 |      1 |      0 | `price not positive`, price `-71438.9` as landed |
| no `trade_id`             |      1 |      0 | `missing trade_id`                               |
| duplicate of 115452847    |      2 |      1 | none; dropped by dedup                           |

The negative price takes the id one below the lowest in the written-record log, 115448301 from the first snapshot. Kraken used that id before the session, so no real trade arrives with it, and it extends the id range without opening a gap.

## The reconciliations

```
INFO [2026-11-12 15:09:09] __main__ - message_count: bronze 4095, record 4095, matches: True.
INFO [2026-11-12 15:09:09] __main__ - trade_count: bronze 8621, record 8621, matches: True.
INFO [2026-11-12 15:09:09] __main__ - min_trade_id: bronze 115448300, record 115448300, matches: True.
INFO [2026-11-12 15:09:09] __main__ - max_trade_id: bronze 115456866, record 115456866, matches: True.
INFO [2026-11-12 15:09:09] __main__ - Distinct trade ids: 8567, repeated: 53 (snapshot replays).
INFO [2026-11-12 15:09:09] __main__ - Missing trade ids: 0 of 8567 in range (event gaps).
```

```
INFO [2026-11-12 15:09:33] __main__ - dropped vs repeated ids in bronze: 53 vs 53, matches: True.
INFO [2026-11-12 15:09:33] __main__ - silver trades vs distinct silver ids: 8566 vs 8566, matches: True.
INFO [2026-11-12 15:09:33] __main__ - Rejected, missing trade_id: 1.
INFO [2026-11-12 15:09:33] __main__ - Rejected, price not positive: 1.
INFO [2026-11-12 15:09:33] __main__ - Duplicates dropped: 53.
```

| measure                                                 | count |
| ------------------------------------------------------- | ----: |
| Bronze trades                                           | 8,621 |
| Silver trades                                           | 8,566 |
| rejected                                                |     2 |
| dropped: 8,621 − 8,566 − 2                              |    53 |
| duplicates in Bronze: 8,620 with an id − 8,567 distinct |    53 |

In the Bronze report, the first four lines are the reconciliation with the written-record log. In the Silver report, the first two are the reconciliation with Bronze and the last three are the reject metrics. A mismatch in either raises `ReconciliationError`.

These are the session's logs, from `d076351`. Since `3e92eb4` the same lines follow the glossary: `duplicates: 53`, `(gaps)` and `dropped vs duplicates in bronze`, and a rerun on the same `data/` gave the same numbers.

## Findings

- **The 53 duplicates came from three places.** The snapshot after the scripted reset replayed 2 trades the ingester already had, the one after the unscripted disconnect replayed 50, and the injector added 1. The natural ones were at most 15 s behind the newest event time and the injected one 28 s, well inside the 5-minute watermark threshold, so dedup kept one copy of each.
- **The reset's snapshot had almost no room.** Its 50 trades covered 4 s of event time, about 12 trades a second, and only 2 of them overlapped what the ingester already had, under a fifth of a second of margin. An outage that long, at that rate, sits right at the edge of a gap like cp1's stall. This session had none.
- **The connection dropped once on its own.** At 15:02:48, with no rule in place (`failures.log` holds only the reset), it closed without a close frame. The ingester flushed and was `Connected` 2 s later. The cause is not visible from the client.
- **Parquet shrank its row groups all session.** The processor logged 812 `WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory` lines from 14:42:59 to 15:05:43. That figure is 95% of the driver's 1 GiB default heap. The Parquet writers asked for more row-group buffer than that, so Parquet scaled the row groups down. Nothing failed.
- **The processor still exits 137.** As in cp1, `python -m processing.main` is PID 1 with no SIGTERM handler, so Compose kills it after the 10 s grace period. No data was harmed: the ingester had stopped 3 minutes earlier and the last commits were in.

## Left for later

- **Gap recovery, decided in cp3.** This session had no gap, but the reset's snapshot overlapped by 2 trades. Either cp3's tolerance absorbs a gap or the ingester backfills from Kraken's REST trades endpoint after a reconnect.
- **The processor's heap, before Gold.** Gold adds writers to the same 1 GiB heap in cp3. Watch `docker stats` and the `MemoryManager` warnings; a larger `spark.driver.memory` or smaller Parquet row groups are the two levers.
- **A SIGTERM handler in the processor, in cp4.** It ends the exit 137.
- **Reconnects as metrics, in cp4.** This session had two, one of them unscripted, and they show only as log lines.
- **A bad copy of a real trade.** If a rejected trade ever shares its id with a good one, Bronze counts the id as a duplicate but nothing is dropped, and the Silver reconciliation reports a mismatch. Kraken has not sent one.
- **Tests.** `tests/` still holds no file.
