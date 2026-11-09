# cp1: trades survive the network

Written 2026-11-09 from one session of the ingester and the Bronze query with two scripted failures, and from the report `python -m reconciliation.bronze` writes. It is the record of the cp1 done-criterion: a one-hour session including two scripted failure modes ends with Bronze counts reconciling against the ingester's own written-record log. The quoted files are under `data/`, which git ignores.

## What the numbers say

- **2,978 trade messages and 7,346 trades on both sides.** Bronze and `data/record/written.jsonl` agree on both counts and on the lowest and highest `trade_id`. The 59 landing files hold the same 2,978 lines, and each matches its own line in the written-record log.
- **Both failures recovered on their own.** After TCP resets, the ingester was `Connected` 4 s after the rule went on. After a silent stall, its 5 s heartbeat timeout fired and it was `Connected` 3 s after traffic came back.
- **One landing file, one Bronze commit.** The Delta log holds 59 commits, and in order each commit's `numOutputRows` equals its landing file's message count. A landing file reached Bronze 2.1 s after its write at the median and 8.2 s at worst.

## How it was run

```sh
rm -rf data/landing data/tmp data/record data/bronze data/checkpoints   # keeps data/.gitkeep
mkdir -p data/session
docker compose up -d
scripts/inject_failure.sh reset 3 | tee -a data/session/failures.log    # 11:24:24
scripts/inject_failure.sh stall 30 | tee -a data/session/failures.log   # 11:44:03
docker compose stop ingester                                            # 12:14:22
docker compose stop processor                                           # killed 12:15:35
docker compose logs --no-color > data/session/compose.log
docker compose run --rm processor python -m reconciliation.bronze 2>&1 \
    | tee data/session/reconciliation.log
```

- **Code at `9530a7d`.** The code inside both images, built at 11:10 UTC, matches that commit file for file. The containers started at 11:14:27.
- **iptables, because `ss -K` does nothing here.** This WSL kernel has `# CONFIG_INET_DIAG_DESTROY is not set`, so `ss -K` exits 0 and the socket survives. An Alpine sidecar with `NET_ADMIN` in the ingester's network namespace holds a `REJECT --reject-with tcp-reset` rule on port 443 for `reset`, and `DROP` rules both ways for `stall`.
- **Ingester first.** Its SIGTERM handler flushed the open batch at 12:14:22, and Bronze committed that file at 12:14:24, before the processor stopped.
- **Reconciliation in the processor image, twice.** The host has no Java. The script only reads Bronze and the written-record log, so it is idempotent (safe to rerun: a second run of the same input changes nothing). Both runs printed the same report; `reconciliation.log` holds the second.

## Timeline

UTC, 2026-11-09. Fractions of a second come from `received_time` in the landing files.

| time         | what happened                                                 |
| ------------ | ------------------------------------------------------------- |
| 11:14:27     | Both containers start.                                        |
| 11:14:29     | First snapshot, 50 trades with event times from 11:13:38.     |
| 11:24:23.926 | Last trade before the reset, 114916911.                       |
| 11:24:24     | Reset on. `ConnectionClosedError`; the open batch is flushed. |
| 11:24:25     | Reconnect refused.                                            |
| 11:24:27     | Reset off.                                                    |
| 11:24:28     | `Connected`; snapshot of 114916870 to 114916919.              |
| 11:44:03.456 | Last trade before the stall, 114919133.                       |
| 11:44:03     | Stall on.                                                     |
| 11:44:13     | `TimeoutError: timed out in 5.0s`; the open batch is flushed. |
| 11:44:32     | Stall off.                                                    |
| 11:44:33     | Reconnect attempt times out.                                  |
| 11:44:35     | `Connected`; snapshot of 114919136 to 114919185 at 11:44:36.  |
| 12:14:22     | Ingester stopped, exit 0.                                     |
| 12:14:24     | Last Bronze commit, version 58.                               |
| 12:15:35     | Processor killed after the 10 s grace period, exit 137.       |

## The terminated connection

```
2026-11-09T11:24:24Z reset on for 3s
2026-11-09T11:24:27Z reset off
```

```
ingester-1   | WARNING [2026-11-09 11:24:24] __main__ - ConnectionClosedError: no close frame received or sent. Reconnecting...
ingester-1   | INFO [2026-11-09 11:24:24] ingestion.landing - Cleared batch after flushing at 2026-11-09T11:24:24.675403Z with 40 messages and 77 trades.
ingester-1   | WARNING [2026-11-09 11:24:25] __main__ - ConnectionRefusedError: [Errno 111] Connection refused. Reconnecting...
ingester-1   | INFO [2026-11-09 11:24:28] __main__ - Connected to wss://ws.kraken.com/v2.
```

The first retry, after a 1 s backoff, met the same rule and was refused. The second, 2 s later, came after the rule was off.

## The stall

```
2026-11-09T11:44:03Z stall on for 30s
2026-11-09T11:44:32Z stall off
```

```
ingester-1   | WARNING [2026-11-09 11:44:13] __main__ - TimeoutError: timed out in 5.0s. Reconnecting...
ingester-1   | INFO [2026-11-09 11:44:13] ingestion.landing - Cleared batch after flushing at 2026-11-09T11:44:13.727521Z with 8 messages and 24 trades.
ingester-1   | WARNING [2026-11-09 11:44:33] __main__ - TimeoutError: timed out. Reconnecting...
ingester-1   | INFO [2026-11-09 11:44:35] __main__ - Connected to wss://ws.kraken.com/v2.
```

`timed out in 5.0s` is the receive timeout's message (`websockets/sync/messages.py`, line 89), raised by `websocket.recv(timeout=5)` in `receive_trades`. That is the detection; the connection never reported a close. `timed out` is a reconnect attempt that failed while the rule was still on.

## The reconciliation

```
INFO [2026-11-09 12:21:55] __main__ - message_count: bronze 2978, record 2978, matches: True.
INFO [2026-11-09 12:21:55] __main__ - trade_count: bronze 7346, record 7346, matches: True.
INFO [2026-11-09 12:21:55] __main__ - min_trade_id: bronze 114915030, record 114915030, matches: True.
INFO [2026-11-09 12:21:55] __main__ - max_trade_id: bronze 114922335, record 114922335, matches: True.
INFO [2026-11-09 12:21:55] __main__ - Distinct trade ids: 7304, repeated: 42 (snapshot replays).
INFO [2026-11-09 12:21:55] __main__ - Missing trade ids: 2 of 7306 in range (event gaps).
```

| measure          |    Bronze | written-record log |
| ---------------- | --------: | -----------------: |
| trade messages   |     2,978 |              2,978 |
| trades           |     7,346 |              7,346 |
| lowest trade_id  | 114915030 |          114915030 |
| highest trade_id | 114922335 |          114922335 |

The first four lines are the reconciliation, and any mismatch raises `ReconciliationError`. The last two describe Bronze alone. Its lowest id, 114915030, is a trade from before the session that the first snapshot carried.

## Findings

- **The duplicates are the reset's snapshot.** Each of the session's three snapshots holds the latest 50 trades. The one at 11:24:28 held 114916870 to 114916919 while the ingester already had up to 114916911, so 42 came in twice and 8 were made during the outage. No other id repeats, a duplicate rate of 0.57%. Bronze keeps them; dedup is Silver's work in cp2.
- **The stall left a real gap.** The last trade before it was 114919133 and the next snapshot starts at 114919136: 52 trades in those 33 s, and the snapshot carries 50. The rate was not unusual. At the session's median of 111 trades per event-time minute, any outage over about 27 s leaves a gap. The 2 trades never reached the ingester, so the counts still reconcile.
- **One reconnect attempt can take 20 s.** The two stall warnings are 20 s apart: a 1 s backoff and one connect. `websockets` 17.3 passes its 10 s `open_timeout` to `socket.create_connection` (`websockets/sync/client.py`, line 349), which gives each address the full timeout, and `ws.kraken.com` resolves to two, 104.16.14.28 and 104.16.15.28.
- **The stall warning is logged 5 s after the detection.** The receive timeout fired near 11:44:08, 5 s after the last message. Leaving the `with connect(...)` block then closes the connection, which waits its 5 s `close_timeout` for a close frame the DROP rules block, so the warning and the flush read 11:44:13.
- **The processor exits 137.** `python -m processing.main` is PID 1 with no SIGTERM handler, and Linux ignores an unhandled signal for PID 1, so Compose kills it after the 10 s grace period. `docker inspect` shows 137 with `OOMKilled` false; a throwaway processor did the same in an 11 s `docker stop`. No data was harmed: Delta commits are atomic, the query checkpoint resumes the query, and the counts reconcile.

## Left for later

- **Gap recovery.** After a reconnect, Kraken's REST trades endpoint could fetch what the snapshot missed. The ingester does not call it, so the 2 trades stay missing.
- **A SIGTERM handler in the processor.** Stopping the Bronze query on SIGTERM ends the exit 137, and it fits cp4's operability work. `processing/main.py` does not import `signal`.
- **Landing rotation, in cp4.** All 59 landing files are still in `data/landing/`, and `data/archive/` is empty.
- **Tests for `landing.py`.** Deferred during cp1; `tests/` holds no file yet.
- **Received time that steps back.** In 18 places a `received_time` is earlier than the line before it in the same landing file, by up to 1.038 s, and the stall's 30 s hold is stamped 29 s apart. The VM's wall clock steps back, for a reason not yet found; it matters once received time feeds the ingest delay.
