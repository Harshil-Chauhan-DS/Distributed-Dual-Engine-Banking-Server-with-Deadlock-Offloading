# Distributed Dual Engine Banking Server with Deadlock Offloading

Team Hyphen. Lead: Harshil Chauhan. Third year undergraduate project for Operating Systems and DBMS.

A banking server written in Python. Many clients connect over TCP and send money transfers. Two concurrency engines can run those transfers. Engine A waits for locks and detects deadlocks. Engine B prevents deadlocks with the Wound Wait rule. When either engine would kill a transaction, it sends that transaction to a separate Overflow Server instead. The Overflow Server runs it alone on a single thread. It cannot deadlock.

Standard library only. No pip install needed. Python 3.10 or newer.

## How it fits together

```
 client (load_tester.py)
    |  TCP, one text line per command
    v
 main_server.py          acceptor thread + fixed pool of worker threads
    |
 client_handler.py       reads lines, one session per client
    |
 bank_dispatcher.py      collects BEGIN ... COMMIT into one script
    |
 execution_router.py     parse, timestamp, priority queue with aging
    |        \
    |         sql_parser.py
    |
    +-- flag A --> EngineA (engine_a_2pl.py)
    |                strict 2PL + Wait For Graph
    |                deadlock_detector.py scans every 0.2 s
    |
    +-- flag B --> EngineB (engine_b_prevention.py)
                     Wound Wait with timestamps
                     transaction_context.py holds per txn state

 Deadlock victim (A) or wounded txn (B)
    |  length prefixed JSON over a TCP socket
    v
 overflow_server.py      one executor thread, one txn at a time
    |
    +-- result and balance deltas go back to the engine
```

## Files

| File | What it does | Syllabus link |
| --- | --- | --- |
| main_server.py | Listening socket, connection pool, bounded queue, startup flags | OS Unit 2 |
| client_handler.py | One TCP session, line framing, idle timeout | OS Unit 2 |
| bank_dispatcher.py | Buffers lines from BEGIN to COMMIT and hands the script to the router | OS Unit 2 |
| sql_parser.py | Parses BEGIN, TRANSFER, BALANCE, COMMIT, ROLLBACK | DBMS Unit 5 |
| execution_router.py | Priority queue with aging, worker threads, engine flag | OS Unit 2, DBMS Unit 5 |
| engine_adapters.py | Converts router transactions into the shape each engine wants | Glue code |
| engine_a_2pl.py | Strict Two Phase Locking, shared and exclusive locks, undo log | DBMS Unit 5, OS Unit 3 |
| deadlock_detector.py | Wait For Graph, cycle search, daemon thread, IPC client | OS Unit 3 |
| engine_b_prevention.py | Wound Wait, wound and wait decisions, IPC client | DBMS Unit 5, OS Unit 3 |
| transaction_context.py | Timestamp authority and per transaction state | DBMS Unit 5 |
| overflow_server.py | Separate process, single executor thread, result cache | OS Unit 1, 2 and 3 |
| load_tester.py | Hundreds of client threads, hot accounts, barrier start | Benchmark |
| telemetry_logger.py | JSON Lines events and a summary file | Benchmark |

## Run it

Open three terminals in the project folder.

Terminal 1. Start the Overflow Server first.

```
python overflow_server.py
```

Terminal 2. Start the main server.

```
python main_server.py --engine A
```

Terminal 3. Start small. Then scale up.

```
python load_tester.py --clients 1 --txns 1
python load_tester.py --clients 20 --txns 5
python load_tester.py --clients 200 --txns 5 --timeout 60
```

Press Ctrl+C in Terminal 2 when you finish. The server prints an engine summary. The last line checks total money before and after. It must say OK. Transfers only move money. They never create or destroy it.

To test Engine B, stop both servers and start both again with `--engine B`. Restart the Overflow Server on every run. It remembers transaction ids and would return stale results for reused ids.

## Command line options

Main server:

| Option | Default | Meaning |
| --- | --- | --- |
| --engine | A | A for strict 2PL with detection. B for Wound Wait. |
| --port | 9090 | Port clients connect to |
| --pool | 300 | Client threads. Keep it above your client count. |
| --pending | 1000 | Accepted sockets waiting for a free thread |
| --workers | 64 | Router threads that run transactions |
| --accounts | 100 | Number of ACC accounts |
| --balance | 1000000 | Start balance per account |
| --op-delay | 0.02 | Seconds a transaction holds a lock between steps. Makes deadlocks show up. |
| --overflow-port | 9100 | Port of the Overflow Server |

Overflow server: `--port` (9100), `--count` (100), `--balance` (1000000), `--prefix` (ACC), `--queue-size` (1000). Account settings must match the main server.

Load tester: `--clients` (200), `--txns` (5 per client), `--hot-accounts` (3), `--hot-pct` (50), `--timeout` (10 seconds), `--seed` (42), `--events-out`, `--summary-out`. Fewer hot accounts means more lock conflicts.

## Client protocol

One command per line. One reply per line.

```
BEGIN [PRIORITY 0..9]
TRANSFER ACC1 ACC2 50
BALANCE ACC1
COMMIT
```

The server greets with `OK READY session=N`. BEGIN replies `OK BEGIN`. Each step replies `OK QUEUED`. COMMIT runs the whole script and replies with the result:

```
COMMITTED txn=12 engine=EngineA wait_ms=3.10
OFFLOADED txn=13 engine=EngineA wait_ms=210.40 overflow=COMMITTED
ABORTED txn=14 engine=EngineB wait_ms=0.00 reason=...
ERROR txn=0 engine=- parse error: ...
```

Priority 0 runs first. Waiting transactions move up one level every 2 seconds, so none starve.

## Overflow IPC

Each message is a 4 byte big endian length followed by UTF 8 JSON. The engine sends an OFFLOAD packet. The Overflow Server answers ACK when it queues the job. It answers RESULT when the job has run. The RESULT carries the net balance change per account. The engine applies those changes to its own ledger. A repeated transaction id returns the cached result and never runs twice.

## Output files

`telemetry_events.jsonl` holds one JSON line per transaction with status, latency and lock wait. `telemetry_summary.json` holds totals: throughput, offload rate, latency percentiles and lock wait percentiles.

## Sample result

One run on a Linux machine. 200 clients, 5 transactions each, 20 ms lock hold. Your numbers will differ.

| | Engine A | Engine B |
| --- | --- | --- |
| Committed | 822 | 362 |
| Offloaded | 178 | 638 |
| Total money check | OK | OK |

Engine B offloads more because it aborts a younger transaction at the first conflict. Engine A lets transactions wait and acts only when the detector finds a real cycle.

## Known limits

* The Overflow Server keeps a shadow ledger. The real balances live in the main engine. Deltas flow back after each offload.
* Engine B uses exclusive locks only. A read locks like a write.
* Transfer amounts must be whole numbers.
* Two engines keep separate balances. Switch engines only when the system is idle.
* Heavy load on three hot accounts makes transactions queue. Raise the tester timeout for large runs.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| timeout in tester status | Raise `--timeout` |
| ERR SERVER_BUSY or WinError 10053 | Raise `--pool` and `--pending` |
| Connection refused | Check the main server is up on 9090 |
| overflow unreachable in replies | Start overflow_server.py first and check port 9100 |
| 0 offloaded | Lower `--hot-accounts` or raise `--op-delay` so locks overlap |
| Stale results after a restart | Restart the Overflow Server too |
