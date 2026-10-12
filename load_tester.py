"""
load_tester.py  (Module 6)

Spawns hundreds of client threads. Each one opens its own TCP connection to
main_server.py and runs a batch of BEGIN / TRANSFER / COMMIT transactions.

Usage:
    python load_tester.py --clients 300 --txns 5 --hot-pct 60

ASSUMED WIRE FORMAT (match this to your Module 1 and 2 handler):
    client -> server : one text line ending in \\n
    server -> client : one text line ending in \\n per command
The reply to COMMIT decides the outcome:
    contains OFFLOAD            -> offloaded
    contains COMMIT or OK       -> committed
    contains ABORT              -> aborted
    anything else               -> error
The server may add key=value tokens, for example "COMMITTED wait_ms=12.4".
The tester reads wait_ms as the lock wait time. Change parse_reply() if your
handler uses another format.
"""

import argparse
import random
import re
import socket
import threading
import time

from telemetry_logger import TelemetryLogger

WAIT_RE = re.compile(r"wait_ms=([0-9.]+)", re.IGNORECASE)


def parse_reply(reply):
    """Turn a raw server reply into (status, lock_wait_ms, offloaded)."""
    up = reply.upper()
    m = WAIT_RE.search(reply)
    wait = float(m.group(1)) if m else None

    if "OFFLOAD" in up:
        return "offloaded", wait, True
    if "ABORT" in up or "WOUND" in up:
        return "aborted", wait, False
    if "COMMIT" in up or up.startswith("OK"):
        return "committed", wait, False
    return "error", wait, False


class LoadClient(threading.Thread):
    """One simulated bank client. One thread. One socket."""

    def __init__(self, cid, args, logger, barrier):
        super().__init__(name="client-%d" % cid, daemon=True)
        self.cid = cid
        self.args = args
        self.logger = logger
        self.barrier = barrier
        # Own Random per thread. Seeded so a run can be repeated.
        self.rng = random.Random(args.seed + cid)

    # ---- helpers --------------------------------------------------------

    def _pick_accounts(self):
        """
        Pick two different accounts. With hot_pct chance each side comes
        from a tiny hot set. Hot accounts create lock conflicts, and
        conflicts create deadlocks. That is the whole point of the test.
        """
        a = self.args
        hot = max(2, a.hot_accounts)

        def one():
            if self.rng.randint(1, 100) <= a.hot_pct:
                return self.rng.randrange(hot)
            return self.rng.randrange(a.accounts)

        src = one()
        dst = one()
        while dst == src:
            dst = one()
        return "%s%d" % (a.prefix, src), "%s%d" % (a.prefix, dst)

    def _send(self, rf, wf, line):
        wf.write(line + "\n")
        wf.flush()
        reply = rf.readline()
        if not reply:
            raise ConnectionError("server closed the socket")
        return reply.strip()

    # ---- thread body ----------------------------------------------------

    def run(self):
        a = self.args
        sock = None
        try:
            sock = socket.create_connection((a.host, a.port), timeout=a.timeout)
            sock.settimeout(a.timeout)
            rf = sock.makefile("r", encoding="utf-8", newline="\n")
            wf = sock.makefile("w", encoding="utf-8", newline="\n")
        except OSError as exc:
            # Connection failed. Log every planned txn as an error and leave.
            # Still hit the barrier so the other clients do not hang.
            try:
                self.barrier.wait(timeout=a.timeout)
            except threading.BrokenBarrierError:
                pass
            for n in range(a.txns):
                self.logger.log_txn(self.cid, n, "error", 0.0, reply=str(exc))
            return

        # Wait here until every client has connected. Then all fire together.
        try:
            self.barrier.wait(timeout=a.timeout * 2)
        except threading.BrokenBarrierError:
            pass

        for n in range(a.txns):
            src, dst = self._pick_accounts()
            amount = self.rng.randint(1, a.max_amount)
            t0 = time.perf_counter()
            try:
                self._send(rf, wf, "BEGIN")
                self._send(rf, wf, "TRANSFER %s %s %d" % (src, dst, amount))
                reply = self._send(rf, wf, "COMMIT")
                status, wait, off = parse_reply(reply)
            except socket.timeout:
                reply, status, wait, off = "timeout", "timeout", None, False
            except (OSError, ConnectionError) as exc:
                reply, status, wait, off = str(exc), "error", None, False

            ms = (time.perf_counter() - t0) * 1000.0
            self.logger.log_txn(self.cid, n, status, ms,
                                lock_wait_ms=wait, offloaded=off, reply=reply)

            # A dead socket cannot carry more transactions.
            if status in ("error", "timeout"):
                break

            if a.think_ms > 0:
                time.sleep(self.rng.uniform(0, a.think_ms) / 1000.0)

        try:
            self._send(rf, wf, "QUIT")
        except Exception:
            pass
        sock.close()


def parse_args():
    p = argparse.ArgumentParser(description="Banking server load tester")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9090)
    p.add_argument("--clients", type=int, default=200)
    p.add_argument("--txns", type=int, default=5, help="txns per client")
    p.add_argument("--accounts", type=int, default=100)
    p.add_argument("--hot-accounts", type=int, default=3)
    p.add_argument("--hot-pct", type=int, default=50,
                   help="percent of picks that land on hot accounts")
    p.add_argument("--max-amount", type=int, default=100)
    p.add_argument("--prefix", default="ACC")
    p.add_argument("--think-ms", type=int, default=0)
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--events-out", default="telemetry_events.jsonl")
    p.add_argument("--summary-out", default="telemetry_summary.json")
    return p.parse_args()


def main():
    args = parse_args()
    logger = TelemetryLogger(args.events_out, args.summary_out)
    # Fallback clock in case the barrier never opens (all connects fail).
    logger.mark_start()
    # The action runs once, on the thread that releases the barrier.
    # So the clock restarts after every client has connected.
    barrier = threading.Barrier(args.clients, action=logger.mark_start)

    clients = [LoadClient(i, args, logger, barrier) for i in range(args.clients)]
    for c in clients:
        c.start()
    for c in clients:
        c.join()
    logger.mark_end()

    s = logger.close()
    print("clients      :", args.clients)
    print("total txns   :", s["total_txns"])
    print("elapsed      : %.2fs" % s["elapsed_sec"])
    print("throughput   : %.1f txn/s" % s["throughput_tps"])
    print("status       :", s["status_counts"])
    print("offloaded    : %d (%.1f%%)" % (s["offloaded_to_overflow"],
                                          s["offload_rate_pct"]))
    print("latency ms   : p50=%.1f p95=%.1f p99=%.1f" % (
        s["latency_ms"]["p50"], s["latency_ms"]["p95"], s["latency_ms"]["p99"]))
    print("lock wait ms : avg=%.1f p95=%.1f (%d samples)" % (
        s["lock_wait_ms"]["avg"], s["lock_wait_ms"]["p95"],
        s["lock_wait_ms"]["samples"]))


if __name__ == "__main__":
    main()
