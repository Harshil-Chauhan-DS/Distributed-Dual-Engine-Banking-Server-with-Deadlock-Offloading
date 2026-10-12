"""
telemetry_logger.py  (Module 6)

Collects one record per transaction and writes structured JSON.

Two outputs:
  1. A JSON Lines file. One JSON object per line, written as events happen.
  2. A summary JSON file written once at close().

Many client threads call log_txn() at the same time. A single Lock guards
the shared list and the file handle. This is the classic critical section
problem from OS Unit 2.
"""

import json
import threading
import time


def percentile(sorted_vals, pct):
    """Nearest rank percentile on an already sorted list."""
    if not sorted_vals:
        return 0.0
    idx = int(round((pct / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


class TelemetryLogger:
    def __init__(self, event_path="telemetry_events.jsonl",
                 summary_path="telemetry_summary.json"):
        self.event_path = event_path
        self.summary_path = summary_path

        self._lock = threading.Lock()      # guards everything below
        self._file = open(event_path, "w", encoding="utf-8")
        self._records = []                 # kept in memory for the summary
        self._t_start = None               # set by mark_start()
        self._t_end = None                 # set by mark_end()
        self._closed = False

    # ---- run boundaries -------------------------------------------------

    def mark_start(self):
        """Call right after all clients are released from the barrier."""
        self._t_start = time.perf_counter()
        self._write_event({"event": "run_start", "ts": time.time()})

    def mark_end(self):
        """Call after every client thread has joined."""
        self._t_end = time.perf_counter()
        self._write_event({"event": "run_end", "ts": time.time()})

    # ---- per transaction record ----------------------------------------

    def log_txn(self, client_id, txn_no, status, latency_ms,
                lock_wait_ms=None, offloaded=False, reply=""):
        """
        status is one of: committed, aborted, offloaded, error, timeout.
        lock_wait_ms comes from the server reply when it reports it.
        If the server stays silent we store None and skip it in averages.
        """
        rec = {
            "event": "txn",
            "ts": time.time(),
            "client_id": client_id,
            "txn_no": txn_no,
            "status": status,
            "latency_ms": round(latency_ms, 3),
            "lock_wait_ms": None if lock_wait_ms is None else round(lock_wait_ms, 3),
            "offloaded": bool(offloaded),
            "reply": reply[:120],
        }
        with self._lock:
            self._records.append(rec)
            self._write_event_unlocked(rec)

    # ---- summary --------------------------------------------------------

    def summary(self):
        """Build the summary dict from the records collected so far."""
        with self._lock:
            recs = list(self._records)

        total = len(recs)
        by_status = {}
        for r in recs:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1

        lat = sorted(r["latency_ms"] for r in recs)
        waits = sorted(r["lock_wait_ms"] for r in recs
                       if r["lock_wait_ms"] is not None)
        offloaded = sum(1 for r in recs if r["offloaded"])

        # Wall clock time of the whole run. Falls back to now if unset.
        end = self._t_end if self._t_end else time.perf_counter()
        elapsed = (end - self._t_start) if self._t_start else 0.0

        # Offloaded transactions still finish, so they count as done work.
        done = by_status.get("committed", 0) + by_status.get("offloaded", 0)

        return {
            "total_txns": total,
            "elapsed_sec": round(elapsed, 3),
            "throughput_tps": round(done / elapsed, 2) if elapsed > 0 else 0.0,
            "status_counts": by_status,
            "offloaded_to_overflow": offloaded,
            "offload_rate_pct": round(100.0 * offloaded / total, 2) if total else 0.0,
            "latency_ms": {
                "avg": round(sum(lat) / len(lat), 3) if lat else 0.0,
                "p50": percentile(lat, 50),
                "p95": percentile(lat, 95),
                "p99": percentile(lat, 99),
                "max": lat[-1] if lat else 0.0,
            },
            "lock_wait_ms": {
                "samples": len(waits),
                "avg": round(sum(waits) / len(waits), 3) if waits else 0.0,
                "p95": percentile(waits, 95),
                "max": waits[-1] if waits else 0.0,
            },
        }

    def close(self):
        """Flush the event file and write the summary file. Safe to call twice."""
        if self._closed:
            return None
        self._closed = True
        data = self.summary()
        with self._lock:
            self._file.flush()
            self._file.close()
        with open(self.summary_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return data

    # ---- internals ------------------------------------------------------

    def _write_event(self, obj):
        with self._lock:
            self._write_event_unlocked(obj)

    def _write_event_unlocked(self, obj):
        # Caller already holds self._lock.
        self._file.write(json.dumps(obj) + "\n")
