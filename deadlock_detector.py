"""
deadlock_detector.py  (Module 3, Team Hyphen)

Three jobs live here:
  1. WaitForGraph    : thread safe graph of "who waits for whom".
  2. DeadlockDetector: background daemon thread that scans the graph for cycles.
  3. OverflowClient  : IPC socket client that ships a packaged transaction to
                       the Overflow Server (Module 5).

Edge meaning: edge T1 -> T2 says "T1 waits for a lock that T2 holds".
A cycle in this graph IS a deadlock. No cycle means no deadlock.

Lock ordering rule (so the detector can never deadlock itself):
  LockManager mutex  ->  WFG lock.   Never the other way round.
  The detector only holds the WFG lock while it copies the graph.
"""
import json
import socket
import struct
import threading
import time
from collections import defaultdict


class WaitForGraph:
    """Directed graph of waiting relationships between transactions."""

    def __init__(self):
        self._edges = defaultdict(set)   # waiter -> set of holders
        self._lock = threading.Lock()    # protects _edges

    def set_waits(self, waiter, holders):
        """Replace all outgoing edges of `waiter` (called on every wake up)."""
        with self._lock:
            self._edges[waiter] = set(holders) - {waiter}

    def clear_waiter(self, txn):
        """`txn` stopped waiting (got the lock, aborted or timed out)."""
        with self._lock:
            self._edges.pop(txn, None)

    def remove_txn(self, txn):
        """`txn` finished. Drop its edges and every edge that points at it."""
        with self._lock:
            self._edges.pop(txn, None)
            for targets in self._edges.values():
                targets.discard(txn)

    def snapshot(self):
        """Copy of the graph. The detector works on the copy, not the live data."""
        with self._lock:
            return {k: set(v) for k, v in self._edges.items() if v}

    @staticmethod
    def find_cycle(graph):
        """
        Depth first search with three colours.
          WHITE = not seen, GREY = on the current path, BLACK = fully explored.
        Hitting a GREY node means we walked back into our own path: a cycle.
        Returns the cycle as a list of txn ids, or None.
        Recursion depth equals the longest wait chain, fine for a few hundred txns.
        """
        WHITE, GREY, BLACK = 0, 1, 2
        color = {}
        path = []

        def dfs(node):
            color[node] = GREY
            path.append(node)
            for nxt in graph.get(node, ()):
                state = color.get(nxt, WHITE)
                if state == GREY:
                    return path[path.index(nxt):]      # slice out the loop
                if state == WHITE:
                    found = dfs(nxt)
                    if found:
                        return found
            path.pop()
            color[node] = BLACK
            return None

        for start in list(graph):
            if color.get(start, WHITE) == WHITE:
                found = dfs(start)
                if found:
                    return found
        return None


def package_transaction(txn, origin, reason, cycle=None):
    """
    Turn a live transaction into a plain JSON friendly dict.
    Only data crosses the socket. Locks, threads and undo logs stay behind.
    The Overflow Server replays `ops` from the start.
    """
    return {
        "txn_id": txn.txn_id,
        "priority": getattr(txn, "priority", 0),
        "ops": [list(op) for op in txn.ops],
        "origin": origin,            # "ENGINE_A" or "ENGINE_B"
        "reason": reason,            # e.g. "DEADLOCK_VICTIM"
        "cycle": list(cycle) if cycle else [],
        "packaged_at": time.time(),
    }


class OverflowClient:
    """
    IPC client. Wire format: 4 byte big endian length, then UTF 8 JSON.
    One short TCP connection per package. Simple and easy to reason about.
    NOTE: a retry can deliver the same package twice if the reply gets lost.
    The Overflow Server must ignore a txn_id it has already seen.
    """
    MAX_FRAME = 1_000_000   # refuse absurd replies

    def __init__(self, host="127.0.0.1", port=9100, timeout=10.0, retries=2):
        self.host, self.port = host, port
        self.timeout, self.retries = timeout, retries

    def send(self, package):
        payload = json.dumps(package).encode("utf-8")
        frame = struct.pack(">I", len(payload)) + payload
        last_err = None
        for attempt in range(self.retries + 1):
            try:
                with socket.create_connection((self.host, self.port),
                                              timeout=self.timeout) as sock:
                    sock.sendall(frame)
                    return self._read_frame(sock)
            except OSError as err:               # includes timeouts
                last_err = err
                time.sleep(0.1 * (attempt + 1))  # small backoff
        raise ConnectionError("overflow server unreachable: %s" % last_err)

    def _read_frame(self, sock):
        (size,) = struct.unpack(">I", self._read_exact(sock, 4))
        if size > self.MAX_FRAME:
            raise ValueError("reply too large")
        return json.loads(self._read_exact(sock, size).decode("utf-8"))

    @staticmethod
    def _read_exact(sock, n):
        """TCP is a byte stream. recv(n) may return less than n, so loop."""
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("socket closed mid frame")
            buf += chunk
        return buf


class DeadlockDetector(threading.Thread):
    """
    Daemon thread. Every `interval` seconds:
      copy the WFG -> look for a cycle -> pick one victim -> flag it.
    The detector never kills anything itself. It only flags the victim.
    The victim's own thread wakes up, rolls back and offloads (see engine_a_2pl).
    """

    def __init__(self, wfg, lock_manager, interval=0.2):
        super().__init__(name="deadlock-detector", daemon=True)
        self.wfg = wfg
        self.lm = lock_manager
        self.interval = interval
        self.deadlocks_found = 0
        self._halt = threading.Event()

    def stop(self):
        self._halt.set()

    def run(self):
        while not self._halt.is_set():
            self.scan_once()
            self._halt.wait(self.interval)

    def scan_once(self):
        """Break every cycle that exists right now. Safe to call from tests."""
        while True:
            cycle = WaitForGraph.find_cycle(self.wfg.snapshot())
            if not cycle:
                return
            victim = self._pick_victim(cycle)
            # The snapshot may be stale. mark_victim re checks under the
            # lock manager mutex and says False if the victim is not waiting.
            if not self.lm.mark_victim(victim, cycle):
                return
            self.deadlocks_found += 1
            self.wfg.clear_waiter(victim)   # so the next loop sees other cycles

    def _pick_victim(self, cycle):
        """
        Policy: the txn holding the FEWEST locks (cheapest to undo).
        Ties go to the highest txn id. Age is not the rule here, and the
        victim is not dropped: it gets replayed by the Overflow Server.
        """
        return min(cycle, key=lambda t: (self.lm.lock_count(t), -t))
