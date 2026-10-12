"""
overflow_server.py  |  Module 5: The Distributed Overflow Server ("Deadlock Purgatory")

Run as its own OS process on its own port:
    python overflow_server.py --port 9100

Wire protocol (shared with Engine A and Engine B IPC clients):
    Frame = 4 byte big endian length + UTF 8 JSON body.

    Client -> Server
      {"type": "OFFLOAD", "txn_id": "T17", "origin": "A" or "B",
       "reason": "DEADLOCK" or "WOUNDED", "priority": 5,
       "operations": [{"op": "TRANSFER", "src": "A1", "dst": "B2", "amount": 50}]}
      {"type": "STATS"}
      {"type": "PING"}

    Server -> Client
      {"type": "ACK", "txn_id": ..., "queue_depth": n}     (queued, not yet run)
      {"type": "BUSY", "txn_id": ...}                      (queue full, retry later)
      {"type": "RESULT", "txn_id": ..., "status": "COMMITTED" or "ABORTED",
       "reason": ..., "deltas": {"A1": -50, "B2": 50}, ...}
      {"type": "STATS_REPLY", ...} / {"type": "PONG"}

Design rule: many threads may TALK, exactly ONE thread may TOUCH data.
Reader threads only parse frames and enqueue jobs. A single executor thread
pops jobs and runs them one by one against the ledger. Because only one
thread ever reads or writes balances, no lock is held while waiting for
another lock. No hold and wait means no deadlock. No lock on the ledger
means nothing to lock in the first place.
"""

import argparse
import itertools
import json
import logging
import queue
import signal
import socket
import struct
import threading
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

# --------------------------------------------------------------------------
# Framing helpers (TCP is a byte stream, so we mark message boundaries ourselves)
# --------------------------------------------------------------------------
_HEADER = struct.Struct("!I")        # 4 byte unsigned length, network byte order
_MAX_FRAME = 1_048_576               # 1 MB cap stops a bad peer from eating memory
DEFAULT_PORT = 9100                  # Main server uses a different port on purpose

log = logging.getLogger("overflow")


# --------------------------------------------------------------------------
# Packet normalising. Engine A and Engine B each build packets in their own
# shape. This turns both into the shape documented at the top of this file,
# so the rest of the server only deals with one format.
#   Engine A: {"txn_id": 7, "ops": [["TRANSFER","A","B",50]], "origin": "ENGINE_A"}
#   Engine B: {"tx_id": 7,  "ops": [{"op":"TRANSFER","src":..,"dst":..,"amt":50}], ...}
# The txn id gets the origin letter in front ("A-7", "B-7"), because both
# engines count from 1 and the duplicate check must not mix them up.
# --------------------------------------------------------------------------
def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value                  # the ledger will reject it with a clear reason


def _normalize_op(op) -> dict:
    if isinstance(op, dict):
        kind = str(op.get("op", "")).upper()
        if kind == "TRANSFER":
            amount = op.get("amount", op.get("amt"))
            return {"op": "TRANSFER", "src": op.get("src"), "dst": op.get("dst"),
                    "amount": _to_int(amount)}
        if kind in ("BALANCE", "READ"):
            return {"op": "READ", "account": op.get("account", op.get("acct"))}
        return op
    if isinstance(op, (list, tuple)) and op:
        kind = str(op[0]).upper()
        if kind == "TRANSFER" and len(op) == 4:
            return {"op": "TRANSFER", "src": op[1], "dst": op[2], "amount": _to_int(op[3])}
        if kind in ("BALANCE", "READ") and len(op) == 2:
            return {"op": "READ", "account": op[1]}
    return {"op": "UNKNOWN"}


def normalize_packet(packet: dict) -> dict:
    if packet.get("type") in ("STATS", "PING"):
        return packet
    raw_id = packet.get("txn_id", packet.get("tx_id"))
    origin_text = str(packet.get("origin", "?")).upper()
    origin = origin_text[-1] if origin_text[-1:] in ("A", "B") else origin_text
    out = dict(packet)
    out["type"] = packet.get("type") or "OFFLOAD"
    out["origin"] = origin
    if raw_id is not None:
        out["txn_id"] = f"{origin}-{raw_id}"
    raw_ops = packet.get("ops", packet.get("operations"))
    if isinstance(raw_ops, list):
        out["operations"] = [_normalize_op(o) for o in raw_ops]
    return out


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes or raise. recv() may return fewer bytes than asked."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed the connection")
        buf.extend(chunk)
    return bytes(buf)


def recv_frame(sock: socket.socket) -> dict:
    """Block until one full JSON frame arrives."""
    (length,) = _HEADER.unpack(_recv_exact(sock, _HEADER.size))
    if length > _MAX_FRAME:
        raise ValueError(f"frame too large: {length}")
    return json.loads(_recv_exact(sock, length).decode("utf8"))


class Peer:
    """
    One connected IPC client (Engine A or Engine B bridge).
    The reader thread and the executor thread both write to this socket,
    so a small send lock keeps their frames from interleaving.
    This lock guards a socket. It never guards ledger data.
    """

    def __init__(self, sock: socket.socket, addr: Tuple[str, int]):
        self.sock = sock
        self.addr = addr
        self._send_lock = threading.Lock()
        self.alive = True

    def send(self, payload: dict) -> bool:
        data = json.dumps(payload).encode("utf8")
        try:
            with self._send_lock:
                self.sock.sendall(_HEADER.pack(len(data)) + data)
            return True
        except OSError:
            self.alive = False       # Result stays cached. Client can ask again.
            return False


# --------------------------------------------------------------------------
# Ledger: the data the executor thread owns
# --------------------------------------------------------------------------
class Ledger:
    """
    In memory account balances. NOT thread safe by design.
    Only the executor thread may call execute().
    Each transaction runs on a shadow copy. We swap the copy in on success
    and drop it on failure. That gives all or nothing behaviour (atomicity)
    with no undo log.
    """

    def __init__(self, balances: Dict[str, int]):
        self._balances: Dict[str, int] = dict(balances)

    def snapshot(self) -> Dict[str, int]:
        return dict(self._balances)

    def execute(self, operations: List[dict]) -> Tuple[bool, str, Dict[str, int]]:
        """Returns (ok, reason, deltas). deltas = net change per touched account."""
        shadow = dict(self._balances)

        for index, op in enumerate(operations):
            kind = str(op.get("op", "")).upper()
            try:
                if kind == "TRANSFER":
                    src, dst, amt = op["src"], op["dst"], op["amount"]
                    self._check_amount(amt)
                    self._need(shadow, src)
                    self._need(shadow, dst)
                    if shadow[src] < amt:
                        return False, f"op {index}: insufficient funds in {src}", {}
                    shadow[src] -= amt
                    shadow[dst] += amt
                elif kind == "DEPOSIT":
                    self._check_amount(op["amount"])
                    self._need(shadow, op["account"])
                    shadow[op["account"]] += op["amount"]
                elif kind == "WITHDRAW":
                    self._check_amount(op["amount"])
                    self._need(shadow, op["account"])
                    if shadow[op["account"]] < op["amount"]:
                        return False, f"op {index}: insufficient funds in {op['account']}", {}
                    shadow[op["account"]] -= op["amount"]
                elif kind == "READ":
                    self._need(shadow, op["account"])   # read only, no change
                else:
                    return False, f"op {index}: unknown operation '{kind}'", {}
            except (KeyError, TypeError, ValueError) as exc:
                return False, f"op {index}: bad operation ({exc})", {}

        deltas = {a: shadow[a] - self._balances[a]
                  for a in shadow if shadow[a] != self._balances[a]}
        self._balances = shadow      # commit point
        return True, "ok", deltas

    @staticmethod
    def _check_amount(amount) -> None:
        if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
            raise ValueError("amount must be a positive integer")

    @staticmethod
    def _need(balances: Dict[str, int], account: str) -> None:
        if account not in balances:
            raise KeyError(f"unknown account {account}")


# --------------------------------------------------------------------------
# Job record placed on the priority queue
# --------------------------------------------------------------------------
class Job:
    __slots__ = ("txn_id", "origin", "reason", "operations", "peer", "enqueued_at")

    def __init__(self, packet: dict, peer: Peer):
        self.txn_id: str = packet["txn_id"]
        self.origin: str = packet.get("origin", "?")
        self.reason: str = packet.get("reason", "?")
        self.operations: List[dict] = packet["operations"]
        self.peer = peer
        self.enqueued_at = time.monotonic()


# --------------------------------------------------------------------------
# The server
# --------------------------------------------------------------------------
class OverflowServer:
    """
    Threads in this process:
      1 acceptor        accepts IPC connections
      N readers         one per connection, parse frames and enqueue
      1 executor        the ONLY thread that runs transactions
    """

    DEFAULT_PRIORITY = 5
    RESULT_CACHE_SIZE = 10_000

    def __init__(self, host: str, port: int, ledger: Ledger, queue_size: int = 1000):
        self.host, self.port = host, port
        self._ledger = ledger
        # Lowest number runs first. The sequence number breaks ties in arrival
        # order, so equal priority jobs never starve each other.
        self._queue: "queue.PriorityQueue" = queue.PriorityQueue(maxsize=queue_size)
        self._seq = itertools.count()
        self._stop = threading.Event()
        self._listener: Optional[socket.socket] = None

        # Bookkeeping for retries. Guards two small dicts. Never held during execution.
        self._meta_lock = threading.Lock()
        self._pending_ids = set()
        self._results: "OrderedDict[str, dict]" = OrderedDict()

        self._stats = {"received": 0, "committed": 0, "aborted": 0, "rejected_busy": 0,
                       "duplicates": 0, "by_origin": {"A": 0, "B": 0}}
        self._exec_order = itertools.count(1)

    # ---- lifecycle -------------------------------------------------------
    def start(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((self.host, self.port))
        self._listener.listen(128)
        self._listener.settimeout(1.0)   # wake up now and then to check the stop flag

        threading.Thread(target=self._executor_loop, name="executor", daemon=True).start()
        log.info("Overflow Server listening on %s:%d (single executor)", self.host, self.port)

        while not self._stop.is_set():
            try:
                sock, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            peer = Peer(sock, addr)
            threading.Thread(target=self._reader_loop, args=(peer,),
                             name=f"reader-{addr[1]}", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        if self._listener:
            self._listener.close()

    # ---- reader side: parse and enqueue only -----------------------------
    def _reader_loop(self, peer: Peer) -> None:
        log.info("IPC peer connected: %s", peer.addr)
        try:
            while not self._stop.is_set():
                packet = normalize_packet(recv_frame(peer.sock))
                kind = packet.get("type")
                if kind == "OFFLOAD":
                    self._handle_offload(packet, peer)
                elif kind == "STATS":
                    peer.send({"type": "STATS_REPLY", **self.stats()})
                elif kind == "PING":
                    peer.send({"type": "PONG"})
                else:
                    peer.send({"type": "ERROR", "reason": f"unknown type {kind}"})
        except (ConnectionError, ValueError, json.JSONDecodeError) as exc:
            log.info("IPC peer %s gone: %s", peer.addr, exc)
        finally:
            peer.alive = False
            peer.sock.close()

    def _handle_offload(self, packet: dict, peer: Peer) -> None:
        txn_id = packet.get("txn_id")
        if not txn_id or not isinstance(packet.get("operations"), list):
            peer.send({"type": "ERROR", "txn_id": txn_id, "reason": "malformed OFFLOAD"})
            return

        with self._meta_lock:
            # Retry after an IPC timeout. Never run the same transaction twice.
            if txn_id in self._results:
                self._stats["duplicates"] += 1
                cached = self._results[txn_id]
                resend = True
            elif txn_id in self._pending_ids:
                self._stats["duplicates"] += 1
                peer.send({"type": "ACK", "txn_id": txn_id, "queue_depth": self._queue.qsize()})
                return
            else:
                resend = False

        if resend:
            peer.send(cached)
            return

        job = Job(packet, peer)
        priority = packet.get("priority", self.DEFAULT_PRIORITY)
        try:
            self._queue.put_nowait((priority, next(self._seq), job))
        except queue.Full:
            # Back pressure: tell the sender now instead of letting it time out.
            with self._meta_lock:
                self._stats["rejected_busy"] += 1
            peer.send({"type": "BUSY", "txn_id": txn_id})
            return

        with self._meta_lock:
            self._pending_ids.add(txn_id)
            self._stats["received"] += 1
            if job.origin in self._stats["by_origin"]:
                self._stats["by_origin"][job.origin] += 1
        peer.send({"type": "ACK", "txn_id": txn_id, "queue_depth": self._queue.qsize()})

    # ---- executor side: one thread, one transaction at a time ------------
    def _executor_loop(self) -> None:
        while not self._stop.is_set():
            try:
                _, _, job = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            started = time.monotonic()
            ok, reason, deltas = self._ledger.execute(job.operations)
            finished = time.monotonic()

            result = {
                "type": "RESULT",
                "txn_id": job.txn_id,
                "origin": job.origin,
                "offload_reason": job.reason,
                "status": "COMMITTED" if ok else "ABORTED",
                "reason": reason,
                "deltas": deltas,            # Main server applies these to its own ledger
                "exec_order": next(self._exec_order),
                "queue_wait_ms": round((started - job.enqueued_at) * 1000, 3),
                "exec_ms": round((finished - started) * 1000, 3),
            }

            with self._meta_lock:
                self._pending_ids.discard(job.txn_id)
                self._results[job.txn_id] = result
                if len(self._results) > self.RESULT_CACHE_SIZE:
                    self._results.popitem(last=False)     # drop oldest
                self._stats["committed" if ok else "aborted"] += 1

            job.peer.send(result)    # if the peer died, the cache still holds the answer
            log.info("T=%s from %s (%s) -> %s", job.txn_id, job.origin, job.reason, result["status"])

    def stats(self) -> dict:
        with self._meta_lock:
            snap = dict(self._stats)
            snap["by_origin"] = dict(self._stats["by_origin"])
        snap["queue_depth"] = self._queue.qsize()
        return snap


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def _load_balances(path: Optional[str], prefix: str, count: int, balance: int) -> Dict[str, int]:
    if path:
        with open(path, "r", encoding="utf8") as fh:
            return {str(k): int(v) for k, v in json.load(fh).items()}
    # Same accounts the main server creates: ACC0..ACC<count-1> plus A to E for hand tests.
    accounts = {f"{prefix}{i}": balance for i in range(count)}
    accounts.update({name: balance for name in ("A", "B", "C", "D", "E")})
    return accounts


def main() -> None:
    parser = argparse.ArgumentParser(description="Deadlock Purgatory: overflow server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--accounts", help="JSON file: {\"A\": 1000, \"B\": 500}")
    parser.add_argument("--queue-size", type=int, default=1000)
    parser.add_argument("--prefix", default="ACC", help="account name prefix")
    parser.add_argument("--count", type=int, default=100, help="how many accounts")
    parser.add_argument("--balance", type=int, default=1_000_000, help="start balance each")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(message)s")
    server = OverflowServer(args.host, args.port,
                            Ledger(_load_balances(args.accounts, args.prefix, args.count,
                                                  args.balance)), args.queue_size)
    signal.signal(signal.SIGINT, lambda *_: server.stop())
    server.start()


if __name__ == "__main__":
    main()
