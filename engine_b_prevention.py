"""
engine_b_prevention.py
Engine B: deadlock PREVENTION with the Wound Wait rule.

Rule:
    requester older than holder  -> requester WOUNDS the holder
    requester younger than holder -> requester WAITS

Wound means: roll back the holder, free its locks, flag it. The holder
thread sees the flag at its next step and forwards itself to the Overflow
Server over IPC instead of failing.

Locking model: strict. Every lock is exclusive and is held until commit
or abort. One engine mutex protects the lock table, the account data and
the undo logs. That keeps a rollback done by a wounder safe, because the
victim can never be halfway through a write at that moment.
"""

import json
import socket
import struct
import threading
import time

from transaction_context import (
    InsufficientFundsError,
    TimestampAuthority,
    TransactionContext,
    TxState,
    WoundedError,
)


# --------------------------------------------------------------------------
# IPC helpers. Module 5 uses the same framing.
# --------------------------------------------------------------------------

def send_msg(sock, obj):
    """Send one JSON message with a 4 byte length prefix."""
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack(">I", len(data)) + data)


def _read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed the socket")
        buf += chunk
    return buf


def recv_msg(sock):
    """Read one length prefixed JSON message."""
    (size,) = struct.unpack(">I", _read_exact(sock, 4))
    return json.loads(_read_exact(sock, size).decode("utf-8"))


class OverflowUnavailable(Exception):
    """The Overflow Server could not be reached."""


class OverflowClient:
    """
    Sends packaged transactions to the Overflow Server.

    - One short TCP connection per transaction. No shared socket, so no
      lock contention between worker threads.
    - A semaphore caps parallel sends. Hundreds of threads hitting one
      listener at once is what causes accept queue timeouts.
    - We retry only the CONNECT step. If we already sent the packet and
      the read times out, a retry could run the transaction twice.
    """

    def __init__(self, host="127.0.0.1", port=9100, connect_timeout=2.0,
                 result_timeout=15.0, retries=3, max_parallel=16):
        self.addr = (host, port)
        self.connect_timeout = connect_timeout
        self.result_timeout = result_timeout
        self.retries = retries
        self._gate = threading.BoundedSemaphore(max_parallel)

    def send(self, packet):
        with self._gate:
            sock = self._connect()
            try:
                send_msg(sock, packet)
                sock.settimeout(self.result_timeout)
                try:
                    return recv_msg(sock)
                except socket.timeout:
                    # Packet is queued over there. Result just is not ready.
                    return {"status": "OFFLOADED_PENDING",
                            "tx_id": packet["tx_id"]}
            finally:
                sock.close()

    def _connect(self):
        last_err = None
        for attempt in range(self.retries):
            try:
                return socket.create_connection(self.addr,
                                                timeout=self.connect_timeout)
            except OSError as err:
                last_err = err
                time.sleep(0.05 * (attempt + 1))   # small backoff
        raise OverflowUnavailable(str(last_err))


# --------------------------------------------------------------------------
# The engine
# --------------------------------------------------------------------------

class EngineB:
    def __init__(self, accounts, overflow_client, think_time=0.0):
        self.accounts = accounts            # shared dict: name -> balance
        self.overflow = overflow_client
        self.think_time = think_time        # fake work per op, for demos

        self._clock = TimestampAuthority()
        self._cv = threading.Condition()    # the engine mutex
        self._lock_table = {}               # account -> holder context

        # Counters for Module 6 telemetry. Own lock, so reading stats
        # never blocks on the engine mutex.
        self._stats_lock = threading.Lock()
        self._stats = {
            "committed": 0, "aborted": 0, "wounds": 0, "waits": 0,
            "offloaded": 0, "offload_failed": 0, "lock_wait_seconds": 0.0,
        }

    # ---- public API ----------------------------------------------------

    def new_context(self, tx_id, ops):
        """Stamp a new transaction. Called once when it enters the system."""
        return TransactionContext(tx_id, ops, self._clock.next())

    def stats(self):
        with self._stats_lock:
            return dict(self._stats)

    def execute(self, ctx):
        """Run one transaction to the end. Returns a result dict."""
        ctx.state = TxState.ACTIVE
        try:
            for op in ctx.ops:
                self._run_op(ctx, op)
                if self.think_time:
                    time.sleep(self.think_time)
            self._commit(ctx)
            self._bump("committed")
            return self._result(ctx, "COMMITTED")

        except WoundedError:
            # Locks and writes are already gone. Wounder cleaned them up.
            return self._offload(ctx)

        except (InsufficientFundsError, KeyError, ValueError) as err:
            self._abort(ctx)
            self._bump("aborted")
            return self._result(ctx, "ABORTED", reason=str(err))

    # ---- operations ----------------------------------------------------

    def _run_op(self, ctx, op):
        kind = op.get("op", "").upper()
        if kind == "TRANSFER":
            self._transfer(ctx, op["src"], op["dst"], int(op["amt"]))
        else:
            raise ValueError("unknown operation: %r" % kind)

    def _transfer(self, ctx, src, dst, amt):
        with self._cv:
            if ctx.is_wounded():
                raise WoundedError()
            if src not in self.accounts or dst not in self.accounts:
                raise KeyError("no such account")
            if amt <= 0:
                raise ValueError("amount must be positive")

            self._lock_account(ctx, src)
            self._lock_account(ctx, dst)

            if self.accounts[src] < amt:
                raise InsufficientFundsError("%s has too little money" % src)

            # Save the before image once per account. Rollback uses it.
            ctx.undo.setdefault(src, self.accounts[src])
            ctx.undo.setdefault(dst, self.accounts[dst])
            self.accounts[src] -= amt
            self.accounts[dst] += amt

    # ---- the Wound Wait core -------------------------------------------

    def _lock_account(self, ctx, acct):
        """
        Caller must hold self._cv.
        Loops until ctx owns the lock, or raises WoundedError.
        """
        wait_started = None
        while True:
            # A waiting thread can get wounded by an older transaction.
            # notify_all wakes us up and we land here.
            if ctx.is_wounded():
                self._close_wait(ctx, wait_started)
                raise WoundedError()

            holder = self._lock_table.get(acct)

            if holder is None or holder is ctx:
                self._lock_table[acct] = ctx
                ctx.held.add(acct)
                self._close_wait(ctx, wait_started)
                ctx.state = TxState.ACTIVE
                return

            if ctx.ts < holder.ts:
                # We are older. Wound the holder, then loop and take the
                # lock that just came free.
                self._wound(holder)
                continue

            # We are younger. We wait. Safe, because we only ever wait
            # for older transactions, so a cycle cannot exist.
            if wait_started is None:
                wait_started = time.monotonic()
                ctx.state = TxState.WAITING
                self._bump("waits")
            self._cv.wait(timeout=0.5)   # timeout is a safety net only

    def _wound(self, victim):
        """Caller must hold self._cv. Kill the victim's work right now."""
        victim.mark_wounded()
        self._rollback_and_release(victim)
        self._bump("wounds")
        self._cv.notify_all()

    # ---- commit, abort, cleanup ----------------------------------------

    def _commit(self, ctx):
        with self._cv:
            # Last check. A wound can land between the final op and here.
            if ctx.is_wounded():
                raise WoundedError()
            ctx.undo.clear()
            self._release_locks(ctx)
            ctx.state = TxState.COMMITTED
            self._cv.notify_all()

    def _abort(self, ctx):
        with self._cv:
            self._rollback_and_release(ctx)
            ctx.state = TxState.ABORTED
            self._cv.notify_all()

    def _rollback_and_release(self, ctx):
        """Caller must hold self._cv. Safe to call twice."""
        for acct, old_balance in ctx.undo.items():
            self.accounts[acct] = old_balance
        ctx.undo.clear()
        self._release_locks(ctx)

    def _release_locks(self, ctx):
        for acct in ctx.held:
            if self._lock_table.get(acct) is ctx:
                del self._lock_table[acct]
        ctx.held.clear()

    # ---- overflow ------------------------------------------------------

    def _offload(self, ctx):
        """
        Forward a wounded transaction to the Overflow Server.
        We do NOT hold the engine mutex here. Network calls under the
        mutex would freeze every other transaction.
        """
        packet = ctx.to_packet(reason="WOUNDED")
        try:
            reply = self.overflow.send(packet)
        except OverflowUnavailable as err:
            self._bump("offload_failed")
            ctx.state = TxState.ABORTED
            return self._result(ctx, "OFFLOAD_FAILED", reason=str(err))

        ctx.state = TxState.OFFLOADED
        self._bump("offloaded")
        result = self._result(ctx, "OFFLOADED")
        result["overflow_reply"] = reply
        return result

    # ---- small helpers -------------------------------------------------

    def _close_wait(self, ctx, wait_started):
        if wait_started is not None:
            waited = time.monotonic() - wait_started
            ctx.wait_seconds += waited
            with self._stats_lock:
                self._stats["lock_wait_seconds"] += waited

    def _bump(self, key):
        with self._stats_lock:
            self._stats[key] += 1

    @staticmethod
    def _result(ctx, status, reason=None):
        out = {"tx_id": ctx.tx_id, "status": status,
               "timestamp": ctx.ts,
               "wait_seconds": round(ctx.wait_seconds, 6)}
        if reason:
            out["reason"] = reason
        return out
