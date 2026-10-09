"""
transaction_context.py
Per transaction state for Engine B (Wound Wait).

One TransactionContext object lives for the whole life of one transaction.
The engine reads and writes it. The only field another thread may touch
without the engine mutex is the `wounded` Event.
"""

import threading
import time
from enum import Enum


class TxState(Enum):
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    WAITING = "WAITING"
    WOUNDED = "WOUNDED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"        # logical abort, for example not enough money
    OFFLOADED = "OFFLOADED"    # handed to the Overflow Server


class WoundedError(Exception):
    """Raised inside the victim thread once it notices it was wounded."""


class InsufficientFundsError(Exception):
    """Business rule failure. This is a normal abort, not a deadlock case."""


class TimestampAuthority:
    """
    Logical clock. Hands out strictly increasing integers.
    A smaller number means an older transaction, which means higher priority.
    We use a counter instead of wall clock time because two threads can
    read the same clock tick. A counter never gives out a duplicate.
    """

    def __init__(self, start=1):
        self._next = start
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            value = self._next
            self._next += 1
            return value


class TransactionContext:
    def __init__(self, tx_id, ops, timestamp):
        self.tx_id = tx_id
        self.ops = list(ops)
        # The timestamp never changes, even after a wound and restart.
        # This stops starvation. A wounded transaction keeps its age,
        # so it gets older relative to new arrivals and finally wins.
        self.ts = timestamp
        self.state = TxState.NEW
        self.held = set()            # accounts this transaction has locked
        self.undo = {}               # account -> balance before first write
        self.wounded = threading.Event()
        self.created_at = time.monotonic()
        self.wait_seconds = 0.0      # total time spent blocked on locks
        self.restarts = 0            # how many times Overflow re run this

    # ---- state helpers -------------------------------------------------

    def is_wounded(self):
        return self.wounded.is_set()

    def mark_wounded(self):
        self.wounded.set()
        self.state = TxState.WOUNDED

    # ---- IPC packaging -------------------------------------------------

    def to_packet(self, reason):
        """
        Turn the transaction into plain JSON data for the Overflow Server.
        We send the full op list because the engine already rolled back
        every write. The Overflow Server starts from a clean slate.
        """
        return {
            "type": "OFFLOAD",
            "origin": "ENGINE_B",
            "reason": reason,
            "tx_id": self.tx_id,
            "timestamp": self.ts,
            "ops": self.ops,
            "wait_seconds": round(self.wait_seconds, 6),
            "restarts": self.restarts + 1,
        }

    @classmethod
    def from_packet(cls, packet):
        """Rebuild a context on the receiving side."""
        ctx = cls(packet["tx_id"], packet["ops"], packet["timestamp"])
        ctx.restarts = packet.get("restarts", 0)
        return ctx

    def __repr__(self):
        return "<Tx %s ts=%s %s>" % (self.tx_id, self.ts, self.state.value)
