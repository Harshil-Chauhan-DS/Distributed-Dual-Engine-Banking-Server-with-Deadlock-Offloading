"""
engine_a_2pl.py  (Module 3, Team Hyphen)

Engine A: pessimistic concurrency control with STRICT Two Phase Locking.

  Growing phase : a txn takes locks as it touches accounts.
  Strict rule   : it keeps ALL locks (S and X) until commit or abort.
  Shrinking     : everything is released in one step, at the very end.

Locks are taken in the order the transaction asks for them. We do NOT sort
accounts. Sorting would prevent deadlocks, and Engine A exists to detect them.

Expected transaction shape (duck typing, so Module 2 objects can plug in):
    txn.txn_id   int
    txn.priority int (optional)
    txn.ops      list of tuples, e.g. ("TRANSFER", "A", "B", 50), ("BALANCE", "A")
"""
import threading
import time
from collections import defaultdict
from enum import Enum

from deadlock_detector import (DeadlockDetector, OverflowClient,
                               WaitForGraph, package_transaction)


class LockMode(Enum):
    SHARED = "S"       # many readers allowed
    EXCLUSIVE = "X"    # one writer, nobody else


class DeadlockVictim(Exception):
    """Raised inside a waiting thread when the detector picked it."""
    def __init__(self, txn_id, cycle):
        super().__init__("txn %s chosen as deadlock victim" % txn_id)
        self.txn_id, self.cycle = txn_id, cycle


class LockTimeout(Exception):
    pass


class InsufficientFunds(Exception):
    pass


class UnknownAccount(Exception):
    pass


class LockManager:
    """
    Lock table plus a single Condition variable.
    One big mutex keeps the logic easy to prove correct. Fine for this scale.
    Grant rule: compatible with every OTHER current holder.
    Known limit: no FIFO queue, so a steady stream of readers can starve a writer.
    """

    def __init__(self, wfg):
        self._cond = threading.Condition(threading.Lock())
        self._holders = {}                    # resource -> {txn: LockMode}
        self._held_by = defaultdict(set)      # txn -> set of resources
        self._waiting = {}                    # txn -> (resource, mode)
        self._victims = {}                    # txn -> cycle
        self._wfg = wfg

    def _blockers(self, txn, res, mode):
        """Holders that stop `txn` from getting `res` in `mode`."""
        others = {t: m for t, m in self._holders.get(res, {}).items() if t != txn}
        if mode is LockMode.SHARED:
            return [t for t, m in others.items() if m is LockMode.EXCLUSIVE]
        return list(others)                   # X conflicts with everyone

    def acquire(self, txn, res, mode, timeout=None):
        """Block until the lock is granted. May raise DeadlockVictim or LockTimeout."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            held = self._holders.get(res, {}).get(txn)
            if held is LockMode.EXCLUSIVE or held is mode:
                return                         # already strong enough
            # Falls through for an S -> X upgrade. Upgrades can deadlock too.
            self._waiting[txn] = (res, mode)
            try:
                while True:
                    if txn in self._victims:   # check first, every wake up
                        raise DeadlockVictim(txn, self._victims.pop(txn))
                    blockers = self._blockers(txn, res, mode)
                    if not blockers:
                        break
                    self._wfg.set_waits(txn, blockers)   # publish wait edges
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise LockTimeout("txn %s timed out on %s" % (txn, res))
                    self._cond.wait(remaining)
                self._holders.setdefault(res, {})[txn] = mode
                self._held_by[txn].add(res)
            finally:
                self._waiting.pop(txn, None)
                self._wfg.clear_waiter(txn)

    def release_all(self, txn):
        """The ONLY release path. Called at commit or after rollback."""
        with self._cond:
            for res in self._held_by.pop(txn, ()):
                holders = self._holders.get(res)
                if holders:
                    holders.pop(txn, None)
                    if not holders:
                        del self._holders[res]
            self._victims.pop(txn, None)
            self._wfg.remove_txn(txn)
            self._cond.notify_all()            # let waiters re check

    def mark_victim(self, txn, cycle):
        """Called by the detector. Returns False if the txn is no longer waiting."""
        with self._cond:
            if txn not in self._waiting:
                return False
            self._victims[txn] = cycle
            self._cond.notify_all()
            return True

    def lock_count(self, txn):
        with self._cond:
            return len(self._held_by.get(txn, ()))


class AccountStore:
    """In memory balances. No lock inside: callers must hold the right 2PL lock."""

    def __init__(self, balances):
        self._bal = dict(balances)

    def has(self, acct):
        return acct in self._bal

    def get(self, acct):
        return self._bal[acct]

    def set(self, acct, value):
        self._bal[acct] = value

    def snapshot(self):
        return dict(self._bal)


class EngineA:
    """Executes whole transactions under strict 2PL and offloads deadlock victims."""

    def __init__(self, balances, overflow_client=None, scan_interval=0.2,
                 lock_timeout=30.0, op_delay=0.0):
        self.store = AccountStore(balances)
        self.wfg = WaitForGraph()
        self.locks = LockManager(self.wfg)
        self.detector = DeadlockDetector(self.wfg, self.locks, scan_interval)
        self.overflow = overflow_client or OverflowClient()
        self.lock_timeout = lock_timeout
        self.op_delay = op_delay               # pause between two lock requests (demos)
        self._stats = {"committed": 0, "aborted": 0, "offloaded": 0,
                       "offload_failed": 0}
        self._stats_lock = threading.Lock()

    def start(self):
        self.detector.start()

    def stop(self):
        self.detector.stop()

    def stats(self):
        with self._stats_lock:
            out = dict(self._stats)
        out["deadlocks_found"] = self.detector.deadlocks_found
        return out

    def _bump(self, key):
        with self._stats_lock:
            self._stats[key] += 1

    # ---------------------------------------------------------------- public
    def execute(self, txn):
        """Run one transaction. Always returns a result dict, never leaks locks."""
        undo = []                              # (account, old balance) pairs
        try:
            results = [self._run_op(txn.txn_id, op, undo) for op in txn.ops]
            self.locks.release_all(txn.txn_id)   # commit point: shrinking phase
            self._bump("committed")
            return {"status": "COMMITTED", "txn_id": txn.txn_id, "results": results}

        except DeadlockVictim as dv:
            self._rollback(undo)               # undo FIRST, while locks still held
            self.locks.release_all(txn.txn_id)
            return self._offload(txn, dv.cycle)

        except (InsufficientFunds, UnknownAccount, LockTimeout, ValueError) as err:
            self._rollback(undo)
            self.locks.release_all(txn.txn_id)
            self._bump("aborted")
            return {"status": "ABORTED", "txn_id": txn.txn_id, "reason": str(err)}

        except Exception:
            self._rollback(undo)               # unknown bug: clean up, then re raise
            self.locks.release_all(txn.txn_id)
            raise

    # --------------------------------------------------------------- private
    def _run_op(self, tid, op, undo):
        name = str(op[0]).upper()
        if name == "TRANSFER":
            _, src, dst, amount = op
            amount = int(amount)
            if amount <= 0:
                raise ValueError("amount must be positive")
            for acct in (src, dst):
                if not self.store.has(acct):
                    raise UnknownAccount(acct)
            self.locks.acquire(tid, src, LockMode.EXCLUSIVE, self.lock_timeout)
            if self.op_delay:
                time.sleep(self.op_delay)      # widen the window so deadlocks show up
            self.locks.acquire(tid, dst, LockMode.EXCLUSIVE, self.lock_timeout)
            src_bal, dst_bal = self.store.get(src), self.store.get(dst)
            if src_bal < amount:
                raise InsufficientFunds("%s has %s, needs %s" % (src, src_bal, amount))
            undo.append((src, src_bal))        # before images for rollback
            undo.append((dst, dst_bal))
            self.store.set(src, src_bal - amount)
            self.store.set(dst, self.store.get(dst) + amount)  # re read: src may equal dst
            return "TRANSFER %s->%s %d ok" % (src, dst, amount)

        if name == "BALANCE":
            acct = op[1]
            if not self.store.has(acct):
                raise UnknownAccount(acct)
            self.locks.acquire(tid, acct, LockMode.SHARED, self.lock_timeout)
            return "BALANCE %s = %s" % (acct, self.store.get(acct))

        raise ValueError("unsupported op: %s" % name)

    def _rollback(self, undo):
        for acct, old in reversed(undo):       # newest change first
            self.store.set(acct, old)

    def _offload(self, txn, cycle):
        """Package the victim and hand it to the Overflow Server. No retry here."""
        package = package_transaction(txn, "ENGINE_A", "DEADLOCK_VICTIM", cycle)
        try:
            reply = self.overflow.send(package)
        except (ConnectionError, ValueError, OSError) as err:
            self._bump("offload_failed")
            return {"status": "OFFLOAD_FAILED", "txn_id": txn.txn_id,
                    "reason": str(err), "cycle": cycle}
        self._bump("offloaded")
        return {"status": "OFFLOADED", "txn_id": txn.txn_id, "cycle": cycle,
                "overflow_reply": reply}


# ------------------------------------------------------------------ self test
if __name__ == "__main__":
    from collections import namedtuple
    Txn = namedtuple("Txn", "txn_id priority ops")

    class StubOverflow:
        """Stands in for Module 5 until it exists."""
        def send(self, pkg):
            print("overflow got txn", pkg["txn_id"], "cycle", pkg["cycle"])
            return {"status": "STUB_OK"}

    engine = EngineA({"A": 500, "B": 500}, overflow_client=StubOverflow(), op_delay=0.3)
    engine.start()
    jobs = [Txn(1, 5, [("TRANSFER", "A", "B", 50)]),
            Txn(2, 5, [("TRANSFER", "B", "A", 20)])]    # opposite order: deadlock
    out = {}
    threads = [threading.Thread(target=lambda t=t: out.update({t.txn_id: engine.execute(t)}))
               for t in jobs]
    for th in threads: th.start()
    for th in threads: th.join()
    for tid in sorted(out): print(tid, out[tid]["status"])
    print("balances", engine.store.snapshot())
    print("stats", engine.stats())
    engine.stop()
