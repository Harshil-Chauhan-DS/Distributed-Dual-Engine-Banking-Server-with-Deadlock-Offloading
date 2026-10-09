"""
execution_router.py  (Module 2: Transaction Scheduler, part 2 of 2)
===================================================================
Team Hyphen | Distributed Dual Engine Banking Server

PURPOSE
-------
Sits between the network layer (Module 1) and the two concurrency
engines (Module 3 and Module 4). It does four jobs:

  1. Parse    : raw text -> TransactionScript (via sql_parser.py)
  2. Admit    : stamp the transaction with an id and a logical timestamp
  3. Schedule : hold it in a priority queue with aging
  4. Dispatch : a worker thread pulls it and runs it on Engine A or
                Engine B, chosen by a configuration flag

THREADS
-------
  client threads (Module 1)  ->  submit()  ->  PriorityScheduler
  worker threads (this file) <-  get()     <-  PriorityScheduler
                              -> engine.execute(txn)

A client thread blocks on its TransactionTicket until a worker finishes
the transaction. Many workers run at once, so Engine A and Engine B must
protect their own shared data. That is exactly what Module 3 and 4 do.

ACADEMIC LINKS
--------------
OS Unit 2   : priority scheduling, aging to prevent starvation, worker
              thread pool, producer consumer queue with a condition
              variable, bounded buffer (backpressure), graceful shutdown.
DBMS Unit 5 : the router is the transaction manager front door. The txn
              timestamp issued here is the one Wound Wait uses in Module 4.
"""

from __future__ import annotations

import heapq
import itertools
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from sql_parser import MAX_PRIORITY, MIN_PRIORITY, ParseError, SqlParser, TransactionScript

log = logging.getLogger("router")


# ===========================================================================
# Configuration and result types
# ===========================================================================
class EngineMode(Enum):
    """The configuration flag that picks the concurrency engine."""
    ENGINE_A = "A"   # Strict 2PL with Wait For Graph deadlock detection
    ENGINE_B = "B"   # Wound Wait deadlock prevention


class TxnStatus(Enum):
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"        # rolled back (client asked, or engine decided)
    OFFLOADED = "OFFLOADED"    # sent to the Overflow Server (Module 5)
    ERROR = "ERROR"            # parse failure, timeout, queue full, engine crash


@dataclass(frozen=True)
class RouterConfig:
    """Immutable settings. Build one at startup and hand it to the router."""
    engine_mode: EngineMode = EngineMode.ENGINE_A
    worker_threads: int = 4          # size of the scheduler thread pool
    queue_limit: int = 1000          # bounded buffer: reject when full
    aging_interval_s: float = 2.0    # every N seconds waiting = 1 priority level
    default_timeout_s: float = 30.0  # how long a client waits for its result


@dataclass(frozen=True)
class ExecutionResult:
    """What an engine (or the router) reports back for one transaction."""
    status: TxnStatus
    message: str = ""
    txn_id: int = 0
    engine: str = "-"

    def to_wire(self) -> str:
        """One line reply for the client socket."""
        return f"{self.status.value} txn={self.txn_id} engine={self.engine} {self.message}".rstrip() + "\n"


@dataclass(frozen=True)
class ScheduledTransaction:
    """A parsed transaction plus the identity the router gave it."""
    txn_id: int
    script: TransactionScript
    client_id: str
    submitted_at: float   # time.monotonic() at admission

    @property
    def timestamp(self) -> int:
        """
        Logical timestamp. Ids come from one counter, so a smaller id means
        an older transaction. Wound Wait (Module 4) compares these values.
        """
        return self.txn_id

    @property
    def priority(self) -> int:
        return self.script.priority


# ===========================================================================
# Engine contract. Module 3 and Module 4 implement this interface.
# ===========================================================================
class ExecutionEngine(ABC):
    """Every concurrency engine plugs into the router through this class."""

    name: str = "engine"

    def start(self) -> None:
        """Optional hook. Engine A starts its deadlock daemon here."""

    def stop(self) -> None:
        """Optional hook. Stop background threads here."""

    @abstractmethod
    def execute(self, txn: ScheduledTransaction) -> ExecutionResult:
        """Run one transaction to the end. Called from many threads at once."""


class PlaceholderEngine(ExecutionEngine):
    """
    Stand in used until Module 3 and 4 exist. It does no locking and no
    banking work. It lets you test parsing, scheduling and dispatch alone.
    """

    def __init__(self, name: str, work_seconds: float = 0.01):
        self.name = name
        self._work_seconds = work_seconds

    def execute(self, txn: ScheduledTransaction) -> ExecutionResult:
        log.info("%s runs txn %d (priority %d)", self.name, txn.txn_id, txn.priority)
        time.sleep(self._work_seconds)   # pretend to work
        return ExecutionResult(TxnStatus.COMMITTED, "placeholder engine, no data touched",
                               txn.txn_id, self.name)


# ===========================================================================
# Ticket: the handle a client thread waits on
# ===========================================================================
class TransactionTicket:
    """A tiny future. The worker completes it. The client waits on it."""

    def __init__(self, txn_id: int):
        self.txn_id = txn_id
        self._done = threading.Event()
        self._result: Optional[ExecutionResult] = None

    def complete(self, result: ExecutionResult) -> None:
        self._result = result
        self._done.set()

    def wait(self, timeout: Optional[float] = None) -> Optional[ExecutionResult]:
        """Block until finished. Returns None if the timeout expires."""
        return self._result if self._done.wait(timeout) else None


# ===========================================================================
# Priority scheduler with aging
# ===========================================================================
class QueueFullError(RuntimeError):
    """The bounded queue has no room. The caller should tell the client to retry."""


class SchedulerClosedError(RuntimeError):
    """The router is shutting down and takes no new work."""


@dataclass(order=True)
class _Entry:
    """
    Heap item. Only the first two fields take part in ordering:
      effective_priority  smaller value runs first
      seq                 arrival number, so equal priorities stay FIFO
    Everything else is excluded from comparison with compare=False, so the
    heap never tries to compare two transaction objects.
    """
    effective_priority: int
    seq: int
    txn: ScheduledTransaction = field(compare=False)
    ticket: TransactionTicket = field(compare=False)
    base_priority: int = field(compare=False)
    enqueued_at: float = field(compare=False)


class PriorityScheduler:
    """
    Thread safe min heap guarded by one Condition variable.

    Why not queue.PriorityQueue? It cannot age waiting items. Without
    aging, a steady stream of priority 0 work starves priority 9 work
    forever. Here every aging_interval seconds of waiting lifts an item
    by one level, so each transaction eventually runs.
    """

    def __init__(self, limit: int, aging_interval_s: float):
        self._heap: List[_Entry] = []
        self._cond = threading.Condition()
        self._seq = itertools.count()
        self._limit = limit
        self._aging_interval = aging_interval_s
        self._closed = False

    def put(self, txn: ScheduledTransaction, ticket: TransactionTicket) -> None:
        with self._cond:
            if self._closed:
                raise SchedulerClosedError("router is shutting down")
            if len(self._heap) >= self._limit:
                raise QueueFullError("scheduler queue is full")
            entry = _Entry(
                effective_priority=txn.priority,
                seq=next(self._seq),
                txn=txn,
                ticket=ticket,
                base_priority=txn.priority,
                enqueued_at=time.monotonic(),
            )
            heapq.heappush(self._heap, entry)
            self._cond.notify()          # wake exactly one sleeping worker

    def get(self) -> Optional[_Entry]:
        """
        Block until work exists. Returns None once the scheduler is closed
        and empty, which tells a worker thread to exit.
        """
        with self._cond:
            while not self._heap and not self._closed:
                self._cond.wait()
            if not self._heap:
                return None
            self._apply_aging()
            return heapq.heappop(self._heap)

    def close(self) -> None:
        """Refuse new work. Workers drain what is left, then exit."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def size(self) -> int:
        with self._cond:
            return len(self._heap)

    def _apply_aging(self) -> None:
        """Recompute priorities and rebuild the heap. Caller holds the lock."""
        if self._aging_interval <= 0 or not self._heap:
            return
        now = time.monotonic()
        for e in self._heap:
            boost = int((now - e.enqueued_at) / self._aging_interval)
            e.effective_priority = max(MIN_PRIORITY, e.base_priority - boost)
        heapq.heapify(self._heap)   # O(n), fine for a bounded queue


# ===========================================================================
# The router
# ===========================================================================
class ExecutionRouter:
    """
    Public face of Module 2. Module 1's client handler calls
    execute_script() and writes result.to_wire() back to the socket.
    """

    def __init__(self, config: RouterConfig = RouterConfig(),
                 engines: Optional[Dict[EngineMode, ExecutionEngine]] = None):
        self._config = config
        self._parser = SqlParser()
        self._scheduler = PriorityScheduler(config.queue_limit, config.aging_interval_s)

        # Engine registry. Placeholders fill any gap so the module runs alone.
        self._engines: Dict[EngineMode, ExecutionEngine] = {
            EngineMode.ENGINE_A: PlaceholderEngine("EngineA"),
            EngineMode.ENGINE_B: PlaceholderEngine("EngineB"),
        }
        if engines:
            self._engines.update(engines)

        self._mode = config.engine_mode
        self._mode_lock = threading.Lock()      # guards reads and writes of _mode
        self._id_lock = threading.Lock()
        self._id_counter = itertools.count(1)   # ids start at 1, never repeat
        self._workers: List[threading.Thread] = []
        self._started = False

    # ----------------------------- lifecycle -----------------------------
    def start(self) -> None:
        """Start engine background tasks and the worker thread pool."""
        if self._started:
            return
        self._started = True
        for engine in self._engines.values():
            engine.start()
        for i in range(self._config.worker_threads):
            t = threading.Thread(target=self._worker_loop,
                                 name=f"txn-worker-{i}", daemon=True)
            t.start()
            self._workers.append(t)
        log.info("router started: mode=%s workers=%d",
                 self._mode.value, self._config.worker_threads)

    def stop(self, join_timeout: float = 5.0) -> None:
        """Graceful shutdown. Queued work finishes first."""
        self._scheduler.close()
        for t in self._workers:
            t.join(join_timeout)
        for engine in self._engines.values():
            engine.stop()
        self._started = False
        log.info("router stopped")

    # ----------------------------- configuration -------------------------
    def set_engine_mode(self, mode: EngineMode) -> None:
        """
        Switch the engine flag. A transaction already running keeps its
        engine. Only later dispatches see the new value. Switch while the
        system is idle, because the two engines do not share lock tables.
        """
        with self._mode_lock:
            self._mode = mode
        log.info("engine mode set to %s", mode.value)

    def get_engine_mode(self) -> EngineMode:
        with self._mode_lock:
            return self._mode

    # ----------------------------- client API ----------------------------
    def submit(self, script: TransactionScript, client_id: str = "local") -> TransactionTicket:
        """
        Admit a parsed transaction. Returns at once with a ticket.
        Raises QueueFullError or SchedulerClosedError.
        """
        with self._id_lock:
            txn_id = next(self._id_counter)
        txn = ScheduledTransaction(txn_id, script, client_id, time.monotonic())
        ticket = TransactionTicket(txn_id)
        self._scheduler.put(txn, ticket)
        log.debug("admitted txn %d from %s priority %d", txn_id, client_id, txn.priority)
        return ticket

    def execute_script(self, text: str, client_id: str = "local",
                       timeout: Optional[float] = None) -> ExecutionResult:
        """
        Blocking helper for the network layer: parse, submit, wait.
        Never raises. Every failure comes back as an ERROR result.
        """
        try:
            script = self._parser.parse(text)
        except ParseError as exc:
            return ExecutionResult(TxnStatus.ERROR, f"parse error: {exc}")

        try:
            ticket = self.submit(script, client_id)
        except QueueFullError:
            return ExecutionResult(TxnStatus.ERROR, "server busy, retry later")
        except SchedulerClosedError:
            return ExecutionResult(TxnStatus.ERROR, "server shutting down")

        wait_for = self._config.default_timeout_s if timeout is None else timeout
        result = ticket.wait(wait_for)
        if result is None:
            # The transaction stays queued and may still run. We only stop waiting.
            return ExecutionResult(TxnStatus.ERROR, "timed out waiting for result", ticket.txn_id)
        return result

    def queue_depth(self) -> int:
        return self._scheduler.size()

    # ----------------------------- worker side ---------------------------
    def _worker_loop(self) -> None:
        """Each worker: take the best transaction, run it, report back."""
        while True:
            entry = self._scheduler.get()
            if entry is None:               # closed and drained
                return
            result = self._dispatch(entry.txn)
            entry.ticket.complete(result)

    def _dispatch(self, txn: ScheduledTransaction) -> ExecutionResult:
        """Pick the engine from the flag and run. Engine crashes become ERROR."""
        with self._mode_lock:
            mode = self._mode
        engine = self._engines[mode]
        try:
            return engine.execute(txn)
        except Exception as exc:            # one bad txn must not kill a worker
            log.exception("engine %s crashed on txn %d", engine.name, txn.txn_id)
            return ExecutionResult(TxnStatus.ERROR, f"engine failure: {exc}",
                                   txn.txn_id, engine.name)


# ===========================================================================
# Demo: python execution_router.py
# One worker plus a pre loaded queue makes the priority order visible.
# ===========================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(threadName)s %(message)s")

    router = ExecutionRouter(RouterConfig(worker_threads=1, aging_interval_s=0))
    parser = SqlParser()

    # Submit before start(). Lowest urgency first, so FIFO would run it first.
    tickets = []
    for prio in (9, 5, 0, 5, 2):
        script = parser.parse(f"BEGIN PRIORITY {prio}; TRANSFER A B 10; COMMIT;")
        tickets.append(router.submit(script, "demo"))

    router.start()
    for t in tickets:
        print(t.wait(5).to_wire(), end="")
    router.stop()
