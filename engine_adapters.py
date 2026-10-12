"""
engine_adapters.py
Plugs Engine A and Engine B into the ExecutionRouter.

The router hands each engine a ScheduledTransaction (parsed script, Decimal
amounts, router id). The engines want their own shapes:
    Engine A : object with txn_id, priority, ops = [("TRANSFER", src, dst, 50)]
    Engine B : TransactionContext with ops = [{"op": "TRANSFER", "src": ..}]
Each adapter converts the input, runs the engine, then converts the engine's
result dict into an ExecutionResult the router and the client understand.

The reply text carries "wait_ms=<number>". load_tester.py reads that token
to measure lock wait time.
"""

from collections import namedtuple

from execution_router import ExecutionEngine, ExecutionResult, TxnStatus
from sql_parser import FinalAction, OpType
from transaction_context import TransactionContext


def make_balances(prefix="ACC", count=100, balance=1_000_000):
    """Start balances for ACC0..ACC<count-1>, plus A to E for hand tests."""
    accounts = {"%s%d" % (prefix, i): balance for i in range(count)}
    accounts.update({name: balance for name in ("A", "B", "C", "D", "E")})
    return accounts


def _precheck(txn, engine_name):
    """Cases that never need an engine. Returns a result or None."""
    script = txn.script
    if script.final_action is FinalAction.ROLLBACK:
        return ExecutionResult(TxnStatus.ABORTED, "client asked for rollback",
                               txn.txn_id, engine_name)
    for op in script.operations:
        if op.op_type is OpType.TRANSFER and op.amount != int(op.amount):
            return ExecutionResult(TxnStatus.ERROR, "whole amounts only",
                                   txn.txn_id, engine_name)
    return None


def _to_result(out, txn_id, engine_name, wait_ms):
    """Engine result dict -> ExecutionResult. Same status words for A and B."""
    tag = "wait_ms=%.2f" % wait_ms
    status = out.get("status")
    if status == "COMMITTED":
        return ExecutionResult(TxnStatus.COMMITTED, tag, txn_id, engine_name)
    if status == "ABORTED":
        return ExecutionResult(TxnStatus.ABORTED, "%s reason=%s" % (tag, out.get("reason", "")),
                               txn_id, engine_name)
    if status == "OFFLOADED":
        overflow = (out.get("overflow_reply") or {}).get("status", "?")
        return ExecutionResult(TxnStatus.OFFLOADED, "%s overflow=%s" % (tag, overflow),
                               txn_id, engine_name)
    # OFFLOAD_FAILED. Report it as an error so it is not counted as offloaded.
    return ExecutionResult(TxnStatus.ERROR, "%s overflow unreachable: %s" % (tag, out.get("reason", "")),
                           txn_id, engine_name)


# ---------------------------------------------------------------------------
_JobA = namedtuple("_JobA", "txn_id priority ops")


class EngineAAdapter(ExecutionEngine):
    name = "EngineA"

    def __init__(self, engine):
        self.engine = engine

    def start(self):
        self.engine.start()          # starts the deadlock detector thread

    def stop(self):
        self.engine.stop()

    def execute(self, txn):
        early = _precheck(txn, self.name)
        if early:
            return early
        ops = []
        for op in txn.script.operations:
            if op.op_type is OpType.TRANSFER:
                # int(): Decimal cannot go through json.dumps when a txn is offloaded
                ops.append(("TRANSFER", op.source, op.target, int(op.amount)))
            else:
                ops.append(("BALANCE", op.source))
        out = self.engine.execute(_JobA(txn.txn_id, txn.priority, ops))
        # Same thread ran the locks, so its wait total is ours to read.
        wait_ms = self.engine.locks.take_wait() * 1000.0
        return _to_result(out, txn.txn_id, self.name, wait_ms)


class EngineBAdapter(ExecutionEngine):
    name = "EngineB"

    def __init__(self, engine):
        self.engine = engine

    def execute(self, txn):
        early = _precheck(txn, self.name)
        if early:
            return early
        ops = []
        for op in txn.script.operations:
            if op.op_type is OpType.TRANSFER:
                ops.append({"op": "TRANSFER", "src": op.source, "dst": op.target,
                            "amt": int(op.amount)})
            else:
                ops.append({"op": "BALANCE", "acct": op.source})
        # Use the router's id as the timestamp. Smaller id = older = wins.
        ctx = TransactionContext(txn.txn_id, ops, txn.timestamp)
        out = self.engine.execute(ctx)
        wait_ms = out.get("wait_seconds", 0.0) * 1000.0
        return _to_result(out, txn.txn_id, self.name, wait_ms)
