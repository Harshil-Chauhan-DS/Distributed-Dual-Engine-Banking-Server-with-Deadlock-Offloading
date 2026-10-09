"""
sql_parser.py  (Module 2: Transaction Scheduler, part 1 of 2)
=============================================================
Team Hyphen | Distributed Dual Engine Banking Server

PURPOSE
-------
Turns the raw text a client sends into a validated, immutable
TransactionScript object. The execution router (execution_router.py)
only ever sees parsed objects, never raw strings. That keeps every
engine free of string handling and keeps bad input out of the lock
manager.

SUPPORTED GRAMMAR (case insensitive, one statement per line or
separated by semicolons, "--" starts a comment)

    BEGIN [PRIORITY <0..9>]      0 is the highest priority, default is 5
    TRANSFER <src> <dst> <amount>
    BALANCE <account>            read only operation
    COMMIT | ROLLBACK

A script must start with BEGIN and end with exactly one COMMIT or
ROLLBACK. Nothing may follow the final statement.

ACADEMIC LINKS
--------------
DBMS Unit 5 : transaction model (BEGIN, read/write operations, COMMIT,
              ROLLBACK) and the idea that a transaction is a unit of work.
OS Unit 2   : the PRIORITY clause feeds the priority scheduling policy
              used by the router.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Limits. Kept as module constants so a viva examiner can see every rule.
# ---------------------------------------------------------------------------
DEFAULT_PRIORITY = 5
MIN_PRIORITY = 0                      # most urgent
MAX_PRIORITY = 9                      # least urgent
MAX_AMOUNT = Decimal("1000000000")    # sanity cap on one transfer
MAX_STATEMENTS = 100                  # stops a client from sending a giant script


class ParseError(ValueError):
    """Raised when a script breaks the grammar or a validation rule."""


class OpType(Enum):
    """Kinds of data operations inside a transaction."""
    TRANSFER = "TRANSFER"   # writes two accounts (debit and credit)
    BALANCE = "BALANCE"     # reads one account


class FinalAction(Enum):
    """How the transaction asked to end."""
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"


@dataclass(frozen=True)
class Operation:
    """
    One data operation. Frozen so no thread can change it after parsing.

    For TRANSFER: source, target and amount are all set.
    For BALANCE : only source is set (the account being read).
    """
    op_type: OpType
    source: str
    target: Optional[str] = None
    amount: Optional[Decimal] = None

    def read_set(self) -> Tuple[str, ...]:
        """Accounts this operation only reads (needs shared locks later)."""
        return (self.source,) if self.op_type is OpType.BALANCE else ()

    def write_set(self) -> Tuple[str, ...]:
        """Accounts this operation changes (needs exclusive locks later)."""
        if self.op_type is OpType.TRANSFER:
            return (self.source, self.target)
        return ()


@dataclass(frozen=True)
class TransactionScript:
    """A fully validated transaction, ready for the scheduler."""
    priority: int
    operations: Tuple[Operation, ...]
    final_action: FinalAction
    raw_text: str

    def accounts(self) -> Tuple[str, ...]:
        """Every account touched, in first use order, no duplicates."""
        seen: List[str] = []
        for op in self.operations:
            for acct in op.read_set() + op.write_set():
                if acct not in seen:
                    seen.append(acct)
        return tuple(seen)


# ---------------------------------------------------------------------------
# Compiled patterns. One pattern per statement type keeps the parser a
# simple dispatch table instead of a hand written tokenizer.
# ---------------------------------------------------------------------------
_ACCOUNT = r"([A-Za-z0-9_]{1,32})"
_BEGIN_RE = re.compile(r"^BEGIN(?:\s+PRIORITY\s+(\d+))?$", re.IGNORECASE)
_TRANSFER_RE = re.compile(
    rf"^TRANSFER\s+{_ACCOUNT}\s+{_ACCOUNT}\s+(\d+(?:\.\d{{1,2}})?)$",
    re.IGNORECASE,
)
_BALANCE_RE = re.compile(rf"^BALANCE\s+{_ACCOUNT}$", re.IGNORECASE)
_COMMIT_RE = re.compile(r"^COMMIT$", re.IGNORECASE)
_ROLLBACK_RE = re.compile(r"^ROLLBACK$", re.IGNORECASE)


class SqlParser:
    """
    Stateless parser. One instance can be shared by every client thread
    because parse() keeps all of its state in local variables.
    """

    def parse(self, script: str) -> TransactionScript:
        """Parse and validate one whole transaction. Raises ParseError."""
        statements = self._split(script)
        if not statements:
            raise ParseError("empty transaction script")
        if len(statements) > MAX_STATEMENTS:
            raise ParseError(f"too many statements (limit {MAX_STATEMENTS})")

        priority = self._parse_begin(statements[0])

        operations: List[Operation] = []
        final: Optional[FinalAction] = None

        # Statement numbers start at 2 because BEGIN was number 1.
        for number, stmt in enumerate(statements[1:], start=2):
            if final is not None:
                raise ParseError(
                    f"statement {number}: nothing may follow {final.value}"
                )
            if _COMMIT_RE.match(stmt):
                final = FinalAction.COMMIT
            elif _ROLLBACK_RE.match(stmt):
                final = FinalAction.ROLLBACK
            else:
                operations.append(self._parse_operation(stmt, number))

        if final is None:
            raise ParseError("transaction must end with COMMIT or ROLLBACK")
        if not operations:
            raise ParseError("transaction has no operations")

        return TransactionScript(
            priority=priority,
            operations=tuple(operations),
            final_action=final,
            raw_text=script.strip(),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _split(script: str) -> List[str]:
        """Drop comments and blank lines. Split on newlines and semicolons."""
        statements: List[str] = []
        for line in script.splitlines():
            line = line.split("--", 1)[0]          # remove trailing comment
            for piece in line.split(";"):
                piece = " ".join(piece.split())    # collapse inner whitespace
                if piece:
                    statements.append(piece)
        return statements

    @staticmethod
    def _parse_begin(stmt: str) -> int:
        match = _BEGIN_RE.match(stmt)
        if not match:
            raise ParseError("transaction must start with BEGIN")
        if match.group(1) is None:
            return DEFAULT_PRIORITY
        priority = int(match.group(1))
        if not MIN_PRIORITY <= priority <= MAX_PRIORITY:
            raise ParseError(
                f"priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
            )
        return priority

    @staticmethod
    def _parse_operation(stmt: str, number: int) -> Operation:
        match = _TRANSFER_RE.match(stmt)
        if match:
            src, dst, raw_amount = match.groups()
            src, dst = src.upper(), dst.upper()    # account names are case blind
            if src == dst:
                raise ParseError(f"statement {number}: cannot transfer to the same account")
            try:
                amount = Decimal(raw_amount)
            except InvalidOperation:                # regex should prevent this
                raise ParseError(f"statement {number}: bad amount")
            if amount <= 0:
                raise ParseError(f"statement {number}: amount must be positive")
            if amount > MAX_AMOUNT:
                raise ParseError(f"statement {number}: amount too large")
            return Operation(OpType.TRANSFER, src, dst, amount)

        match = _BALANCE_RE.match(stmt)
        if match:
            return Operation(OpType.BALANCE, match.group(1).upper())

        raise ParseError(f"statement {number}: unknown or malformed statement '{stmt}'")


if __name__ == "__main__":
    # Quick manual check: python sql_parser.py
    sample = """
        BEGIN PRIORITY 2;
        TRANSFER A B 50;
        BALANCE B;
        COMMIT;
    """
    parsed = SqlParser().parse(sample)
    print(parsed)
    print("accounts:", parsed.accounts())
