"""
bank_dispatcher.py
Glue between the line protocol and the execution router.

client_handler.py gives us ONE line at a time and wants ONE reply line.
The router wants a WHOLE script (BEGIN ... COMMIT) and gives one result.
So this class buffers lines on the session until COMMIT arrives,
then sends the full script to the router and returns its result.

The buffer lives on session.transaction. Each client has its own session,
and one worker thread serves one client at a time, so no lock is needed.
"""


import json


class TxnDispatcher:
    def __init__(self, router, snapshot=None, stats=None):
        # router is an ExecutionRouter from execution_router.py
        self.router = router
        # snapshot() returns {account: balance}. Used only by the SHOW demo command.
        self.snapshot = snapshot
        # stats() returns a dict of engine counters. Used by the STATS command.
        self.stats = stats

    def __call__(self, session, line):
        word = line.split()[0].upper()

        # Handy for checking the network layer alone.
        if word == "PING":
            return "OK PONG"

        # SHOW           -> total money in the system
        # SHOW A B       -> balances of A and B
        # Demo helper. It reads without locks, so use it when the system is quiet.
        if word == "SHOW":
            if self.snapshot is None:
                return "ERR SHOW_UNAVAILABLE"
            bal = self.snapshot()
            names = [n.upper() for n in line.split()[1:]]
            if not names:
                return "OK TOTAL=%d" % sum(bal.values())
            return "OK " + " ".join("%s=%s" % (n, bal.get(n, "?")) for n in names)

        # STATS -> engine counters as one JSON line. The load tester reads it before and
        # after a run and compares it with what the clients saw.
        if word == "STATS":
            if self.stats is None:
                return "ERR STATS_UNAVAILABLE"
            return "OK " + json.dumps(self.stats())

        # Start a new transaction buffer.
        if word == "BEGIN":
            if session.transaction is not None:
                return "ERR NESTED_BEGIN"
            session.transaction = [line]            # keep "BEGIN PRIORITY n" as typed
            return "OK BEGIN"

        # Any other command needs an open transaction.
        if session.transaction is None:
            return "ERR NO_TXN send BEGIN first"

        session.transaction.append(line)
        if word not in ("COMMIT", "ROLLBACK"):
            return "OK QUEUED"

        # COMMIT or ROLLBACK arrived. Run the whole script now.
        # The router sees the final word. A ROLLBACK script never touches a balance.
        script = "\n".join(session.transaction)
        session.transaction = None          # slot is free for the next txn
        result = self.router.execute_script(
            script, client_id=str(session.session_id)
        )
        if result is None:                  # router gave up waiting
            return "ERR TIMEOUT"
        return result.to_wire()
