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


class TxnDispatcher:
    def __init__(self, router):
        # router is an ExecutionRouter from execution_router.py
        self.router = router

    def __call__(self, session, line):
        word = line.split()[0].upper()

        # Handy for checking the network layer alone.
        if word == "PING":
            return "OK PONG"

        # Start a new transaction buffer.
        if word == "BEGIN":
            if session.transaction is not None:
                return "ERR NESTED_BEGIN"
            session.transaction = ["BEGIN"]
            return "OK BEGIN"

        # Any other command needs an open transaction.
        if session.transaction is None:
            return "ERR NO_TXN send BEGIN first"

        session.transaction.append(line)
        if word != "COMMIT":
            return "OK QUEUED"

        # COMMIT arrived. Run the whole script now.
        script = "\n".join(session.transaction)
        session.transaction = None          # slot is free for the next txn
        result = self.router.execute_script(
            script, client_id=str(session.session_id)
        )
        if result is None:                  # router gave up waiting
            return "ERR TIMEOUT"
        return result.to_wire()
