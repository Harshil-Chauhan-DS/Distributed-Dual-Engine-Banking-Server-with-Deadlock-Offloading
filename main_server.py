"""
main_server.py
Entry point of the Distributed Dual Engine Banking Server.

Threading model: ACCEPTOR + FIXED WORKER POOL
---------------------------------------------
* One acceptor thread (the main thread) does nothing but accept().
* N worker threads are created ONCE at startup and reused for every client.
  This is the "connection pool". It avoids the cost of creating a thread per
  connection and puts a hard cap on how much work runs at once.
* Between them sits a BOUNDED queue (the producer consumer pattern).
  If the queue is full the server says "ERR SERVER_BUSY" and hangs up.
  Shedding load early beats collapsing under it.

Run:      python main_server.py --host 127.0.0.1 --port 9090 --pool 32
Test:     nc 127.0.0.1 9090      then type PING
Stop:     Ctrl+C  (graceful: workers finish, sockets close)
"""

from __future__ import annotations

import argparse
import logging
import queue
import signal
import socket
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Set, Tuple

from client_handler import (
    ClientHandler,
    Dispatcher,
    default_dispatcher,
)

log = logging.getLogger("bank.server")


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class ServerConfig:
    """
    Immutable settings. frozen=True means no thread can change them mid run,
    so every thread can read them without a lock.
    """

    host: str = "127.0.0.1"
    port: int = 9090
    backlog: int = 128           # kernel queue of fully opened connections
    pool_size: int = 32          # worker threads = max live sessions
    pending_limit: int = 64      # accepted sockets waiting for a free worker
    idle_timeout: float = 60.0   # seconds before an idle client is dropped
    accept_timeout: float = 0.5  # lets the accept loop notice a stop request


# A factory builds one handler per accepted socket.
HandlerFactory = Callable[[socket.socket, Tuple[str, int]], ClientHandler]


# ----------------------------------------------------------------------------
# Connection pool
# ----------------------------------------------------------------------------
class ConnectionPool:
    """
    Fixed set of worker threads fed by a bounded queue.

    Shared state and its protection
    -------------------------------
    _pending     queue.Queue   thread safe by design (has its own locks)
    _active      set           guarded by _lock
    _served      int           guarded by _lock
    _rejected    int           guarded by _lock
    _stop        Event         thread safe by design
    """

    def __init__(self, size: int, pending_limit: int, factory: HandlerFactory) -> None:
        self._size = size
        self._factory = factory
        self._pending: "queue.Queue[Tuple[socket.socket, Tuple[str, int]]]" = (
            queue.Queue(maxsize=pending_limit)
        )
        self._workers: List[threading.Thread] = []
        self._active: Set[ClientHandler] = set()
        self._lock = threading.Lock()
        self._served = 0
        self._rejected = 0
        self._stop = threading.Event()

    # ---- lifecycle --------------------------------------------------------
    def start(self) -> None:
        """Spawn all workers up front. They live until shutdown()."""
        for i in range(self._size):
            t = threading.Thread(
                target=self._worker_loop, name=f"worker-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)
        log.info("pool started with %d workers", self._size)

    def shutdown(self, join_timeout: float = 5.0) -> None:
        """Stop taking work, wake blocked workers, and wait for them."""
        self._stop.set()

        # Workers can be stuck inside recv() on a quiet client.
        # Closing the handler's socket makes recv() fail so the worker exits.
        with self._lock:
            live = list(self._active)
        for handler in live:
            handler.close()

        # Sockets that were accepted but never served must be closed too.
        while True:
            try:
                sock, _ = self._pending.get_nowait()
            except queue.Empty:
                break
            self._close_quietly(sock)

        for t in self._workers:
            t.join(timeout=join_timeout)
        log.info("pool stopped")

    # ---- producer side (acceptor thread) ----------------------------------
    def submit(self, sock: socket.socket, peer: Tuple[str, int]) -> bool:
        """
        Offer a new connection to the pool.
        Returns False when the queue is full (the caller must reject).
        put_nowait never blocks, so the acceptor is never stuck here.
        """
        try:
            self._pending.put_nowait((sock, peer))
            return True
        except queue.Full:
            with self._lock:
                self._rejected += 1
            return False

    # ---- consumer side (worker threads) -----------------------------------
    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                # Timeout keeps the loop alive to re check _stop.
                sock, peer = self._pending.get(timeout=0.5)
            except queue.Empty:
                continue

            handler = self._factory(sock, peer)
            with self._lock:
                self._active.add(handler)
            try:
                handler.run()
            except Exception:  # noqa: BLE001  one bad client must not kill a worker
                log.exception("unexpected handler failure")
            finally:
                handler.close()
                with self._lock:
                    self._active.discard(handler)
                    self._served += 1

    # ---- observability ----------------------------------------------------
    def stats(self) -> Dict[str, int]:
        """Snapshot used by logs now and by telemetry in Module 6."""
        with self._lock:
            return {
                "pool_size": self._size,
                "active": len(self._active),
                "queued": self._pending.qsize(),
                "served": self._served,
                "rejected": self._rejected,
            }

    @staticmethod
    def _close_quietly(sock: socket.socket) -> None:
        try:
            sock.close()
        except OSError:
            pass


# ----------------------------------------------------------------------------
# The server
# ----------------------------------------------------------------------------
class BankingServer:
    """Owns the listening socket and the pool."""

    def __init__(
        self,
        config: ServerConfig = ServerConfig(),
        dispatcher: Dispatcher = default_dispatcher,
    ) -> None:
        self._cfg = config
        self._dispatcher = dispatcher
        self._listener: socket.socket | None = None
        self._stop = threading.Event()
        self._pool = ConnectionPool(
            size=config.pool_size,
            pending_limit=config.pending_limit,
            factory=self._make_handler,
        )

    def _make_handler(self, sock: socket.socket, peer: Tuple[str, int]) -> ClientHandler:
        return ClientHandler(sock, peer, self._dispatcher, self._cfg.idle_timeout)

    # ---- control ----------------------------------------------------------
    def stop(self) -> None:
        """Safe inside a signal handler: it only sets a flag."""
        self._stop.set()

    def stats(self) -> Dict[str, int]:
        return self._pool.stats()

    # ---- main loop --------------------------------------------------------
    def serve_forever(self) -> None:
        cfg = self._cfg
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR lets us restart at once without waiting out TIME_WAIT.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((cfg.host, cfg.port))
        listener.listen(cfg.backlog)
        # accept() wakes every so often so we can check the stop flag.
        listener.settimeout(cfg.accept_timeout)
        self._listener = listener

        self._pool.start()
        log.info("listening on %s:%d", cfg.host, cfg.port)

        try:
            while not self._stop.is_set():
                try:
                    sock, peer = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break  # listener closed

                # Our messages are tiny. Disable Nagle so replies leave at once.
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                if not self._pool.submit(sock, peer):
                    self._reject(sock)
        finally:
            self._shutdown()

    def _reject(self, sock: socket.socket) -> None:
        """Tell the client we are full, then hang up. Short timeout on purpose."""
        try:
            sock.settimeout(1.0)
            sock.sendall(b"ERR SERVER_BUSY\n")
        except OSError:
            pass
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _shutdown(self) -> None:
        log.info("shutting down, final stats: %s", self._pool.stats())
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        self._pool.shutdown()


# ----------------------------------------------------------------------------
# Command line entry
# ----------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Banking server, Module 1")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--pool", type=int, default=32, help="worker thread count")
    ap.add_argument("--pending", type=int, default=64, help="wait queue size")
    ap.add_argument("--idle", type=float, default=60.0, help="idle timeout seconds")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )

    server = BankingServer(
        ServerConfig(
            host=args.host,
            port=args.port,
            pool_size=args.pool,
            pending_limit=args.pending,
            idle_timeout=args.idle,
        )
    )

    # Ctrl+C and kill both ask for a graceful stop.
    signal.signal(signal.SIGINT, lambda *_: server.stop())
    signal.signal(signal.SIGTERM, lambda *_: server.stop())

    server.serve_forever()


if __name__ == "__main__":
    main()
