"""
client_handler.py
=====================================================
Talks to exactly ONE client over ONE TCP connection.

Responsibilities
----------------
1. Framing:  TCP is a byte stream. It has no message boundaries. We add them
             by ending every message with a newline (a "line protocol").
2. Session:  Keep per client state (id, peer address, open transaction slot).
3. Safety:   Limit line size, enforce an idle timeout, never leak an
             exception to the network, and close the socket exactly once.
4. Hand off: Give each complete line to a pluggable "dispatcher" function.
             Module 2 will replace the stub dispatcher with the real
             SQL parser and execution router.

Wire protocol (text, UTF 8, one message per line)
-------------------------------------------------
    client -> server :  BEGIN | TRANSFER A B 50 | COMMIT | PING | QUIT
    server -> client :  OK ...   or   ERR <CODE> ...

A handler instance is used by ONE worker thread at a time, so most of its
state needs no lock. The two exceptions are close() (the server may call it
from another thread during shutdown) and send() (later modules may push
results from a different thread). Both carry a small lock.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple

log = logging.getLogger("bank.handler")

# ----------------------------------------------------------------------------
# Protocol constants
# ----------------------------------------------------------------------------
ENCODING = "utf-8"
LINE_END = b"\n"
MAX_LINE_BYTES = 4096      # A longer line is treated as abuse and rejected.
RECV_CHUNK = 4096          # Bytes asked from the kernel per recv() call.
DEFAULT_IDLE_TIMEOUT = 60  # Seconds of silence before we drop a client.


class ProtocolError(Exception):
    """Raised when the client breaks the line protocol (too long, bad bytes)."""


# ----------------------------------------------------------------------------
# Session identity
# ----------------------------------------------------------------------------
class _SessionIdGenerator:
    """
    Hands out unique session ids to many threads at once.
    The lock makes "read counter, add one" a single atomic step.
    Without it two threads could read the same value (a race condition).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next = 1

    def next(self) -> int:
        with self._lock:
            value = self._next
            self._next += 1
            return value


_ids = _SessionIdGenerator()


@dataclass
class ClientSession:
    """
    Everything the server remembers about one connected client.
    Later modules attach their own data to this object.
    """

    session_id: int
    peer: Tuple[str, int]
    opened_at: float = field(default_factory=time.monotonic)
    commands_processed: int = 0
    # Slot for the open transaction. Module 2 and Module 4 fill this in
    # (the TransactionContext). Typed as Any so Module 1 has no dependency
    # on code that does not exist yet.
    transaction: Optional[Any] = None


# A dispatcher takes (session, one request line) and returns one response line.
Dispatcher = Callable[[ClientSession, str], str]


def default_dispatcher(session: ClientSession, line: str) -> str:
    """
    Stub used until Module 2 arrives. It lets you test the network layer
    alone with telnet or netcat.
    """
    if line.upper() == "PING":
        return "OK PONG"
    return f"OK ECHO {line}"


# ----------------------------------------------------------------------------
# Line framing
# ----------------------------------------------------------------------------
class LineReader:
    """
    Turns a raw byte stream into complete text lines.

    Why it exists: one recv() call can return half a line, one line, or five
    lines glued together. TCP promises order and delivery. It does not promise
    message sizes. So we keep a buffer and cut lines out of it.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()

    def read_line(self) -> Optional[str]:
        """
        Block until a full line arrives.
        Returns the line without its newline, or None if the peer closed.
        Raises socket.timeout on idle and ProtocolError on bad input.
        """
        while True:
            end = self._buf.find(LINE_END)
            if end >= 0:
                raw = bytes(self._buf[:end])
                del self._buf[: end + 1]           # drop line and newline
                try:
                    return raw.rstrip(b"\r").decode(ENCODING)
                except UnicodeDecodeError as exc:
                    raise ProtocolError("invalid UTF 8") from exc

            # No newline yet. If the buffer is already too big, stop now.
            # Otherwise a client could send endless bytes and eat our memory.
            if len(self._buf) > MAX_LINE_BYTES:
                raise ProtocolError("line too long")

            chunk = self._sock.recv(RECV_CHUNK)
            if not chunk:                          # empty bytes means EOF
                return None
            self._buf.extend(chunk)


# ----------------------------------------------------------------------------
# The handler
# ----------------------------------------------------------------------------
class ClientHandler:
    """Owns one client socket from first byte to close."""

    def __init__(
        self,
        sock: socket.socket,
        peer: Tuple[str, int],
        dispatcher: Dispatcher = default_dispatcher,
        idle_timeout: float = DEFAULT_IDLE_TIMEOUT,
    ) -> None:
        self._sock = sock
        self._sock.settimeout(idle_timeout)  # recv() raises after this long
        self._dispatcher = dispatcher
        self._reader = LineReader(sock)
        self.session = ClientSession(session_id=_ids.next(), peer=peer)

        self._send_lock = threading.Lock()   # serialises writes to the socket
        self._close_lock = threading.Lock()  # makes close() run only once
        self._closed = False

    # ---- main loop --------------------------------------------------------
    def run(self) -> None:
        """
        Serve the client until it quits, times out, errs, or the server stops.
        This is the body of the worker thread while it holds this client.
        """
        s = self.session
        log.info("session %d opened from %s:%d", s.session_id, *s.peer)
        try:
            self._send(f"OK READY session={s.session_id}")
            while True:
                line = self._reader.read_line()
                if line is None:                   # client closed its side
                    break
                line = line.strip()
                if not line:                       # ignore blank lines
                    continue
                if line.upper() == "QUIT":
                    self._send("OK BYE")
                    break
                s.commands_processed += 1
                self._send(self._safe_dispatch(line))

        # Order matters: socket.timeout is a subclass of OSError,
        # so it has to be caught before the general OSError clause.
        except socket.timeout:
            self._try_send("ERR IDLE_TIMEOUT")
        except ProtocolError as exc:
            self._try_send(f"ERR PROTOCOL {exc}")
        except OSError:
            pass  # peer reset the connection, or the server closed us
        finally:
            self.close()
            log.info(
                "session %d closed after %d commands",
                s.session_id,
                s.commands_processed,
            )

    def _safe_dispatch(self, line: str) -> str:
        """
        Run the dispatcher but never let its bugs kill the session.
        A failed command should cost one ERR reply, not the connection.
        """
        try:
            reply = self._dispatcher(self.session, line)
        except Exception:  # noqa: BLE001 (we want every failure here)
            log.exception("dispatcher failed on session %d", self.session.session_id)
            return "ERR INTERNAL"
        # One reply is one line. A stray newline would break the framing.
        return str(reply).replace("\r", " ").replace("\n", " ")

    # ---- I/O helpers ------------------------------------------------------
    def _send(self, text: str) -> None:
        """sendall() loops until every byte is written. send() may write less."""
        data = text.encode(ENCODING) + LINE_END
        with self._send_lock:
            self._sock.sendall(data)

    def _try_send(self, text: str) -> None:
        """Best effort send for error notices. The peer may already be gone."""
        try:
            self._send(text)
        except OSError:
            pass

    def close(self) -> None:
        """
        Idempotent close. Safe to call from any thread.
        shutdown() first: it wakes a thread blocked in recv() on this socket.
        close() alone would not wake it on every platform.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
