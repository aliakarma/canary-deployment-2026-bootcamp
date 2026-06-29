"""
Asynchronous console abort listener for the Canary Deployment Simulator.

Provides a thread-safe listener that runs a background daemon thread to monitor
standard input (or a custom stream) for the `"abort"` command, immediately
signaling registered deployment run events to cancel.
"""

from __future__ import annotations

import sys
import threading
from typing import TYPE_CHECKING

from logging_config import get_logger

if TYPE_CHECKING:
    import io

logger = get_logger(__name__)


class ConsoleAbortListener:
    """Listens asynchronously on a stream for user abort signals.

    Spawns a background thread that continuously reads lines from the input
    stream. If the command ``"abort"`` is entered (case-insensitive), the
    currently registered :class:`threading.Event` is set.
    """

    def __init__(self, input_stream: io.TextIOBase | None = None) -> None:
        """Initialize the listener.

        Args:
            input_stream: Optional file-like stream to read from. Defaults to ``sys.stdin``.
        """
        self._input_stream = input_stream or sys.stdin
        self._current_event: threading.Event | None = None
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_requested = False

    def start(self) -> None:
        """Start the background listener thread.

        Does nothing if the thread is already running.
        """
        with self._lock:
            if self._thread is not None:
                logger.debug("Abort listener thread already running")
                return

            self._stop_requested = False
            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="ConsoleAbortListenerThread",
            )
            self._thread.start()
            logger.info("Asynchronous console abort listener started")

    def stop(self) -> None:
        """Signal the listener thread to stop.

        The thread will stop execution upon the next line read or stream close.
        """
        with self._lock:
            self._stop_requested = True
            self._current_event = None
        logger.debug("Stop requested for abort listener thread")

    def set_abort_event(self, event: threading.Event) -> None:
        """Register the abort event for the active deployment.

        Args:
            event: The :class:`threading.Event` to set on abort command.
        """
        with self._lock:
            self._current_event = event
        logger.debug("Registered new abort event with listener")

    def clear_abort_event(self) -> None:
        """Unregister the current deployment's abort event."""
        with self._lock:
            self._current_event = None
        logger.debug("Cleared active abort event from listener")

    def _run(self) -> None:
        """Core loop for the background reader thread."""
        while True:
            # Check stop request before blocking
            with self._lock:
                if self._stop_requested:
                    break

            try:
                # Blocks synchronously until a newline is read or EOF is reached
                line = self._input_stream.readline()
                if not line:
                    # EOF/closed stream
                    logger.debug("Abort listener: Input stream closed (EOF)")
                    break

                with self._lock:
                    if self._stop_requested:
                        break

                cmd = line.strip().lower()
                if cmd == "abort":
                    with self._lock:
                        if self._current_event is not None:
                            logger.warning(
                                "ABORT command detected from console! "
                                "Setting deployment abort event..."
                            )
                            self._current_event.set()
                        else:
                            logger.debug(
                                "Abort command received but no deployment event was active"
                            )
            except Exception as exc:
                logger.error("Exception in abort listener thread: %s", exc)
                break

        with self._lock:
            self._thread = None
        logger.info("Asynchronous console abort listener stopped")
