"""
Unit tests for the asynchronous abort listener module.

Covers: Listener startup/stop lifecycle, queue-based blocking stream simulation,
whitespace padding/casing handling, event registration, and thread termination.
"""

from __future__ import annotations

import queue
import threading
import time

import pytest

from deploy.abort_listener import ConsoleAbortListener


class MockInputStream:
    """A thread-safe mock input stream that simulates blocking console reads."""

    def __init__(self) -> None:
        self._queue: queue.Queue[str] = queue.Queue()

    def write(self, text: str) -> None:
        """Inject text into the stream."""
        self._queue.put(text)

    def close(self) -> None:
        """Close the stream by injecting an EOF marker (empty string)."""
        self._queue.put("")

    def readline(self) -> str:
        """Read a line from the stream. Blocks until input is injected."""
        return self._queue.get()


class TestAbortListener:
    """Tests for the ConsoleAbortListener class."""

    @pytest.fixture
    def mock_stream(self) -> MockInputStream:
        return MockInputStream()

    @pytest.fixture
    def listener(self, mock_stream: MockInputStream) -> ConsoleAbortListener:
        return ConsoleAbortListener(mock_stream)

    def test_listener_lifecycle(
        self, listener: ConsoleAbortListener, mock_stream: MockInputStream
    ) -> None:
        """Verify listener starts and stops without errors."""
        listener.start()
        # Verify double start is idempotent
        listener.start()

        listener.stop()
        mock_stream.close()  # Allow thread to exit blocking read

    def test_abort_detection_triggers_event(
        self,
        listener: ConsoleAbortListener,
        mock_stream: MockInputStream,
    ) -> None:
        """Verify that typing 'abort' sets the registered abort event."""
        listener.start()
        event = threading.Event()
        listener.set_abort_event(event)

        # Write abort command with varying whitespace and case
        mock_stream.write("  ABorT  \n")

        # Event should be signaled
        assert event.wait(timeout=2.0) is True

        listener.stop()
        mock_stream.close()

    def test_other_commands_ignored(
        self,
        listener: ConsoleAbortListener,
        mock_stream: MockInputStream,
    ) -> None:
        """Verify that other console inputs do not set the event."""
        listener.start()
        event = threading.Event()
        listener.set_abort_event(event)

        # Write unrelated input
        mock_stream.write("status\n")
        mock_stream.write("help\n")

        # Let the thread process
        time.sleep(0.2)
        assert event.is_set() is False

        listener.stop()
        mock_stream.close()

    def test_no_event_registered(
        self,
        listener: ConsoleAbortListener,
        mock_stream: MockInputStream,
    ) -> None:
        """Verify that typing 'abort' is harmless if no event is registered."""
        listener.start()

        # Write abort command
        mock_stream.write("abort\n")

        # Thread should execute without errors
        time.sleep(0.2)

        listener.stop()
        mock_stream.close()

    def test_clear_abort_event(
        self,
        listener: ConsoleAbortListener,
        mock_stream: MockInputStream,
    ) -> None:
        """Verify clear_abort_event unregisters the event so it isn't set."""
        listener.start()
        event = threading.Event()
        listener.set_abort_event(event)
        listener.clear_abort_event()

        mock_stream.write("abort\n")

        time.sleep(0.2)
        assert event.is_set() is False

        listener.stop()
        mock_stream.close()
