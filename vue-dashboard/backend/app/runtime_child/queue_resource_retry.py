"""Retain a queued frame when Windows cannot start its pipe write."""

import os
import threading
import time


class ResourceRetry:
    """Keep a queue's sending thread on the same message while pipe submissions fail.

    DiagnosticQueue owns the backlog and uses this object for serialized sends.
    A terminal failure prevents later messages from passing the failed message.
    """

    def __init__(self, writer, timeout, identity):
        self.writer = writer
        self.timeout = timeout
        self.identity = identity
        self.cancel = threading.Event()
        self.failure = None
        # DiagnosticQueue installs its error reporter before the sending thread starts.
        self.on_completion_error = None
        self._submitted = False

    def send(self, data):
        """Send one serialized message, retaining it while Windows rejects submission."""
        if self.failure is not None:
            raise OSError(self.failure)
        self._submitted = False
        try:
            if self.cancel.is_set():
                raise OSError("Queue transport retry cancelled")
            if os.name == "nt":
                self._send_windows(data)
            else:
                self.writer.send_bytes(data)
        except Exception as exc:
            # A failed message must not be overtaken by later queued frames.
            self.failure = f"{type(exc).__name__}: {exc}"
            if self._submitted:
                # The reader may already have released this message's queue slot.
                # Raising here would make Queue._feed release that slot twice.
                # Report through the callback; returning does not confirm persistence.
                self.on_completion_error(exc, data)
                return
            raise

    def _start_write(self, data):
        """Retry rejected pipe writes until Windows accepts one or the recovery budget expires."""
        import _winapi

        from app.runtime_child.queue_diagnostics import _message_fields, emit_queue_diagnostic

        # Start the budget at the first rejection and never renew it on another retry.
        started = None
        attempts = 0
        last_failure = None
        while True:
            if self.cancel.is_set():
                raise OSError("Queue transport retry cancelled")
            if started is not None and time.monotonic() - started >= self.timeout:
                raise TimeoutError(f"Queue pipe-write recovery deadline expired: {last_failure}")
            attempts += 1
            try:
                # Keep these bytes in this call so later queue messages cannot overtake them.
                result = _winapi.WriteFile(self.writer.fileno(), data, overlapped=True)
            except OSError as exc:
                # The submission boundary determines retry safety, not an error-code list.
                last_failure = (
                    f"{type(exc).__name__}: {exc}; "
                    f"winerror={getattr(exc, 'winerror', None)}, errno={exc.errno}"
                )
                now = time.monotonic()
                if started is None:
                    started = now
                    emit_queue_diagnostic(
                        "ipc_queue_resource_retry_started", **self.identity,
                        **_message_fields(data), winerror=getattr(exc, "winerror", None),
                        errno=exc.errno, error_type=type(exc).__name__,
                        error_message=str(exc)[:512], retry_seconds=self.timeout,
                    )
                remaining = self.timeout - (now - started)
                if remaining <= 0:
                    raise TimeoutError(
                        f"Queue pipe-write recovery deadline expired: {last_failure}"
                    ) from exc
                # Avoid a busy retry loop while allowing shutdown to interrupt the wait.
                self.cancel.wait(min(0.1, remaining))
                continue
            if started is not None:
                # Recovery here means submission succeeded; completion is checked separately.
                emit_queue_diagnostic(
                    "ipc_queue_resource_retry_recovered", **self.identity,
                    **_message_fields(data), attempts=attempts,
                    elapsed_seconds=time.monotonic() - started,
                )
            return result

    def _send_windows(self, data):
        """Wait for an accepted Windows pipe write without resubmitting its bytes."""
        import _winapi

        writer = self.writer
        if writer._send_ov is not None:
            raise ValueError("Concurrent pipe writes are not supported")
        # Retry only rejected submissions. An accepted write must never be replayed.
        operation, error = self._start_write(data)
        self._submitted = True
        # Preserve PipeConnection's pending-operation handle so closing it can cancel I/O.
        writer._send_ov = operation
        try:
            if error == _winapi.ERROR_IO_PENDING:
                while True:
                    if self.cancel.is_set():
                        raise OSError("Queue transport write cancelled")
                    # Check cancellation between waits so a stalled pipe cannot hide Stop.
                    result = _winapi.WaitForMultipleObjects([operation.event], False, 100)
                    if result == _winapi.WAIT_OBJECT_0:
                        break
                    if result != _winapi.WAIT_TIMEOUT:
                        raise OSError("Pipe write completion wait failed")
        except BaseException:
            operation.cancel()
            raise
        finally:
            writer._send_ov = None
            # Settle completion or cancellation before releasing the message's bytes.
            written, error = operation.GetOverlappedResult(True)
        if error or written != len(data):
            raise OSError(
                f"Pipe write completion failed: error={error}, bytes={written}/{len(data)}"
            )
