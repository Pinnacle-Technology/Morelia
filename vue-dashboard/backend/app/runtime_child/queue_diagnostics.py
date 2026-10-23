"""Identify failed IPC messages without sending diagnostics through another queue."""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import pickle
import time
from multiprocessing.queues import Queue
from uuid import uuid4


def emit_queue_diagnostic(event: str, **fields) -> None:
    """Write best-effort failure evidence to the watchdog's captured stderr."""
    try:
        record = {**fields, "event": event, "timestamp_ns": time.time_ns(),
                  "producer_pid": os.getpid(), "process_name": mp.current_process().name}
        os.write(2, (json.dumps(record, default=str) + "\n").encode("utf-8"))
    except Exception:
        pass


def _message_fields(obj):
    if isinstance(obj, (bytes, bytearray, memoryview)):
        if len(obj) > 65536:
            return {"message_metadata": "omitted_large_payload"}
        # Only our own serialized IPC messages reach this error hook.
        try:
            obj = pickle.loads(obj)
        except Exception:
            return {"message_metadata": "unavailable"}
    result = {"message_type": type(obj).__name__}
    for key in ("device_id", "source_id", "sink_id", "output_id", "stream_index",
                "shutdown_id", "event_type", "action", "phase", "frame_sequence",
                "kind", "segment", "sequence", "ordinal", "confirmed", "checkpoint_id",
                "first_sample_at", "last_sample_at"):
        value = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
        if isinstance(value, (str, int, float, bool)):
            result[key] = value[:256] if isinstance(value, str) else value
    return result


class _FeederDiagnostics:
    def __init__(self, identity):
        self.identity = identity
        self.send_attempt = 0

    def failed(self, exc, obj):
        try:
            serialized = isinstance(obj, (bytes, bytearray, memoryview))
            fields = {**self.identity, **_message_fields(obj)}
            emit_queue_diagnostic(
                "ipc_queue_feeder_error", **fields,
                stage="pipe_write" if serialized else "serialization",
                send_attempt=self.send_attempt if serialized else None,
                serialized_bytes=len(obj) if serialized else None,
                error_type=type(exc).__name__, error_message=str(exc)[:512],
                winerror=getattr(exc, "winerror", None), errno=getattr(exc, "errno", None),
            )
        except Exception:
            pass
        Queue._on_queue_feeder_error(exc, obj)


class DiagnosticQueue(Queue):
    """Keep normal queue delivery while labeling asynchronous feeder failures."""

    def __init__(self, purpose: str, *, ctx=None, maxsize=0, resource_retry_seconds=0, **identity):
        self._diagnostic_identity = {**identity, "queue_purpose": purpose,
                                     "queue_id": uuid4().hex, "creator_pid": os.getpid()}
        self._resource_retry_seconds = resource_retry_seconds
        super().__init__(maxsize=maxsize, ctx=ctx or mp.get_context())

    def __getstate__(self):
        return super().__getstate__(), self._diagnostic_identity, self._resource_retry_seconds

    def __setstate__(self, state):
        base, self._diagnostic_identity, self._resource_retry_seconds = state
        super().__setstate__(base)

    def _reset(self, after_fork=False):
        super()._reset(after_fork=after_fork)
        diagnostics = _FeederDiagnostics(self._diagnostic_identity)
        send = self._send_bytes
        self._resource_retry = None
        if self._resource_retry_seconds:
            from app.runtime_child.queue_resource_retry import ResourceRetry

            self._resource_retry = ResourceRetry(
                self._writer, self._resource_retry_seconds, self._diagnostic_identity,
            )
            send = self._resource_retry.send

        def send_bytes(data):
            diagnostics.send_attempt += 1
            return send(data)

        # The feeder must not retain the queue itself and prevent its finalizer.
        self._send_bytes = send_bytes
        self._on_queue_feeder_error = diagnostics.failed
        if self._resource_retry is not None:
            retry = self._resource_retry
            reported = False

            def failed(exc, obj):
                nonlocal reported
                retry.failure = retry.failure or f"{type(exc).__name__}: {exc}"
                if not reported:
                    reported = True
                    diagnostics.failed(exc, obj)

            self._on_queue_feeder_error = failed
            retry.on_completion_error = failed

    @property
    def transport_error(self):
        return None if self._resource_retry is None else self._resource_retry.failure

    def cancel_pending_writes(self):
        """Interrupt resource retries after the owner finishes or abandons shutdown."""
        if self._resource_retry is not None:
            self._resource_retry.cancel.set()


def label_queue(queue, **identity):
    """Add stream context before sending, leaving injected test queues unchanged."""
    if isinstance(queue, DiagnosticQueue):
        queue._diagnostic_identity.update(identity)
