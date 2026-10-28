"""Morelia-backed runtime driver for one dataflow.

Importing this module does not load Morelia or touch hardware; that happens
only when ``MoreliaRuntime`` is constructed.
"""

from __future__ import annotations

import importlib
import math
import os
import queue
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager, suppress
from dataclasses import replace
from enum import Enum
from functools import partial
from typing import Any

import structlog

from app.config import get_config
from app.diagnostic_events import diagnostic_operation
from app.domain.enums import CommsStatus, DeviceType, SinkType, StreamStatus
from app.runtime_child.driver import (
    DeviceReport,
    ReportCallback,
    RuntimePhase,
    RuntimeReport,
    SinkDeliveryState,
    SinkHealth,
    SinkReport,
)
from app.database import create_database_app, db
from app.models.output_file import OutputFile
from app.runtime_child.sink_factory import RuntimeContext, build_sink, build_sinks
from app.runtime_child.device_connection import constructor_connection_kwargs, apply_or_defer_parameters, current_connection_address, current_connection_present, routing_key
from app.runtime_child.queue_diagnostics import DiagnosticQueue, label_queue
from app.services.connections import EndpointInventoryCache
from app.domain.recovery_progress import normalize_recovery_progress
from app.runtime_host.manifest import Manifest

_log = structlog.get_logger(__name__)

_HARDWARE_ABSENT_ACTIONS = frozenset(
    {
        "unplug_detected",
        "waiting_for_port",
    }
)
_HARDWARE_PRESENT_ACTIONS = frozenset(
    {
        "connection_restored",
        "waiting_for_port_release",
    }
)


class WorkerStartupError(RuntimeError):
    """A stream worker failed before every stream produced its first packet."""

    def __init__(
        self,
        *,
        device_id: str | None,
        sink_id: str | None,
        error_type: str,
        reason: str,
    ) -> None:
        self.device_id = device_id
        self.sink_id = sink_id
        self.error_type = error_type
        self.reason = reason
        target = f"sink {sink_id!r}" if sink_id else f"device {device_id!r}"
        super().__init__(f"startup failed for {target}: {error_type}: {reason}")


@contextmanager
def _database_context(config_overrides=None):
    """App context for a parent-side ``output_files`` read, reusing any current one.

    The runtime child normally already runs inside an app context; recovery can
    also reach here from a watchdog thread that does not, so push a minimal one
    rather than assuming either.
    """
    from flask import has_app_context

    if has_app_context():
        yield
        return
    context = create_database_app(config_overrides=config_overrides).app_context()
    context.push()
    try:
        yield
    finally:
        with suppress(Exception):
            context.pop()


# ---------------------------------------------------------------------------
# Helpers passed into the DataFlow worker process (Windows spawn pickles them) so
# that we can add the sink error report object and track if data is being received 
# in Influx/Quest DB sink
# ---------------------------------------------------------------------------


def resolve_secret_from_env(env_var_name: str) -> str | None:
    """Look up an Influx token from an environment-variable *name*.

    The manifest stores the name, never the token. Called inside the worker
    just before the Influx client is built.
    """
    return os.environ.get(env_var_name)


def open_sink_delivery_outbox(path: str) -> Any:
    """Open a SQLite spool for samples that failed to reach Influx/Quest.

    Path-based so the worker opens its own handle; a live connection cannot
    cross the process boundary. Distinct from the watchdog telemetry outbox.
    """
    from app.watchdog_process.sink_delivery_outbox import SinkDeliveryOutbox

    return SinkDeliveryOutbox(path)


def _normalize_sink_error(event: object) -> dict[str, Any]:
    """Normalize a stream sink-error event into the dashboard contract."""
    if isinstance(event, Mapping):
        get = event.get  # type: ignore[assignment]
    else:
        def get(key: str, default: object = None) -> object:
            return getattr(event, key, default)

    return {
        "source_id": get("source_id"),
        "output_id": get("output_id"),
        "confirmed_samples": get("confirmed_samples"),
        "sink_id": get("sink_id"),
        "sink_class": get("sink_class"),
        "failure_kind": get("failure_kind"),
        "exception_type": get("exception_type"),
        "message": get("message"),
        "state": get("state"),
        "last_success_seq": get("last_success_seq"),
        "timestamp_ns": get("timestamp_ns"),
        "buffered_samples": get("buffered_samples", 0),
        "buffered_bytes": get("buffered_bytes", 0),
        "sample_loss": get("sample_loss", 0),
        "byte_loss": get("byte_loss", 0),
    }


def _normalize_source_status(event: object) -> dict[str, Any]:
    """Convert a source-read status event into a bounded plain dict."""
    if isinstance(event, Mapping):
        get = event.get  # type: ignore[assignment]
    else:
        def get(key: str, default: object = None) -> object:
            return getattr(event, key, default)

    return {
        "source_id": _bounded_text(get("source_id"), 160) or "unknown-source",
        "source_port": _bounded_text(get("source_port"), 160),
        "failure_kind": _bounded_text(get("failure_kind"), 120),
        "exception_type": _bounded_text(get("exception_type"), 120),
        "message": _bounded_text(get("message"), 500),
        "state": _bounded_text(get("state"), 80) or "degraded",
        "consecutive_failures": _sink_counter(get("consecutive_failures")),
        "timestamp_ns": get("timestamp_ns"),
    }


class _SinkErrorSender:
    """Enqueue a sink write failure for the parent to turn into a ``SinkReport``.

    Passed as Morelia's ``on_sink_error``. Separate from watchdog stream health:
    one destination can fail while samples still flow and sibling sinks succeed.
    Must stay picklable (module-level, queue only) and must not raise.
    """

    __slots__ = ("_queue",)

    def __init__(self, sink_error_queue: Any) -> None:
        self._queue = sink_error_queue

    def __call__(self, event: object) -> None:
        self._queue.put(_normalize_sink_error(event))


class _SourceErrorSender:
    """Enqueue a source-read status event for the parent to drain."""

    __slots__ = ("_queue",)

    def __init__(self, source_error_queue: Any) -> None:
        self._queue = source_error_queue

    def __call__(self, event: object) -> None:
        self._queue.put(_normalize_source_status(event))


def _default_sink_error_queue() -> Any:
    """Create a cross-process queue for sink-error events.

    Tests may inject an in-process ``queue.Queue`` instead.
    """
    return DiagnosticQueue("sink_error")


def _default_source_error_queue() -> Any:
    """Create a cross-process queue for source-read status events."""
    return DiagnosticQueue("source_status")


def _sink_field(value: object, limit: int) -> str | None:
    """Return a truncated string field, or ``None`` if empty/non-str."""
    text = _bounded_text(value, limit)
    return text or None


def _sink_counter(value: object) -> int:
    """Return a non-negative int from worker data, else ``0``."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return 0


def _import_morelia() -> tuple[Callable, type, type, type, type, type, type, type]:
    """Import Morelia classes, optionally via ``MORELIA_SRC`` on ``sys.path``."""

    morelia_src = os.environ.get("MORELIA_SRC")
    if morelia_src and morelia_src not in sys.path:
        sys.path.insert(0, morelia_src)

    watchdog_module = importlib.import_module("Morelia.Watchdog.watchdog")
    devices_module = importlib.import_module("Morelia.Devices")
    packet_module = importlib.import_module("Morelia.packet")
    from app.runtime_child.acknowledged_dataflow import AcknowledgedDataFlow
    from app.runtime_child.acknowledged_monitor import AcknowledgedDataFlowMonitor
    from app.output.managed_csv_sink import ManagedCsvSink
    from app.discovery.managed_pods import Pod8206HR, Pod8401HR

    return (
        partial(watchdog_module.Watchdog, dataflow_monitor_factory=AcknowledgedDataFlowMonitor),
        Pod8206HR,
        Pod8401HR,
        AcknowledgedDataFlow,
        ManagedCsvSink,
        devices_module.Preamp,
        packet_module.PrimaryChannelMode,
        packet_module.SecondaryChannelMode,
    )


def preflight_sink_dependencies(manifest: Manifest) -> None:
    """Check that optional packages for this manifest's sink types are installed.

    Only probes sinks listed in the manifest. Raises
    ``SinkDependencyMissing`` for the failing ``sink_id``; does not open
    hardware, files, or network connections.
    """
    from app.runtime_child.sink_factory import _require_dependencies

    for device_flow in manifest.device_flows:
        for sink_config in device_flow.sinks:
            _require_dependencies(sink_config)


class MoreliaRuntime:
    """Owns one Morelia DataFlow + Watchdog for a single dataflow."""

    def __init__(
        self,
        *,
        manifest: Manifest,
        on_report: ReportCallback,
        failure_threshold: int = 3,
        max_heartbeat_age_sec: float = 10.0,
        first_packet_timeout_sec: float | None = None,
        report_interval_sec: float = 3.0,
        stream_interval_sec: float = 1.0,
        timeout_sec: float = 5.0,
        shutdown_timeout_sec: float = 15.0,
        importer: Callable[
            [],
            tuple[Callable, type, type, type, type, type, type, type],
        ] = _import_morelia,
        sink_delivery_outbox_factory: Any | None = None,
        sink_error_queue_factory: Callable[[], Any] = _default_sink_error_queue,
        source_error_queue_factory: Callable[[], Any] = _default_source_error_queue,
    ) -> None:
        self._manifest = manifest
        self._database_config = None
        self._on_report = on_report
        self._failure_threshold = failure_threshold
        self._max_heartbeat_age_sec = max_heartbeat_age_sec
        self._first_packet_timeout_sec = (
            max_heartbeat_age_sec
            if first_packet_timeout_sec is None
            else first_packet_timeout_sec
        )
        self._report_interval_sec = report_interval_sec
        self._stream_interval_sec = stream_interval_sec
        self._timeout_sec = timeout_sec
        self._shutdown_timeout_sec = shutdown_timeout_sec
        self._importer = importer
        # Factories the worker uses to open its own outbox / error queues.
        self._sink_delivery_outbox_factory = sink_delivery_outbox_factory
        self._sink_error_queue_factory = sink_error_queue_factory
        self._source_error_queue_factory = source_error_queue_factory

        self._phase = RuntimePhase.IDLE
        self._sequence = 0
        self._device_ids = tuple(df.device_id for df in manifest.device_flows)
        self._stream_by_device = {
            device_id: index for index, device_id in enumerate(self._device_ids)
        }

        self._pods: list[Any] = []
        # Current pod per stream index. Distinct from _pods, which accumulates
        # every pod ever built (including superseded ones) purely so shutdown
        # can close them all; recovery needs to know which one is live.
        self._pod_by_stream: dict[int, Any] = {}
        self._sinks: list[Any] = []
        self._flowgraph: Any | None = None
        self._watchdog: Any | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._cancelled = lambda: False
        self._local_cancel = threading.Event()
        self._on_recording = None
        self._on_first_data = None
        self._on_runtime_failure = None
        self._recording_queue = None
        self._recording_thread = None
        self._recording_closed = threading.Event()
        self._arrival_seen = threading.Event()
        self.shutdown_result = None
        self._startup_event = threading.Event()
        self._startup_failure: WorkerStartupError | None = None
        self._nonhealthy_streak_by_device = {device_id: 0 for device_id in self._device_ids}
        self._nonhealthy_since_by_device: dict[str, float] = {}

        # Latest sink-write failures, kept separate from device/stream health.
        self._sink_error_queue: Any | None = None
        self._sink_reports: dict[tuple[str, str], SinkReport] = {}
        self._sink_seq: dict[tuple[str, str], int] = {}
        self._sink_recovery_attempted: set[str] = set()
        self._failed_output_ids: set[str] = set()
        self._source_error_queue: Any | None = None
        self._source_status_by_device: dict[str, dict[str, object]] = {}
        self._source_failure_by_device: dict[str, dict[str, object]] = {}
        self._source_recovery_by_device: dict[str, list[dict[str, object]]] = {}
        self._pending_recovery_by_device: dict[str, str] = {}
        self._device_id_by_port = {
            routing_key(str(device_flow.port), native=bool(getattr(device_flow, "connection", None) and device_flow.connection["transport"] == "d2xx")): device_flow.device_id
            for device_flow in manifest.device_flows
        }
        self._port_map_lock = threading.Lock()
        self._presence_inventory = EndpointInventoryCache()

    def bind_lifecycle(self, *, on_recording, cancelled, on_failure=None, on_first_data=None):
        self._on_recording = on_recording
        self._on_first_data = on_first_data
        self._on_runtime_failure = on_failure
        self._cancelled = cancelled

    def request_cancel(self):
        # Never take recovery's lifecycle locks here: native recovery may block.
        self._local_cancel.set()
        self._startup_event.set()
        flow = self._flowgraph
        if flow is not None:
            publish = getattr(flow, "publish_shutdown_deadline", None)
            if callable(publish):
                publish(time.monotonic() + self._shutdown_timeout_sec)
            for event in list(getattr(flow, "_manual_stop_events", [])) + list(getattr(flow, "_registered_stop_events", [])):
                if event is not None:
                    event.set()

    def _check_cancelled(self):
        if self._local_cancel.is_set() or self._cancelled():
            raise RuntimeError("acquisition cancelled")

    def _recording_loop(self):
        from queue import Empty
        while not self._recording_closed.is_set():
            try:
                record = self._recording_queue.get(timeout=.05)
            except Empty:
                continue
            try:
                self._handle_recording_event(record)
            except Exception:
                _log.error("arrival or recording evidence failed", exc_info=True)
                self.request_cancel()
                if self._on_runtime_failure is not None:
                    self._on_runtime_failure()
                return

    def _handle_recording_event(self, record):
        if (record.get("event_type") not in {"pvfs_progress", "pvfs_gap"}
                and record.get("output_id") and record.get("confirmed_samples") is not None):
            from app.repositories.output_files import record_pvfs_progress

            with _database_context(self._database_config):
                record_pvfs_progress(record["output_id"], record["confirmed_samples"])
            if (record.get("first_sample_at") is not None
                    and record.get("last_sample_at") is not None
                    and record["output_id"] not in self._failed_output_ids):
                self._sink_recovery_attempted.discard(record.get("source_id"))
        if record.get("event_type") in {"pvfs_progress", "pvfs_gap"}:
            from app.repositories.output_files import mark_pvfs_recording_gap, record_pvfs_progress

            with _database_context(self._database_config):
                if record["event_type"] == "pvfs_gap":
                    mark_pvfs_recording_gap(
                        record["output_id"], record["confirmed_samples"],
                        record.get("termination_reason", "writer_failure"),
                    )
                else:
                    record_pvfs_progress(record["output_id"], record["confirmed_samples"])
        elif record.get("event_type") == "first_data":
            if self._on_first_data is not None:
                self._on_first_data(record["arrived_at"])
            self._arrival_seen.set()
        elif (self._on_recording is not None and record.get("first_sample_at") is not None
              and record.get("last_sample_at") is not None):
            self._on_recording(record)

    @property
    def phase(self) -> RuntimePhase:
        return self._phase

    def preflight(self) -> None:
        self._check_cancelled()
        self._require(RuntimePhase.IDLE)
        with self._hardware_boundary("preflight"):
            self._build_stack()
            assert self._watchdog is not None
            self._check_cancelled()
            self._watchdog.preflight(timeout_sec=self._timeout_sec)
            self._check_cancelled()
        self._emit_all(StreamStatus.HEALTHY, phase=RuntimePhase.PREFLIGHT)

    def start(self) -> None:
        self._check_cancelled()
        self._require(RuntimePhase.PREFLIGHT)
        assert self._flowgraph is not None
        assert self._watchdog is not None

        with self._hardware_boundary("start"):
            self._flowgraph.collect()
        self._phase = RuntimePhase.RUNNING
        self._check_cancelled()
        self._startup_event.clear()
        self._startup_failure = None
        self._watchdog_thread = threading.Thread(
            # watchdog.run() blocks; keep it off the command thread so we can still ack.
            target=self._run_watchdog,
            name=f"morelia-runtime-{self._manifest.dataflow_id}",
            daemon=True,
        )
        self._watchdog_thread.start()
        first_packet_timeout = self._first_packet_timeout_sec
        if any(
            (flow.connection or {}).get("transport") == "d2xx"
            or (flow.connection is None and flow.parameters.get("use_d2xx", False))
            for flow in self._manifest.device_flows
        ):
            from Morelia.Watchdog.watchdog import NATIVE_READINESS_TIMEOUT_SEC

            first_packet_timeout = max(first_packet_timeout, NATIVE_READINESS_TIMEOUT_SEC)
        startup_timeout = (
            first_packet_timeout
            + self._report_interval_sec
            + self._stream_interval_sec * max(1, self._failure_threshold)
        )
        wait_until = time.monotonic() + startup_timeout
        while not self._startup_event.wait(timeout=.05):
            self._check_cancelled()
            if time.monotonic() >= wait_until:
                break
        if not self._startup_event.is_set():
            self._startup_failure = WorkerStartupError(
                device_id=None,
                sink_id=None,
                error_type="StartupTimeout",
                reason=(
                    "not every stream produced a first packet within "
                    f"{startup_timeout:.1f} seconds"
                ),
            )
        self._check_cancelled()
        if self._startup_failure is not None:
            failure = self._startup_failure
            _log.error(
                "runtime startup failed",
                dataflow_id=self._manifest.dataflow_id,
                device_id=failure.device_id,
                sink_id=failure.sink_id,
                error_type=failure.error_type,
                reason=failure.reason,
            )
            self.close()
            raise failure
        _log.info(
            "runtime startup verified",
            dataflow_id=self._manifest.dataflow_id,
            device_count=len(self._device_ids),
        )

    def recover(self, recovery_id: str, device_id: str) -> None:
        self._check_cancelled()
        self._require(RuntimePhase.RUNNING)
        if device_id not in self._stream_by_device:
            raise ValueError(
                f"unknown device {device_id!r} for dataflow {self._manifest.dataflow_id!r}"
            )
        assert self._watchdog is not None

        stream_index = self._stream_by_device[device_id]
        stream_command = getattr(self._watchdog, "stream_command", None)
        if not callable(stream_command):
            raise RuntimeError(
                "Morelia Watchdog does not expose stream_command()"
            )

        result = stream_command(
            stream_index,
            command="restart_stream",
            requested_by="operator",
            blocking=True,
        )
        if not result.get("ok"):
            raise RuntimeError(
                result.get("error") or result.get("status") or "Morelia recovery failed"
            )

        self._sink_recovery_attempted.discard(device_id)
        self._pending_recovery_by_device[device_id] = recovery_id
        self._emit_from_watchdog_report(
            self._watchdog.get_report(verbose=True),
            recovery_id=recovery_id,
        )

    def stop(self) -> None:
        self.request_cancel()
        if self._teardown_confirmed():
            return self.shutdown_result
        # Cancellation can arrive after stack construction, before preflight
        # succeeds. That IDLE stack still needs the same verified teardown.
        self._require(RuntimePhase.IDLE, RuntimePhase.PREFLIGHT, RuntimePhase.RUNNING, RuntimePhase.STOPPED, RuntimePhase.CLOSED)
        never_launched = (
            getattr(self._flowgraph, "_ever_started_worker", None) is False
            and not getattr(self._flowgraph, "_prior_shutdown_results", ())
            and all(getattr(worker, "_popen", True) is None
                    and getattr(worker, "pid", None) is None
                    for worker in getattr(self._flowgraph, "_workers", ()) if worker is not None)
        )
        shutdown_error = None
        if self._watchdog is not None:
            self._watchdog.stop()
            watchers = [*getattr(self._watchdog, "_stream_watchers", {}).values(),
                        *getattr(self._watchdog, "_device_watchers", {}).values()]
            if any(watcher.is_alive() for watcher in watchers):
                self._cleanup_failed("Watchdog recovery watchers have not exited")
                raise RuntimeError("Watchdog recovery watchers have not exited")
            if self._watchdog_thread is not None:
                self._watchdog_thread.join(
                    timeout=self._timeout_sec + get_config().WATCHDOG_THREAD_JOIN_GRACE_SECONDS
                )
                if self._watchdog_thread.is_alive():
                    self._cleanup_failed("Watchdog report thread has not exited")
                    raise RuntimeError("Watchdog report thread has not exited")
                self._watchdog_thread = None
            shutdown_error: Exception | None = None
            monitor = getattr(self._watchdog, "dataflow_monitor", None)
            stop_dataflow = getattr(monitor, "stop_dataflow", None)
            started_at = time.monotonic()
            if callable(stop_dataflow):
                try:
                    result = stop_dataflow(join_timeout_sec=self._shutdown_timeout_sec)
                    self.shutdown_result = result
                    elapsed = time.monotonic() - started_at
                    self._emit_shutdown_transcript(result, elapsed_seconds=elapsed)
                    ok = not isinstance(result, Mapping) or result.get("ok") is True
                    forced = isinstance(result, Mapping) and result.get("forced_termination") is True
                    if not ok or forced:
                        shutdown_error = RuntimeError(
                            self._shutdown_failure_message(result)
                        )
                except Exception as exc:
                    shutdown_error = exc
                    _log.error(
                        "dataflow_shutdown_failed",
                        dataflow_id=self._manifest.dataflow_id,
                        elapsed_seconds=time.monotonic() - started_at,
                        timeout_seconds=self._shutdown_timeout_sec,
                        error_type=type(exc).__name__,
                        reason=str(exc),
                    )
            # Confirm parent-owned native handles separately from worker exit.
            # Constructors can fail before assigning a handle to pod._port.
            cleanup_errors = self._close_pod_handles()
            if cleanup_errors:
                self.shutdown_result = {**(self.shutdown_result or {}), "ok": False,
                                        "teardown_confirmed": False,
                                        "cleanup_errors": cleanup_errors}
                shutdown_error = RuntimeError("Native handle cleanup remains unresolved")
            elif never_launched and not callable(getattr(self._flowgraph, "worker_inventory_complete", None)):
                self._close_sinks()
                never_started = [{"stream_index": index, "ok": True,
                                  "worker_status": "not_started",
                                  "worker_started": False, "worker_exited": False,
                                  "worker_exitcode": None, "worker_accounted": True,
                                  "descendants_teardown_confirmed": True,
                                  "teardown_confirmed": True}
                                 for index, _ in enumerate(self._manifest.device_flows)]
                self.shutdown_result = {**(self.shutdown_result or {}),
                                        "ok": True, "teardown_confirmed": True,
                                        "no_workers_launched": True,
                                        "worker_inventory_complete": True,
                                        "stream_results": never_started,
                                        "previous_stream_results": []}
                shutdown_error = None
            elif isinstance(self.shutdown_result, Mapping):
                # Earlier handle failure must not poison a later successful
                # retry, nor allow current clean rows to hide old generations.
                self.shutdown_result.pop("cleanup_errors", None)
            if self._teardown_confirmed():
                try:
                    # Legacy Watchdog.close suppresses errors for standalone
                    # clients. The dashboard needs affirmative Manager evidence.
                    from Morelia.Watchdog.dataflowMonitor import DataFlowMonitor
                    if isinstance(monitor, DataFlowMonitor):
                        monitor.close(strict=True)
                    self._watchdog.close()
                except Exception as exc:
                    self._cleanup_failed(f"watchdog close: {exc}")
                    raise
            if shutdown_error is not None:
                raise shutdown_error
        else:
            cleanup_errors = self._close_pod_handles()
            if self._flowgraph is None and not cleanup_errors:
                self.shutdown_result = {"ok": True, "teardown_confirmed": True,
                                        "no_workers_launched": True,
                                        "worker_inventory_complete": True,
                                        "stream_results": [], "previous_stream_results": []}
            elif cleanup_errors:
                self.shutdown_result = {"ok": False, "teardown_confirmed": False,
                                        "cleanup_errors": cleanup_errors}
                raise RuntimeError("Native handle cleanup remains unresolved")
        self._close_sinks()
        self._emit_all(StreamStatus.HEALTHY, phase=RuntimePhase.STOPPED, comms=CommsStatus.STOPPED)

    def close(self) -> None:
        if not self._teardown_confirmed():
            try:
                self.stop()
            except Exception:
                _log.error(
                    "stop during close failed — retaining unresolved ownership",
                    dataflow_id=self._manifest.dataflow_id,
                    phase=self._phase.value,
                    exc_info=True,
                )
        if not self._teardown_confirmed():
            raise RuntimeError("Runtime cleanup is incomplete; a later Stop must retry")
        try:
            self._close_runtime_resources()
        except Exception as exc:
            self._cleanup_failed(f"runtime close: {type(exc).__name__}: {exc}")
            raise
        self.shutdown_result["resources_teardown_confirmed"] = True
        self._phase = RuntimePhase.CLOSED

    def _close_runtime_resources(self) -> None:
        self._recording_closed.set()
        if self._recording_thread is not None:
            self._recording_thread.join(timeout=1)
            if self._recording_thread.is_alive():
                raise RuntimeError("Recording callback thread has not exited")
            self._recording_thread = None
        if self._recording_queue is not None:
            from queue import Empty
            while True:
                try:
                    record = self._recording_queue.get_nowait()
                except Empty:
                    break
                self._handle_recording_event(record)
            self._recording_queue.close()
            self._recording_queue.join_thread()
            self._recording_queue = None
        self._close_sink_error_queue()
        self._close_source_error_queue()
        # Recovery can leave earlier source generations outside the current
        # network. Release their parent-owned command queues after workers stop.
        for pod in self._pods:
            if getattr(pod, "_backend_managed", False):
                pod.cleanup()

    def _cleanup_failed(self, reason):
        self.shutdown_result = {**(self.shutdown_result or {}), "ok": False,
                                "teardown_confirmed": False,
                                "resources_teardown_confirmed": False,
                                "cleanup_errors": [reason]}

    def _close_pod_handles(self):
        errors = []
        for pod in self._pods:
            try:
                close = getattr(pod, "close_port", None)
                if callable(close):
                    close()
            except Exception as exc:
                errors.append(f"{getattr(pod, 'port', '?')}: {type(exc).__name__}: {exc}")
        return errors

    def _teardown_confirmed(self):
        result = self.shutdown_result
        if not isinstance(result, Mapping):
            return False
        if "teardown_confirmed" in result:
            return result["teardown_confirmed"] is True
        rows = [*result.get("stream_results", ()), *result.get("previous_stream_results", ())]
        return (all(row.get("teardown_confirmed") is True or row.get("ok") is True for row in rows)
                if rows else result.get("ok") is True)

    def _emit_shutdown_transcript(self, result: object, *, elapsed_seconds: float) -> None:
        """Print each parent-correlated shutdown action and one stream summary."""
        if not isinstance(result, Mapping):
            return
        stream_results = result.get("stream_results", ())
        if not isinstance(stream_results, (list, tuple)):
            return
        for stream_result in stream_results:
            if not isinstance(stream_result, Mapping):
                continue
            transcript = stream_result.get("transcript", ())
            for record in transcript:
                payload = {
                    "dataflow_id": self._manifest.dataflow_id,
                    "shutdown_id": record.shutdown_id,
                    "action_seq": record.action_seq,
                    "stream_index": record.stream_index,
                    "actor": record.actor,
                    "actor_pid": record.actor_pid,
                    "phase": record.phase.value,
                    "action": record.action,
                    "outcome": record.outcome.value,
                    "emitted_at_ns": record.emitted_at_ns,
                    "elapsed_ms": record.elapsed_ms,
                }
                for field in (
                    "sink_id",
                    "output_id",
                    "worker_exitcode",
                    "error_type",
                    "reason",
                ):
                    value = getattr(record, field, None)
                    if value is not None:
                        payload[field] = value
                _log.info("dataflow_shutdown_action", **payload)

            _log.info(
                "dataflow_shutdown_summary",
                dataflow_id=self._manifest.dataflow_id,
                shutdown_id=stream_result.get("shutdown_id"),
                stream_index=stream_result.get("stream_index"),
                terminal_phase=stream_result.get("terminal_phase"),
                ok=stream_result.get("ok") is True,
                forced_termination=stream_result.get("forced_termination") is True,
                worker_exitcode=stream_result.get("worker_exitcode"),
                action_count=len(transcript) if isinstance(transcript, (list, tuple)) else 0,
                missing_phases=stream_result.get("missing_phases", []),
                elapsed_ms=round(elapsed_seconds * 1000),
            )

    @staticmethod
    def _shutdown_failure_message(result: object) -> str:
        if not isinstance(result, Mapping):
            return "DataFlow graceful shutdown returned no acknowledgement result"
        details = []
        if result.get("error"):
            details.append(str(result["error"]))
        if result.get("terminal_phase"):
            details.append(f"terminal_phase={result['terminal_phase']}")
        missing = result.get("missing_phases")
        if missing:
            details.append("missing_phases=" + ",".join(str(item) for item in missing))
        if result.get("forced_termination"):
            details.append("forced_termination=true")
        stream_results = result.get("stream_results", ())
        if isinstance(stream_results, (list, tuple)):
            for stream_result in stream_results:
                if not isinstance(stream_result, Mapping):
                    continue
                stream_details = []
                if stream_result.get("terminal_phase"):
                    stream_details.append(f"terminal_phase={stream_result['terminal_phase']}")
                missing = stream_result.get("missing_phases")
                if missing:
                    stream_details.append("missing_phases=" + ",".join(str(item) for item in missing))
                if stream_result.get("forced_termination"):
                    stream_details.append("forced_termination=true")
                if stream_details:
                    stream_index = stream_result.get("stream_index", "?")
                    details.append(f"stream_index={stream_index}:" + ";".join(stream_details))
        return "DataFlow graceful shutdown failed" + (": " + "; ".join(details) if details else "")

    def _close_sink_error_queue(self) -> None:
        """Close the sink-error queue if open."""
        q = self._sink_error_queue
        if q is None:
            return
        close = getattr(q, "close", None)
        if callable(close):
            close()
        join_thread = getattr(q, "join_thread", None)
        if callable(join_thread):
            join_thread()
        self._sink_error_queue = None

    def _close_source_error_queue(self) -> None:
        """Close the source-status queue if open."""
        q = self._source_error_queue
        if q is None:
            return
        close = getattr(q, "close", None)
        if callable(close):
            close()
        join_thread = getattr(q, "join_thread", None)
        if callable(join_thread):
            join_thread()
        self._source_error_queue = None

    # -- stack construction -------------------------------------------------

    def _build_stack(self) -> None:
        if self._watchdog is not None:
            return

        # Watchdog threads must use this runtime's database, including factory
        # overrides, when they refresh an address or read sink continuation data.
        from flask import current_app, has_app_context

        if has_app_context():
            self._database_config = {
                "SQLALCHEMY_DATABASE_URI": current_app.config["SQLALCHEMY_DATABASE_URI"],
                "SQLITE_BUSY_TIMEOUT_MS": current_app.config.get("SQLITE_BUSY_TIMEOUT_MS", 5_000),
            }

        (
            Watchdog,
            Pod8206HR,
            Pod8401HR,
            DataFlow,
            CSVSink,
            Preamp,
            PrimaryChannelMode,
            SecondaryChannelMode,
        ) = self._importer()
        network = []
        try:
            sample_rates = tuple(
                self._manifest_sample_rate(device_flow)
                for device_flow in self._manifest.device_flows
            )
            for device_flow in self._manifest.device_flows:
                self._check_cancelled()
                device_type = self._device_type(device_flow)
                construction_flow = device_flow
                if self._manifest.session_id is not None:
                    # Recreating a dashboard host also reuses a historical
                    # manifest. Refresh its transport binding just like a stream
                    # restart; never change the manifest used for reconciliation.
                    port = self._resolve_stream_port(len(network))
                    construction_flow = replace(device_flow, port=port)
                pod = self._build_pod(
                    device_type,
                    Pod8206HR,
                    Pod8401HR,
                    Preamp,
                    PrimaryChannelMode,
                    SecondaryChannelMode,
                    construction_flow,
                )
                # Track immediately so rollback can close the port if sinks fail next.
                self._pods.append(pod)
                self._pod_by_stream[len(network)] = pod
                if construction_flow.port != device_flow.port:
                    self._track_device_port(device_flow.device_id, construction_flow.port)
                runtime_context = self._runtime_context(CSVSink, device_flow, device_type, pod)
                sinks = build_sinks(device_flow.sinks, pod, runtime_context)
                self._sinks.extend(sinks)
                network.append((pod, sinks))

            # Worker calls these; parent drains the queues into reports.
            self._sink_error_queue = self._sink_error_queue_factory()
            sink_error_sender = _SinkErrorSender(self._sink_error_queue)
            self._source_error_queue = self._source_error_queue_factory()
            source_error_sender = _SourceErrorSender(self._source_error_queue)
            for queue in (self._sink_error_queue, self._source_error_queue):
                label_queue(queue, dataflow_id=self._manifest.dataflow_id)

            flowgraph_kwargs = {
                "on_sink_error": sink_error_sender,
                "on_source_error": source_error_sender,
            }
            if getattr(DataFlow, "supports_source_recovery_window", False):
                flowgraph_kwargs["source_recovery_window_sec"] = None
                if getattr(DataFlow, "supports_identity_reconnect", False):
                    flowgraph_kwargs["source_recovery_window_sec"] = self._max_heartbeat_age_sec
                    flowgraph_kwargs["source_reconnect_configs"] = {
                        index: (flow, self._device_type(flow))
                        for index, flow in enumerate(self._manifest.device_flows)
                    }
            self._check_cancelled()
            if getattr(DataFlow, "supports_recording_acknowledgements", False):
                self._recording_queue = DiagnosticQueue("recording_ack", dataflow_id=self._manifest.dataflow_id)
                flowgraph_kwargs["recording_queue"] = self._recording_queue
            self._flowgraph = DataFlow(network, **flowgraph_kwargs)
            self._flowgraph._acquisition_guard = self._check_cancelled
            if self._recording_queue is not None:
                self._recording_thread = threading.Thread(target=self._recording_loop, daemon=True, name="sink-recording")
                self._recording_thread.start()
            self._watchdog = Watchdog(
                flowgraph=self._flowgraph,
                failure_threshold=self._failure_threshold,
                max_heartbeat_age_sec=self._max_heartbeat_age_sec,
                first_packet_timeout_sec=self._first_packet_timeout_sec,
                recovery_policy=self._manifest.policy,
                reconstruction_hook=self._reconstruct_stream,
                sample_rates=sample_rates,
                port_resolver=self._resolve_stream_port,
                port_presence_checker=self._stream_port_present,
            )
        except BaseException:
            self._rollback_stack_construction()
            raise

    # -- recovery reconstruction --------------------------------------------

    def _segment_resume(self, device_flow) -> dict[str, dict[str, object]]:
        """Newest persisted component per file sink of this device flow.

        The watchdog rebuilds a failed stream in the PARENT process, but the
        ``output_files`` row is written by the WORKER when it opens the sink —
        so the parent has no in-memory record of which component is current.
        The database is the only place that identity exists, which is exactly
        why the reconstruction hook belongs to the runtime child (it has DB
        access) rather than the Morelia watchdog library (which does not).

        Returns ``{}`` for a sink that has never opened; that sink then builds
        as a first construction and mints component 0, which is correct.
        """
        resume: dict[str, dict[str, object]] = {}
        with _database_context(self._database_config):
            for sink in device_flow.sinks:
                row = db.session.scalars(
                    db.select(OutputFile)
                    .where(
                        OutputFile.dataflow_id == self._manifest.dataflow_id,
                        OutputFile.device_id == device_flow.device_id,
                        OutputFile.sink_id == sink.sink_id,
                    )
                    .order_by(OutputFile.segment_index.desc())
                ).first()
                if row is None:
                    continue
                resume[sink.sink_id] = {
                    "output_id": row.output_id,
                    "logical_sink_id": row.logical_sink_id,
                    "segment_index": row.segment_index,
                }
        return resume

    def _device_for_source_port(self, source_port: str):
        exact = self._device_id_by_port.get(source_port)
        if exact is not None:
            return exact
        folded = self._device_id_by_port.get(source_port.lower())
        for flow in self._manifest.device_flows:
            if flow.device_id == folded and getattr(flow, "connection", None) and flow.connection["transport"] == "d2xx":
                return None
        return folded

    def _track_device_port(self, device_id: str, port: str) -> None:
        """Update status routing when independently watched streams renumber."""
        with self._port_map_lock:
            self._device_id_by_port = {
                **{address: identity for address, identity in self._device_id_by_port.items()
                   if identity != device_id},
                routing_key(port, native=any(flow.device_id == device_id and getattr(flow, "connection", None) and flow.connection["transport"] == "d2xx" for flow in self._manifest.device_flows)): device_id,
            }

    def _resolve_stream_port(self, stream_index: int) -> str:
        """Refresh the dashboard's observed address without editing run history."""
        device_flow = self._manifest.device_flows[stream_index]
        self._check_cancelled()
        with _database_context(self._database_config):
            return current_connection_address(device_flow, self._device_type(device_flow))

    def _stream_port_present(self, stream_index: int) -> bool:
        device_flow = self._manifest.device_flows[stream_index]
        self._check_cancelled()
        with diagnostic_operation(
            "connection_presence", dataflow_id=getattr(self._manifest, "dataflow_id", None),
            device_id=device_flow.device_id, stream_index=stream_index,
            hardware_id=device_flow.hardware_id, port=device_flow.port,
        ) as evidence:
            with _database_context(self._database_config):
                present = current_connection_present(
                    device_flow, self._device_type(device_flow), inventory=self._presence_inventory,
                )
            evidence["present"] = present
            return present

    def _reconstruct_stream(self, stream_index: int):
        """Rebuild one stream's source and sinks for a watchdog recovery restart.

        Installed as the DataFlowMonitor's ``reconstruction_hook``. Without it
        the monitor replays ``snapshot_config``, which is captured once at
        monitor construction — before any worker has run — so every rebuilt file
        sink carries ``output_id=None`` and tries to create a fresh component 0
        over the path the previous segment's file already occupies. That fails
        with OutputFileAlreadyExistsError on every single attempt, and no retry
        budget can ever clear it.

        Resuming from the persisted component instead routes the sink through
        ``managed_file.allocate_continuation``: the closed segment is never
        reopened or overwritten, and the replacement worker writes the next
        linked component.

        Output: tuple (source, sinks) as the hook contract requires.
        """
        monitor = getattr(self._watchdog, "dataflow_monitor", None)
        flowgraph = getattr(monitor, "flowgraph", self._flowgraph)
        previous = getattr(flowgraph, "_shutdown_results", {}).get(stream_index)
        if previous is None or not (
            previous.get("worker_exited") is True or previous.get("teardown_confirmed") is True
        ):
            raise RuntimeError("Cannot reconstruct a stream before its acquisition worker exits")
        (
            _Watchdog,
            Pod8206HR,
            Pod8401HR,
            _DataFlow,
            CSVSink,
            Preamp,
            PrimaryChannelMode,
            SecondaryChannelMode,
        ) = self._importer()

        device_flow = self._manifest.device_flows[stream_index]
        device_type = self._device_type(device_flow)
        # Only override the transport address for construction. Settings, sink
        # identities and hashes continue to come from the immutable manifest.
        current_port = self._resolve_stream_port(stream_index)
        pod = self._build_pod(
            device_type,
            Pod8206HR,
            Pod8401HR,
            Preamp,
            PrimaryChannelMode,
            SecondaryChannelMode,
            replace(device_flow, port=current_port),
        )

        # Track before building sinks so a sink failure still leaves the port
        # closeable by the rollback path, matching _build_stack's ordering.
        self._pods.append(pod)
        self._pod_by_stream[stream_index] = pod
        self._track_device_port(device_flow.device_id, current_port)
        runtime_context = self._runtime_context(
            CSVSink,
            device_flow,
            device_type,
            pod,
            segment_resume=self._segment_resume(device_flow),
        )
        sinks = build_sinks(device_flow.sinks, pod, runtime_context)
        for sink in sinks:
            sink._predecessor_released = previous.get("teardown_confirmed") is True
            sink._predecessor_worker_exited = previous.get("worker_exited") is True
        self._sinks.extend(sinks)
        return pod, sinks

    def _rollback_stack_construction(self) -> None:
        """Tear down pods/sinks/queues built before a failed stack setup."""
        self._watchdog = None
        self._flowgraph = None
        self._close_sink_error_queue()
        self._close_source_error_queue()

        for sink in reversed(self._sinks):
            close = getattr(sink, "close", None)
            if callable(close):
                with suppress(Exception):
                    close()
        self._sinks.clear()

        for pod in reversed(self._pods):
            close = (getattr(pod, "cleanup", None) if getattr(pod, "_backend_managed", False)
                     else getattr(pod, "close_port", None))
            if not callable(close):
                close = getattr(pod, "close", None)
            if callable(close):
                with suppress(Exception):
                    close()
        self._pods.clear()

    def _runtime_context(
        self,
        CSVSink: type,
        device_flow,
        device_type: DeviceType,
        pod: Any,
        segment_resume: dict[str, dict[str, object]] | None = None,
    ) -> RuntimeContext:
        """Build the per-device context passed into the sink factory.

        ``segment_resume`` is passed only on a recovery rebuild; on a first
        build it stays None so every file sink mints component 0.
        """
        return RuntimeContext(
            dataflow_id=self._manifest.dataflow_id,
            device_id=device_flow.device_id,
            schema_hash=self._manifest.hash,
            session_id=self._manifest.session_id,
            csv_sink_class=CSVSink,
            csv_fieldnames=tuple(self._csv_fieldnames(device_type, pod)),
            secret_resolver=resolve_secret_from_env,
            plot_transport=None,
            sink_delivery_outbox_factory=self._sink_delivery_outbox_factory,
            segment_resume=segment_resume,
        )

    def _build_csv_sink(
        self,
        CSVSink: type,
        device_flow,
        sink_config,
        device_type: DeviceType,
        pod: Any,
    ) -> Any:
        """Build one CSV sink via the shared factory (legacy CSV-only entry)."""
        if sink_config.type != SinkType.CSV:
            raise ValueError(
                f"unsupported Morelia sink type: {sink_config.type.value!r}"
            )
        runtime_context = self._runtime_context(CSVSink, device_flow, device_type, pod)
        return build_sink(sink_config, pod, runtime_context)

    def _build_pod(
        self,
        device_type: DeviceType,
        Pod8206HR: type,
        Pod8401HR: type,
        Preamp: type,
        PrimaryChannelMode: type,
        SecondaryChannelMode: type,
        device_flow,
    ) -> Any:
        if device_type is DeviceType.POD8206HR:
            return self._build_pod8206hr(Pod8206HR, device_flow)
        if device_type is DeviceType.POD8401HR:
            return self._build_pod8401hr(
                Pod8401HR,
                Preamp,
                PrimaryChannelMode,
                SecondaryChannelMode,
                device_flow,
            )
        raise ValueError(f"unsupported Morelia device type: {device_type.value!r}")

    @staticmethod
    def _manifest_sample_rate(device_flow) -> int:
        value = device_flow.parameters.get("sample_rate")
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(
                f"device flow {device_flow.device_id!r} requires a positive integer sample_rate"
            )
        return value

    @staticmethod
    def _build_pod8206hr(Pod8206HR: type, device_flow) -> Any:
        params = dict(device_flow.parameters)
        preamp_gain = params.pop("preamp_gain", params.pop("gain", 10))
        baudrate = params.pop("baudrate", 9600)
        device_name = params.pop("device_name", device_flow.name)
        use_d2xx = params.pop("use_d2xx", False)
        sample_rate = params.pop("sample_rate", None)

        pod = Pod8206HR(
            device_flow.port,
            preamp_gain,
            **constructor_connection_kwargs(device_flow, legacy_baudrate=baudrate, legacy_use_d2xx=use_d2xx),
            device_name=device_name,
            sample_rate=sample_rate,
        )
        apply_or_defer_parameters(pod, params, device_flow)
        return pod

    @staticmethod
    def _build_pod8401hr(
        Pod8401HR: type,
        Preamp: type,
        PrimaryChannelMode: type,
        SecondaryChannelMode: type,
        device_flow,
    ) -> Any:
        params = dict(device_flow.parameters)
        preamp = _enum_member(Preamp, params.pop("preamp"), "preamp")
        primary_values = params.pop("primary_channel_modes")
        primary_channel_modes = tuple(
            _enum_member(PrimaryChannelMode, value, "primary_channel_modes")
            for value in _required_four(primary_values, "primary_channel_modes")
        )
        secondary_values = params.pop("secondary_channel_modes")
        secondary_channel_modes = tuple(
            _enum_member(SecondaryChannelMode, value, "secondary_channel_modes")
            for value in _required_six(secondary_values, "secondary_channel_modes")
        )
        ss_gain = _optional_four(params.pop("ss_gain", (None, None, None, None)), "ss_gain")
        preamp_gain = _optional_four(
            params.pop("preamp_gain", (None, None, None, None)),
            "preamp_gain",
        )
        baudrate = params.pop("baudrate", 9600)
        device_name = params.pop("device_name", device_flow.name)
        use_d2xx = params.pop("use_d2xx", False)
      
        sample_rate = params.pop("sample_rate", None)

        pod = Pod8401HR(
            device_flow.port,
            preamp,
            primary_channel_modes,
            secondary_channel_modes,
            ss_gain=ss_gain,
            preamp_gain=preamp_gain,
            **constructor_connection_kwargs(device_flow, legacy_baudrate=baudrate, legacy_use_d2xx=use_d2xx),
            device_name=device_name,
            sample_rate=sample_rate,
        )
        apply_or_defer_parameters(pod, params, device_flow)
        return pod

    @staticmethod
    def _device_type(device_flow) -> DeviceType:
        candidates = []
        if isinstance(device_flow.device_id, str):
            candidates.append(device_flow.device_id.split(":", 1)[0])
        if isinstance(device_flow.name, str):
            candidates.append(device_flow.name)

        for candidate in candidates:
            try:
                return DeviceType(candidate.lower())
            except ValueError:
                continue
        return DeviceType.POD8206HR

    @staticmethod
    def _csv_fieldnames(device_type: DeviceType, pod: Any) -> list[str]:
        from app.output.csv_schema import csv_fieldnames

        if device_type not in (DeviceType.POD8206HR, DeviceType.POD8401HR):
            raise ValueError(f"unsupported Morelia device type: {device_type.value!r}")
        return list(csv_fieldnames(device_type, pod))

    # -- reports ------------------------------------------------------------

    def _run_watchdog(self) -> None:
        # Daemon thread: log unexpected death so a stalled report feed is visible.
        assert self._watchdog is not None
        try:
            self._run_watchdog_loop()
        except Exception as exc:
            if not self._startup_event.is_set():
                self._startup_failure = WorkerStartupError(
                    device_id=None,
                    sink_id=getattr(exc, "sink_id", None),
                    error_type=type(exc).__name__,
                    reason=str(exc),
                )
                self._startup_event.set()
            if self._on_runtime_failure is not None:
                self._on_runtime_failure()
            _log.error(
                "morelia watchdog thread died",
                dataflow_id=self._manifest.dataflow_id,
                error_type=type(exc).__name__,
                reason=str(exc),
            )
            raise

    def _run_watchdog_loop(self) -> None:
        self._watchdog.run(
            report_interval_sec=self._report_interval_sec,
            stream_interval=self._stream_interval_sec,
            timeout_sec=self._timeout_sec,
            on_result=self._emit_from_watchdog_report,
            verbose=True,
        )

    def _emit_from_watchdog_report(
        self,
        report: dict[str, Any],
        recovery_id: str | None = None,
    ) -> None:
        stream_reports = list(report.get("streams", []))
        if recovery_id is None:
            for index, device_id in enumerate(self._device_ids):
                pending_id = self._pending_recovery_by_device.get(device_id)
                stream = stream_reports[index] if index < len(stream_reports) else {}
                if pending_id and self._map_stream_status(stream.get("stream_health")) is StreamStatus.HEALTHY:
                    recovery_id = pending_id
                    self._pending_recovery_by_device.pop(device_id, None)
                    break
        devices = []
        for index, device_id in enumerate(self._device_ids):
            stream = stream_reports[index] if index < len(stream_reports) else {}
            devices.append(
                DeviceReport(
                    device_id=device_id,
                    stream_status=self._map_stream_status(stream.get("stream_health")),
                )
            )

        self._drain_source_errors()
        diagnostics = self._watchdog_diagnostics(stream_reports)
        self._log_watchdog_status(report, diagnostics)
        self._emit_report(
            phase=self._phase,
            comms=CommsStatus.CURRENT,
            devices=tuple(devices),
            recovery_id=recovery_id,
            diagnostics=diagnostics,
        )
        self._observe_startup(diagnostics)

    def _observe_startup(self, diagnostics: Mapping[str, object]) -> None:
        """Release start only after every worker is alive and has produced data."""
        if self._startup_event.is_set():
            return
        streams = diagnostics.get("streams")
        if not isinstance(streams, list) or len(streams) < len(self._device_ids):
            return

        all_verified = True
        for stream in streams:
            if not isinstance(stream, Mapping):
                all_verified = False
                continue
            worker = stream.get("worker")
            worker = worker if isinstance(worker, Mapping) else {}
            fault = worker.get("fault")
            if isinstance(fault, Mapping):
                self._startup_failure = WorkerStartupError(
                    device_id=_bounded_text(stream.get("device_id"), 120),
                    sink_id=_bounded_text(fault.get("sink_id"), 120),
                    error_type=_bounded_text(fault.get("error_type"), 120)
                    or "WorkerStartupError",
                    reason=_bounded_text(fault.get("reason"), 500)
                    or "the data worker exited during startup",
                )
                self._startup_event.set()
                return
            heartbeat = stream.get("heartbeat")
            heartbeat = heartbeat if isinstance(heartbeat, Mapping) else {}
            packet_count = heartbeat.get("packet_count")
            verified = (
                worker.get("status") == "alive"
                and heartbeat.get("status") == "fresh"
                and isinstance(packet_count, int)
                and not isinstance(packet_count, bool)
                and packet_count > 0
            )
            all_verified = all_verified and verified
        if all_verified and (self._manifest.duration_seconds is None or self._arrival_seen.is_set()):
            self._startup_event.set()

    def _log_watchdog_status(
        self,
        report: dict[str, Any],
        diagnostics: dict[str, object],
    ) -> None:
        """Log a compact watchdog status line (and per-stream details)."""
        watchdog_status = _bounded_text(report.get("watchdog_status"), 80)
        _log.info(
            "watchdog status report",
            component="morelia_watchdog",
            dataflow_id=self._manifest.dataflow_id,
            phase=self._phase.value,
            sequence_number=self._sequence,
            watchdog_status=watchdog_status,
        )

        streams = diagnostics.get("streams")
        if not isinstance(streams, list):
            return
        for stream_index, stream in enumerate(streams):
            if not isinstance(stream, dict):
                continue
            health = stream.get("health")
            recovery = stream.get("recovery")
            recovery = recovery if isinstance(recovery, dict) else {}
            progress = recovery.get("progress") or {}
            attempt = recovery.get("attempt") or {}
            failure = stream.get("failure")
            failure = failure if isinstance(failure, dict) else {}
            heartbeat = stream.get("heartbeat")
            heartbeat = heartbeat if isinstance(heartbeat, dict) else {}
            worker = stream.get("worker")
            worker = worker if isinstance(worker, dict) else {}
            source_read = stream.get("source_read")
            source_read = source_read if isinstance(source_read, dict) else {}
            startup = stream.get("startup")
            startup = startup if isinstance(startup, dict) else {}
            disconnect = stream.get("disconnect")
            disconnect = disconnect if isinstance(disconnect, dict) else {}

            log = _log.info if health == StreamStatus.HEALTHY.value else _log.warning
            log(
                "watchdog stream status",
                component="morelia_watchdog",
                dataflow_id=self._manifest.dataflow_id,
                phase=self._phase.value,
                sequence_number=self._sequence,
                stream_index=stream_index,
                device_id=stream.get("device_id"),
                stream_status=health,
                action=stream.get("action"),
                reason=(
                    stream.get("initiating_failure_reason")
                    or stream.get("failure_reason")
                    or heartbeat.get("reason")
                    or disconnect.get("reason")
                ),
                recovery_stage=progress.get("stage") or recovery.get("status"),
                policy_mode=recovery.get("policy"),
                recovery_attempt=attempt.get("current"),
                failure_count=failure.get("count"),
                failure_threshold=failure.get("threshold"),
                error_message=failure.get("last_error"),
                worker_status=worker.get("status"),
                worker_exitcode=worker.get("exitcode"),
                hardware_present=recovery.get("hardware_present"),
                heartbeat_status=heartbeat.get("status"),
                heartbeat_age_seconds=heartbeat.get("age_sec"),
                source_read_state=source_read.get("state"),
                source_read_error_type=source_read.get("exception_type"),
                source_read_error_message=source_read.get("message"),
                source_read_consecutive_failures=source_read.get("consecutive_failures"),
                first_packet_timeout_seconds=startup.get("timeout_sec"),
                first_packet_remaining_seconds=startup.get("remaining_sec"),
                disconnect_elapsed_seconds=disconnect.get("elapsed_sec"),
                max_heartbeat_age_seconds=disconnect.get("max_heartbeat_age_sec"),
                disconnect_recording_continued=disconnect.get("recording_continued"),
            )

    def _watchdog_diagnostics(self, stream_reports: list[object]) -> dict[str, object]:
        """Project Morelia stream reports into a small operator-facing payload."""
        streams: list[dict[str, object]] = []
        observed_at = time.monotonic()
        for index, device_id in enumerate(self._device_ids):
            raw = stream_reports[index] if index < len(stream_reports) else {}
            stream = raw if isinstance(raw, dict) else {}
            health = self._map_stream_status(stream.get("stream_health"))
            if health is StreamStatus.HEALTHY:
                self._nonhealthy_streak_by_device[device_id] = 0
                self._nonhealthy_since_by_device.pop(device_id, None)
            else:
                self._nonhealthy_streak_by_device[device_id] += 1
                self._nonhealthy_since_by_device.setdefault(device_id, observed_at)

            signals = stream.get("signals")
            signals = signals if isinstance(signals, dict) else {}
            heartbeat = signals.get("heartbeat")
            heartbeat = heartbeat if isinstance(heartbeat, dict) else {}
            worker = signals.get("worker")
            worker = worker if isinstance(worker, dict) else {}
            failure = signals.get("failure")
            failure = failure if isinstance(failure, dict) else {}
            action = stream.get("action")
            action = action if isinstance(action, dict) else {}
            action_detail = action.get("detail")
            action_detail = action_detail if isinstance(action_detail, dict) else {}
            action_name = _bounded_text(action.get("taken"), 120)
            hardware_present = action_detail.get("hardware_present")
            if not isinstance(hardware_present, bool):
                hardware_present = None
            disconnect = action_detail.get("disconnect")
            disconnect = disconnect if isinstance(disconnect, dict) else None
            pending = getattr(self, "_source_failure_by_device", {}).get(device_id)
            if pending is not None and pending.get("restart_required"):
                disconnect = self._complete_source_restart(
                    device_id, pending, health, heartbeat, worker, disconnect
                )
                if disconnect is not None:
                    action_name = "connection_restored"
            source_recoveries = self._source_recovery_by_device.get(device_id, [])
            source_recovery = source_recoveries[0] if source_recoveries else None
            if source_recovery is not None and action_name == "connection_restored":
                if disconnect is None:
                    disconnect = source_recoveries.pop(0)
                else:
                    watchdog_start = disconnect.get("started_at")
                    watchdog_end = disconnect.get("ended_at")
                    if (
                        isinstance(watchdog_start, (int, float))
                        and not isinstance(watchdog_start, bool)
                        and isinstance(watchdog_end, (int, float))
                        and not isinstance(watchdog_end, bool)
                    ):
                        for recovery_index, candidate in enumerate(source_recoveries):
                            candidate_start = candidate.get("started_at")
                            candidate_end = candidate.get("ended_at")
                            if (
                                isinstance(candidate_start, (int, float))
                                and not isinstance(candidate_start, bool)
                                and isinstance(candidate_end, (int, float))
                                and not isinstance(candidate_end, bool)
                                and max(watchdog_start, candidate_start)
                                <= min(watchdog_end, candidate_end)
                            ):
                                source_recoveries.pop(recovery_index)
                                break
            if (
                source_recovery is not None
                and action_name in (None, "", "none")
                and health is StreamStatus.HEALTHY
            ):
                source_recoveries.pop(0)
                action_name = "connection_restored"
                disconnect = source_recovery
            if not source_recoveries:
                self._source_recovery_by_device.pop(device_id, None)
            if hardware_present is None:
                if action_name in _HARDWARE_ABSENT_ACTIONS:
                    hardware_present = False
                elif action_name in _HARDWARE_PRESENT_ACTIONS:
                    hardware_present = True
            recovery_event = stream.get("recovery_event")
            recovery_event = recovery_event if isinstance(recovery_event, dict) else {}
            startup = signals.get("startup")
            startup = startup if isinstance(startup, dict) else {}
            source_read = self._current_source_read(device_id)
            if (
                health is StreamStatus.HEALTHY
                and isinstance(source_read, dict)
                and source_read.get("state") == "recovery_window_expired"
            ):
                self._source_status_by_device.pop(device_id, None)
                source_read = None
            streams.append(
                {
                    "device_id": device_id,
                    "health": health.value,
                    "summary": _bounded_text(stream.get("summary"), 320),
                    "rule": _bounded_text(stream.get("rule"), 120),
                    "failure_reason": _bounded_text(stream.get("failure_reason"), 320),
                    "initiating_failure_reason": _bounded_text(
                        stream.get("initiating_failure_reason"), 320
                    ),
                    "action": action_name,
                    "consecutive_nonhealthy_ticks": self._nonhealthy_streak_by_device[device_id],
                    "nonhealthy_seconds": max(
                        0.0, observed_at - self._nonhealthy_since_by_device.get(device_id, observed_at)
                    ),
                    "failure": {
                        "count": failure.get("count"),
                        "threshold": failure.get("threshold"),
                        "last_error": _bounded_text(failure.get("last_error"), 500),
                    },
                    "heartbeat": {
                        "status": _bounded_text(heartbeat.get("status"), 120),
                        "reason": _bounded_text(heartbeat.get("reason"), 320),
                        "age_sec": heartbeat.get("age_sec"),
                        "max_age_sec": heartbeat.get("max_age_sec"),
                        "packet_count": heartbeat.get("packet_count"),
                    },
                    "worker": {
                        "status": _bounded_text(worker.get("status"), 120),
                        "exitcode": worker.get("exitcode"),
                        # The worker's own account of its death, when it left
                        # one. Without this the report carries an exit code and
                        # nothing else, and a config fault raised inside the
                        # worker reads identically to a device that unplugged.
                        "fault": _worker_fault(worker.get("fault")),
                    },
                    "source_read": source_read,
                    "disconnect": disconnect,
                    "startup": {
                        "elapsed_sec": startup.get("elapsed_sec"),
                        "timeout_sec": startup.get("timeout_sec"),
                        "remaining_sec": startup.get("remaining_sec"),
                    },
                    "recovery": {
                        "progress": normalize_recovery_progress(stream.get("recovery_progress")),
                        "status": _bounded_text(recovery_event.get("status"), 80),
                        "policy": _bounded_text(recovery_event.get("recovery_policy"), 80),
                        "hardware_present": hardware_present,
                        # Restart attempts are independent of report frequency.
                        "attempt": _attempt_counts(recovery_event.get("recovery_attempt")),
                    },
                }
            )
        return {
            "watchdog": {
                "failure_threshold": self._failure_threshold,
                "max_heartbeat_age_seconds": self._max_heartbeat_age_sec,
                "first_packet_timeout_seconds": self._first_packet_timeout_sec,
                "report_interval_seconds": self._report_interval_sec,
                "stream_interval_seconds": self._stream_interval_sec,
                "operation_timeout_seconds": self._timeout_sec,
            },
            "streams": streams,
        }

    def _emit_all(
        self,
        stream_status: StreamStatus,
        *,
        phase: RuntimePhase,
        comms: CommsStatus = CommsStatus.CURRENT,
    ) -> None:
        self._phase = phase
        self._emit_report(
            phase=phase,
            comms=comms,
            devices=tuple(
                DeviceReport(device_id=device_id, stream_status=stream_status)
                for device_id in self._device_ids
            ),
        )

    def _emit_report(
        self,
        *,
        phase: RuntimePhase,
        comms: CommsStatus,
        devices: tuple[DeviceReport, ...],
        recovery_id: str | None = None,
        diagnostics: dict[str, object] | None = None,
    ) -> None:
        # Drain sink failures first; they attach on ``sinks``, not ``devices``.
        self._drain_sink_errors()
        self._on_report(
            RuntimeReport(
                dataflow_id=self._manifest.dataflow_id,
                phase=phase,
                comms=comms,
                devices=devices,
                sequence=self._sequence,
                recovery_id=recovery_id,
                diagnostics=diagnostics,
                sinks=tuple(self._sink_reports.values()),
            )
        )
        self._sequence += 1

    def _drain_sink_errors(self) -> None:
        """Apply pending sink-write failures into ``_sink_reports`` (non-blocking)."""
        q = self._sink_error_queue
        if q is None:
            return
        failed_devices = set()
        while True:
            try:
                event = q.get_nowait()
            except queue.Empty:
                break
            except (OSError, ValueError):
                # Queue already closed during teardown.
                break
            if not isinstance(event, Mapping):
                continue
            report = self._sink_report_from_event(event)
            self._sink_reports[(report.source_id, report.sink_id)] = report
            if event.get("state") == "terminal":
                candidates = [
                    flow for flow in self._manifest.device_flows
                    if any(sink.sink_id == report.sink_id and sink.type is SinkType.PVFS
                           for sink in flow.sinks)
                ]
                matched = [flow for flow in candidates if flow.device_id == report.source_id]
                if not matched and len(candidates) == 1:
                    matched = candidates
                if not matched:
                    continue
                output_id = event.get("output_id")
                if output_id is not None:
                    confirmed = event.get("confirmed_samples")
                    if isinstance(confirmed, int) and not isinstance(confirmed, bool):
                        from app.repositories.output_files import mark_pvfs_recording_gap

                        with _database_context(self._database_config):
                            mark_pvfs_recording_gap(output_id, confirmed, "writer_failure")
                    if output_id in self._failed_output_ids:
                        continue
                    self._failed_output_ids.add(output_id)
                failed_devices.update(flow.device_id for flow in matched)
        policy = getattr(self._manifest.policy, "value", self._manifest.policy)
        if str(policy).lower() != "automate" or self._phase is not RuntimePhase.RUNNING:
            return
        for device_id in sorted(failed_devices):
            if device_id in self._sink_recovery_attempted or self._local_cancel.is_set():
                continue
            self._sink_recovery_attempted.add(device_id)
            try:
                result = self._watchdog.stream_command(
                    self._stream_by_device[device_id], command="restart_stream",
                    requested_by="automate", blocking=True,
                )
            except Exception as exc:
                result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            if not result.get("ok"):
                _log.error("PVFS recovery requires attention", device_id=device_id, result=result)

    def _current_source_read(self, device_id: str) -> dict[str, object] | None:
        """Return this device's source-read status, clearing a stale degraded latch."""
        status = self._source_status_by_device.get(device_id)
        if status is None:
            return None
        if status.get("state") != "degraded":
            return status
        timestamp_ns = status.get("timestamp_ns")
        if not isinstance(timestamp_ns, int) or isinstance(timestamp_ns, bool):
            return status
        age_sec = (time.time_ns() - timestamp_ns) / 1_000_000_000
        if age_sec > get_config().SOURCE_STATUS_STALE_AFTER_SECONDS:
            self._source_status_by_device.pop(device_id, None)
            return None
        return status

    def _complete_source_restart(
        self,
        device_id: str,
        pending: dict[str, object],
        health: StreamStatus,
        heartbeat: dict[str, object],
        worker: dict[str, object],
        disconnect: dict[str, object] | None,
    ) -> dict[str, object] | None:
        """Close a retained interruption after the replacement worker produces real samples."""
        if disconnect is not None:
            physical_end = disconnect.get("ended_at")
            if (
                isinstance(physical_end, (int, float))
                and not isinstance(physical_end, bool)
                and math.isfinite(physical_end)
                and physical_end >= pending["started_at"]
            ):
                pending["watchdog_disconnect"] = disconnect
        count = heartbeat.get("packet_count")
        ended_at = heartbeat.get("last_data_at_epoch")
        if (
            health is not StreamStatus.HEALTHY
            or worker.get("status") != "alive"
            or heartbeat.get("status") != "fresh"
            or not isinstance(count, int)
            or isinstance(count, bool)
            or count <= 0
            or not isinstance(ended_at, (int, float))
            or isinstance(ended_at, bool)
            or not math.isfinite(ended_at)
        ):
            return None
        # Cached heartbeat ages cannot date a sample against a later reader event.
        if ended_at <= pending["expired_at"]:
            return None
        physical = pending.get("watchdog_disconnect") or {}
        started_at = pending["started_at"]
        physical_start = physical.get("started_at")
        if (
            isinstance(physical_start, (int, float))
            and not isinstance(physical_start, bool)
            and math.isfinite(physical_start)
            and physical_start <= ended_at
            and physical_start <= physical["ended_at"]
        ):
            started_at = min(started_at, physical_start)
        else:
            physical = {}
        self._source_failure_by_device.pop(device_id, None)
        self._source_status_by_device.pop(device_id, None)
        return {
            **physical,
            "episode_id": physical.get("episode_id") or pending["episode_id"],
            "started_at": started_at,
            "ended_at": ended_at,
            "elapsed_sec": ended_at - started_at,
            "recording_continued": False,
            "detection_source": "source_reader",
            "reason": "source read interruption",
            "read_failure_count": pending.get("read_failure_count", 0),
        }

    def _drain_source_errors(self) -> None:
        """Apply pending source-read events into ``_source_status_by_device``."""
        q = self._source_error_queue
        if q is None:
            return
        while True:
            try:
                event = q.get_nowait()
            except queue.Empty:
                break
            except (OSError, ValueError):
                break
            if not isinstance(event, Mapping):
                continue
            status = _normalize_source_status(event)
            source_port = status.get("source_port")
            device_id = (
                self._device_for_source_port(source_port)
                if isinstance(source_port, str)
                else None
            )
            if device_id is None:
                source_id = status.get("source_id")
                if isinstance(source_id, str) and source_id in self._stream_by_device:
                    device_id = source_id
            if device_id is None:
                continue
            if status.get("state") == "recovered" and isinstance(source_port, str):
                self._track_device_port(device_id, source_port)
            timestamp_ns = status.get("timestamp_ns")
            if (
                not isinstance(timestamp_ns, int)
                or isinstance(timestamp_ns, bool)
                or timestamp_ns < 0
            ):
                status["timestamp_ns"] = time.time_ns()
            timestamp_ns = status["timestamp_ns"]
            state = status.get("state")
            failures = getattr(self, "_source_failure_by_device", None)
            if failures is None:
                failures = {}
                self._source_failure_by_device = failures
            if state in ("degraded", "recovery_window_expired") and device_id not in failures:
                failures[device_id] = {
                    "episode_id": uuid.uuid4().hex,
                    "started_at": timestamp_ns / 1_000_000_000,
                    "exception_type": status.get("exception_type"),
                }
            if state == "recovery_window_expired":
                # The replacement reader cannot report recovery for the old worker.
                failures[device_id].update(
                    restart_required=True,
                    expired_at=timestamp_ns / 1_000_000_000,
                    read_failure_count=status.get("consecutive_failures", 0),
                )
            # ``recovered``/``healthy`` clears the degraded latch; do not keep error fields.
            if state in ("recovered", "healthy"):
                previous = self._source_status_by_device.pop(device_id, None)
                if state == "healthy" and failures.get(device_id, {}).get("restart_required"):
                    continue
                failure = failures.pop(device_id, None)
                episode_id = failure.get("episode_id") if failure else None
                started_at = failure.get("started_at") if failure else None
                ended_at = timestamp_ns / 1_000_000_000
                if (
                    state == "recovered"
                    and isinstance(episode_id, str)
                    and episode_id
                    and isinstance(started_at, (int, float))
                    and not isinstance(started_at, bool)
                    and isinstance(ended_at, (int, float))
                    and not isinstance(ended_at, bool)
                    and ended_at >= started_at
                ):
                    recoveries = getattr(self, "_source_recovery_by_device", None)
                    if recoveries is None:
                        recoveries = {}
                        self._source_recovery_by_device = recoveries
                    recoveries.setdefault(device_id, []).append(
                        {
                            "episode_id": episode_id,
                            "started_at": float(started_at),
                            "ended_at": float(ended_at),
                            "elapsed_sec": float(ended_at - started_at),
                            "recording_continued": not failure.get("restart_required", False),
                            "detection_source": "source_reader",
                            "reason": "source read interruption",
                            "max_heartbeat_age_sec": getattr(
                                self, "_max_heartbeat_age_sec", None
                            ),
                            "read_failure_count": status.get("consecutive_failures", 0),
                        }
                    )
                if previous is not None:
                    _log.info(
                        "source read recovered",
                        component="morelia_watchdog",
                        dataflow_id=self._manifest.dataflow_id,
                        device_id=device_id,
                        source_read_error_type=previous.get("exception_type"),
                        source_read_consecutive_failures=status.get("consecutive_failures"),
                    )
                continue
            self._source_status_by_device[device_id] = status

    def _sink_report_from_event(self, event: Mapping[str, object]) -> SinkReport:
        """Build a bounded ``SinkReport`` from one normalized sink-error event."""
        source_raw = event.get("source_id")
        source_id = source_raw if isinstance(source_raw, str) and source_raw else "unknown-source"
        sink_raw = event.get("sink_id")
        sink_id = sink_raw if isinstance(sink_raw, str) and sink_raw else "unknown-sink"
        sink_class = _bounded_text(event.get("sink_class"), 120) or "unknown"

        state = event.get("state")
        if state == "degraded":
            health = SinkHealth.DEGRADED
            delivery = SinkDeliveryState.DEGRADED
        elif state in {"recovered", "healthy"}:
            health = SinkHealth.HEALTHY
            delivery = SinkDeliveryState.DELIVERED
        else:
            health = SinkHealth.FAILED
            delivery = SinkDeliveryState.FAILED

        identity = (source_id, sink_id)
        seq = self._sink_seq.get(identity, 0)
        self._sink_seq[identity] = seq + 1

        timestamp_ns = event.get("timestamp_ns")
        if not isinstance(timestamp_ns, int) or isinstance(timestamp_ns, bool) or timestamp_ns < 0:
            timestamp_ns = time.time_ns()

        last_success_seq = event.get("last_success_seq")
        if (
            not isinstance(last_success_seq, int)
            or isinstance(last_success_seq, bool)
            or last_success_seq < 0
        ):
            last_success_seq = None

        return SinkReport(
            sink_id=sink_id,
            source_id=source_id,
            sink_class=sink_class,
            health=health,
            delivery=delivery,
            sequence=seq,
            state_timestamp_ns=timestamp_ns,
            buffered_samples=_sink_counter(event.get("buffered_samples")),
            buffered_bytes=_sink_counter(event.get("buffered_bytes")),
            sample_loss=_sink_counter(event.get("sample_loss")),
            byte_loss=_sink_counter(event.get("byte_loss")),
            failure_kind=_sink_field(event.get("failure_kind"), 120),
            exception_type=_sink_field(event.get("exception_type"), 120),
            message=_sink_field(event.get("message"), 500),
            last_success_seq=last_success_seq,
        )

    @staticmethod
    def _map_stream_status(value: object) -> StreamStatus:
        if value == "healthy":
            return StreamStatus.HEALTHY
        if value == "unhealthy":
            return StreamStatus.UNHEALTHY
        return StreamStatus.SUSPECT

    def _require(self, *allowed: RuntimePhase) -> None:
        if self._phase not in allowed:
            allowed_names = ", ".join(phase.value for phase in allowed)
            raise RuntimeError(
                f"cannot do this from phase {self._phase.value!r}; expected one of: {allowed_names}"
            )

    @contextmanager
    def _hardware_boundary(self, step: str):
        """Log hardware-step failures, then re-raise unchanged."""
        try:
            yield
        except Exception as exc:
            _log.error(
                "morelia hardware step failed",
                dataflow_id=self._manifest.dataflow_id,
                action=step,
                error_type=type(exc).__name__,
                reason=str(exc),
            )
            raise

    def _close_sinks(self) -> None:
        errors = []
        for sink in self._sinks:
            close = getattr(sink, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    errors.append(f"{type(sink).__name__}: {type(exc).__name__}: {exc}")
                    _log.warning(
                        "closing output sink failed — data may be unflushed",
                        dataflow_id=self._manifest.dataflow_id,
                        error=type(exc).__name__,
                        message=str(exc),
                    )
        if errors:
            self._cleanup_failed("; ".join(errors))
            raise RuntimeError("Parent output sink cleanup is incomplete")


def _bounded_text(value: object, limit: int) -> str | None:
    """Truncate a string to ``limit``, or return ``None`` if not a str."""
    if not isinstance(value, str):
        return None
    return value[:limit]


def _attempt_counts(value: object) -> dict[str, int] | None:
    """Project the watcher's ``{"current", "max"}`` restart budget, or ``None``.

    Both fields must be present and non-negative ints: a half-formed budget
    would render as "attempt 2 of ?" and is worse than showing nothing. Bools
    are rejected explicitly — ``isinstance(True, int)`` is True in Python, and a
    stray flag must never render as an attempt count.
    """
    if not isinstance(value, Mapping):
        return None
    counts: dict[str, int] = {}
    for key in ("current", "max"):
        raw = value.get(key)
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
            return None
        counts[key] = raw
    return counts


def _worker_fault(value: object) -> dict[str, object] | None:
    """Project the worker's reported death cause, or ``None``.

    Carries whichever of ``error_type``/``reason`` the worker managed to
    report; a fault with neither says nothing an exit code does not, so it
    projects to None rather than an empty shell the UI would have to special-
    case. ``reason`` is exception text of unbounded length (a path, a driver
    message, occasionally a whole SQL statement) and this rides the report
    wire on every tick, so it is bounded like every other free-text field
    here — see _bounded_text.
    """
    if not isinstance(value, Mapping):
        return None
    error_type = _bounded_text(value.get("error_type"), 120)
    reason = _bounded_text(value.get("reason"), 500)
    if error_type is None and reason is None:
        return None
    exitcode = value.get("worker_exitcode")
    fault = {
        "error_type": error_type,
        "reason": reason,
        "action": _bounded_text(value.get("action"), 120),
        "worker_exitcode": (
            exitcode if isinstance(exitcode, int) and not isinstance(exitcode, bool) else None
        ),
    }
    sink_id = _bounded_text(value.get("sink_id"), 120)
    if sink_id is not None:
        fault["sink_id"] = sink_id
    return fault


def _required_four(value: object, key: str) -> tuple[object, object, object, object]:
    values = _optional_four(value, key)
    if any(item is None for item in values):
        raise ValueError(f"{key} must contain four non-null values")
    return values


def _required_six(
    value: object,
    key: str,
) -> tuple[object, object, object, object, object, object]:
    if not isinstance(value, (list, tuple)) or len(value) != 6:
        raise ValueError(f"{key} must be a 6-tuple")
    values = tuple(value)
    if any(item is None for item in values):
        raise ValueError(f"{key} must contain six non-null values")
    return values  # type: ignore[return-value]


def _optional_four(value: object, key: str) -> tuple[object, object, object, object]:
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        raise ValueError(f"{key} must be a 4-tuple")
    return tuple(value)  # type: ignore[return-value]


def _enum_member(enum_type: type, value: object, key: str) -> Any:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, Enum):
        value = value.name
    if not isinstance(value, str):
        raise ValueError(f"{key} must be an enum member name")
    try:
        return getattr(enum_type, value)
    except AttributeError:
        try:
            return enum_type[value]
        except (KeyError, TypeError):
            raise ValueError(f"unknown {key} value: {value!r}") from None
