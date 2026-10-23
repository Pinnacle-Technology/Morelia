"""Writes session samples to a PVFS file.

Only the DataFlow worker may open the file. The parent builds and rebuilds
sink descriptors, but never opens them.

Do not reopen an existing PVFS container to avoid data overwrite or data loss.
Always claim a new empty path first, then hand that path to PvfsDataFile.create.

After close, a segment is immutable. On error, recover keeps the old
segment and starts a linked continuation (recording.pvfs -> recording.recovery-0001.pvfs).
A clean close finalizes the current segment and does not allocate a continuation.

When use_writer_process=True, native PVFS I/O runs in a child process
this adapter owns. Nothing else may open the same segment.

Methods
    open()            Claim an empty path and create the container. Worker only.
    write_row()/flush Write one sample. Opens on first use if needed.
    get_dict()        Snapshot for rebuilding in another process.
    close()           Finalize the current segment. No continuation.
    recover           Finalize as interrupted; open a linked continuation.
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
import sqlite3
import time
from collections.abc import Callable, Iterable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Full
from threading import Condition, Thread, get_ident, local
from types import SimpleNamespace
from uuid import uuid4

import structlog
from Morelia.shutdown import ShutdownActor, ShutdownOutcome, ShutdownPhase

from app.database import create_database_app, transaction
from app.domain.enums import SinkType
from app.models.output_file import OutputFile
from app.output import managed_file
from app.output.managed_file import ManagedOutputFile
from app.runtime_child.queue_diagnostics import DiagnosticQueue, emit_queue_diagnostic
from app.output.recording_bounds import bind_recording

_log = structlog.get_logger(__name__)

# How long to wait for a writer child to drain and exit cleanly before it is
# force-terminated and the segment is marked forcibly closed.
_WRITER_JOIN_TIMEOUT = 15.0
_WRITER_TERMINATE_TIMEOUT = 2.0
_CHECKPOINT_TIMEOUT = 10.0
_RESOURCE_RETRY_SECONDS = 35.0
_WRITER_BUFFER_SECONDS = 35.0

_DEFAULT_SAMPLE_RATE = 400.0

# Retain the excluded 8274D conversion until its descriptor schema is decided.
_LEGACY_8274_PACKET_FIELD_FOR_LABEL: dict[str, str] = {
    "EEG1": "ch0",
    "EEG2": "ch1",
    "EEG3/EMG": "ch2",
    "A": "ch0",
    "B": "ch1",
    "C": "ch2",
    "D": "ch3",
    "length_in_bytes": "length_in_bytes",
    "data": "data",
}


class ManagedPvfsSinkError(Exception):
    """Raised when a PVFS segment cannot be allocated, opened, or reconstructed."""


def _pvfs_writer_target(commands, receipts, config, segment, shutdown_reporter=None):
    """Preserve sample positions across missing frames and confirm persisted output."""
    legacy_writer = state = None
    consumed = ordinal = durable = 0
    missing_frames = skipped_frames = 0
    diagnostic_identity = getattr(commands, "_diagnostic_identity", {})

    def emit(kind, **details):
        nonlocal ordinal
        ordinal += 1
        confirmed = 0 if state is None else state["confirmed"]
        receipts.put({"kind": kind, "segment": segment, "ordinal": ordinal,
                      "consumed": consumed, "confirmed": confirmed,
                      "last_sequence": confirmed, **details})

    def publish(ack):
        nonlocal durable
        if ack is not None:
            emit("written", first_sample_at=ack["first_sample_at"],
                 last_sample_at=ack["last_sample_at"])
            durable = ordinal

    def report(action):
        if shutdown_reporter is not None:
            shutdown_reporter.emit(
                ShutdownPhase.SINKS_FINALIZING, action,
                outcome=ShutdownOutcome.COMPLETED, actor=ShutdownActor.PVFS_WRITER,
                actor_pid=os.getpid(),
            )

    def fill_missing_through(sequence, deadline, command):
        nonlocal consumed, missing_frames
        if sequence <= consumed:
            return
        emit_queue_diagnostic(
            "pvfs_writer_sequence_gap", **diagnostic_identity,
            expected_sequence=consumed + 1, received_sequence=command.get("sequence"),
            missing_last_sequence=sequence, command_kind=command["kind"],
            missing_frame_count=sequence - consumed, resolution="fill_nan",
        )
        placeholder = {"values": (float("nan"),) * len(config["channels"]),
                       "recorded": False}
        # Missing positions advance the sample clock but cannot prove real data arrived.
        while consumed < sequence:
            if time.monotonic() >= deadline:
                raise ManagedPvfsSinkError("PVFS gap fill deadline expired")
            consumed += 1
            ack = ManagedPvfsSink._append_legacy_writer_frame(legacy_writer, state, placeholder)
            missing_frames += 1
            publish(ack)

    try:
        legacy_writer, state = ManagedPvfsSink._create_legacy_writer(config)
        emit("ready")
        while True:
            command = commands.get()
            if not isinstance(command, dict) or command.get("segment") != segment:
                raise ManagedPvfsSinkError("PVFS command identity mismatch")
            kind = command.get("kind")
            if kind == "frame":
                if set(command) != {"kind", "segment", "sequence", "values", "recorded"}:
                    raise ManagedPvfsSinkError("PVFS frame schema mismatch")
                if type(command["sequence"]) is not int or command["sequence"] <= 0:
                    raise ManagedPvfsSinkError("PVFS frame sequence is invalid")
                if (type(command["recorded"]) is not bool
                        or not isinstance(command["values"], (tuple, list))
                        or len(command["values"]) != len(config["channels"])
                        or any(type(value) is not float or math.isinf(value) or
                               (command["recorded"] and math.isnan(value))
                               for value in command["values"])):
                    raise ManagedPvfsSinkError("PVFS frame values are not normalized")
                if command["sequence"] <= consumed:
                    skipped_frames += 1
                    emit_queue_diagnostic(
                        "pvfs_writer_late_frame", **diagnostic_identity,
                        received_sequence=command["sequence"], last_received_sequence=consumed,
                        resolution="skip", skipped_frame_count=skipped_frames,
                    )
                    continue
                fill_missing_through(command["sequence"] - 1,
                                     time.monotonic() + _CHECKPOINT_TIMEOUT, command)
                consumed += 1
                publish(ManagedPvfsSink._append_legacy_writer_frame(legacy_writer, state, command))
            elif kind in {"flush", "close"}:
                if set(command) != {"kind", "segment", "last_sequence", "frame_count",
                                    "checkpoint_id", "deadline"}:
                    raise ManagedPvfsSinkError("PVFS barrier schema mismatch")
                if (any(type(command[key]) is not int for key in ("last_sequence", "frame_count"))
                        or command["last_sequence"] != command["frame_count"]
                        or command["last_sequence"] < consumed):
                    raise ManagedPvfsSinkError("PVFS barrier does not match consumed frames")
                if (type(command["checkpoint_id"]) is not int or command["checkpoint_id"] <= 0
                        or type(command["deadline"]) not in (int, float)
                        or not math.isfinite(command["deadline"])):
                    raise ManagedPvfsSinkError("PVFS barrier checkpoint or deadline is invalid")
                if time.monotonic() >= command["deadline"]:
                    raise ManagedPvfsSinkError("PVFS command deadline expired")
                if kind == "close":
                    report("writer_stop_observed")
                fill_missing_through(command["last_sequence"], command["deadline"], command)
                publish(ManagedPvfsSink._flush_legacy_writer(legacy_writer, state))
                if kind == "flush":
                    emit("flushed", checkpoint_id=command["checkpoint_id"])
                    continue
                report("writer_queue_drained")
                emit_queue_diagnostic("pvfs_writer_drain_summary", **diagnostic_identity,
                                      last_received_sequence=consumed,
                                      samples_written=state["confirmed"],
                                      missing_frame_count=missing_frames,
                                      skipped_frame_count=skipped_frames)
                report("writer_native_flushed")
                result = ManagedPvfsSink._close_legacy_writer(legacy_writer, state)
                publish(result["ack"])
                if result["failure"] or not result["native_closed"]:
                    raise ManagedPvfsSinkError(result["failure"] or "PVFS close unconfirmed")
                report("writer_native_closed")
                emit("closed", native_closed=True, final_durable_ordinal=durable)
                return
            else:
                raise ManagedPvfsSinkError("PVFS unknown command")
    except BaseException as exc:
        legacy_writer = legacy_writer or getattr(exc, "legacy_writer", None)
        state = state or getattr(exc, "legacy_state", None)
        released = legacy_writer is None
        if legacy_writer is not None:
            state["failure"] = state["failure"] or str(exc)
            released = ManagedPvfsSink._close_legacy_writer(legacy_writer, state)["native_closed"]
        emit("failed", error=f"{type(exc).__name__}: {exc}", native_closed=released)


class ManagedPvfsSink:
    """PVFS sink backed by exclusively-allocated, immutable, linked segments.

    Invariants:
    - Construction opens nothing and imports no native library (SINK-21).
    - The configured path is claimed create-once via ``managed_file.create``
      before any pvfs_tools container is created; a foreign file is
      refused, and the container is only ever created over a zero-byte placeholder.
    - Component containers are immutable after close; error recovery always
      allocates a linked continuation and never reopens/mutates a prior segment.
    - A clean stop marks the acquisition complete and allocates no continuation.
    - With ``use_writer_process`` the adapter is the sole owner of the writer
      child: it is started, fed, stopped, joined, and force-terminated by this
      object — never orphaned, never duplicated.
    - The ``pod`` kwarg injected by Morelia reconstruction is accepted; channels,
      units, and the sample rate are derived from it when not given explicitly.
    """

    supports_missing_samples = True

    def __init__(
        self,
        *,
        path: str | Path,
        dataflow_id: str,
        channels: list[str] | None = None,
        units: list[str] | None = None,
        sample_rate: float | None = None,
        output_id: str | None = None,
        logical_sink_id: str | None = None,
        segment_index: int = 0,
        previous_output_id: str | None = None,
        session_id: int | None = None,
        device_id: str | None = None,
        sink_id: str | None = None,
        schema_hash: str | None = None,
        pod: object = None,
        observe_on_scheduler: str | None = None,
        use_writer_process: bool = False,
        device_preferences: list[dict] | None = None,
    ) -> None:
        # Descriptor only — no filesystem, database, container, or native-import
        # work happens here so the parent watchdog can build/rebuild it safely.
        self._base_path = Path(path)
        self._dataflow_id = dataflow_id
        self._channels = list(channels) if channels is not None else None
        self._units = list(units) if units is not None else None
        self._sample_rate = sample_rate
        self._output_id = output_id
        self._logical_sink_id = logical_sink_id
        self._segment_index = segment_index
        self._previous_output_id = previous_output_id
        self._session_id = session_id
        self._device_id = device_id
        self._sink_id = sink_id
        self._schema_hash = schema_hash
        self._pod = pod
        self._use_writer_process = use_writer_process
        self._device_preferences = device_preferences
        # Morelia forces the scheduler off in writer-process mode (the child owns
        # all I/O), so mirror that so a reconstruction cannot double-schedule.
        self.observe_on_scheduler = None if use_writer_process else observe_on_scheduler

        self._record: OutputFile | None = None
        self._legacy_writer = None
        self._legacy_state = None
        self._startup_error = None
        self._samples_written = 0
        self._forced_termination = False
        self._pvfs_close_failed = False
        self._on_sink_error: Callable[[dict], None] | None = None
        self._database_context = None
        self._opened = False
        self._closed = False

        # Writer-process handles (worker-side only).
        self._writer_queue: mp.Queue | None = None
        self._writer_proc: mp.Process | None = None
        self._writer_stop = None
        self._writer_evidence_queue = None
        self._writer_sample_count = None
        self._shutdown_reporter = None
        self._writer_recording_queue = None
        self._recording_thread = None
        self._recording_error = None
        self._terminal_error = None
        self._callback_context = local()
        self._receipt_condition = Condition()
        self._accepted_sequence = 0
        self._receipt_ordinal = 0
        self._delivered_ordinal = 0
        self._checkpoint_id = 0
        self._checkpoint_receipts = {}
        self._ready = False
        self._terminal_receipt = None
        self._shutdown_deadline = None
        self._owner_thread = None
        self._error_reported = False
        self._predecessor_released = False
        self._predecessor_worker_exited = False
        self._last_durable_ordinal = 0
        self._consumed_sequence = 0
        self._segment_identity = None
        self._loss_counts = (0, 0)
        self._persisted_count = -1
        self._reservation = None
        self._resource_diagnostics = None

    # -- lifecycle ----------------------------------------------------------

    @property
    def opened(self) -> bool:
        return self._opened

    @property
    def record(self) -> OutputFile | None:
        return self._record

    @property
    def use_writer_process(self) -> bool:
        return self._use_writer_process

    @property
    def writer_alive(self) -> bool:
        """True while the owned writer child is still running (test/introspection)."""
        return self._writer_proc is not None and self._writer_proc.is_alive()

    @property
    def forced_termination(self) -> bool:
        """True if the writer child had to be force-terminated on finalize."""
        return self._forced_termination

    def _ownership_released(self) -> bool:
        """Report release separately from whether recording finalization succeeded."""
        if self._reservation is not None and not self._reservation._handle.closed:
            return False
        if self._use_writer_process:
            return self._writer_proc is None or not self._writer_proc.is_alive()
        return self._legacy_state is None or self._legacy_state["closed"]

    def open(self) -> ManagedPvfsSink:
        """Create a fresh segment; a failed startup permits cleanup only."""
        self._check_available()
        if self._opened:
            return self
        self._owner_thread = get_ident()
        try:
            self._database_context = _ensure_database_context()
            self._resolve_channels_units_rate()
            if self._output_id is None:
                managed = managed_file.create(
                    self._base_path, dataflow_id=self._dataflow_id, sink_type=SinkType.PVFS,
                    session_id=self._session_id, device_id=self._device_id, sink_id=self._sink_id,
                    schema_hash=self._schema_hash, logical_sink_id=self._logical_sink_id,
                    segment_index=self._segment_index, previous_output_id=self._previous_output_id,
                    require_new_file=True,
                )
            else:
                from app.repositories.output_files import latest_component

                try:
                    previous = latest_component(self._output_id, dataflow_id=self._dataflow_id,
                                                sink_id=self._sink_id)
                except ValueError as exc:
                    raise ManagedPvfsSinkError(str(exc)) from exc
                if previous.device_id != self._device_id:
                    raise ManagedPvfsSinkError("PVFS predecessor belongs to another device")
                # A confirmed worker exit permits a fresh segment, not a clean-close claim.
                if (previous.status != "closed" and not self._predecessor_released
                        and not self._predecessor_worker_exited):
                    raise ManagedPvfsSinkError("PVFS predecessor ownership release is unconfirmed")
                managed = managed_file.allocate_continuation(
                    previous, termination_reason="recovery", schema_hash=self._schema_hash,
                    require_new_file=True,
                    previous_writer_released=(previous.status == "closed" or self._predecessor_released),
                )
            self._start_segment(managed)
            self._opened = True
            return self
        except BaseException as exc:
            self._startup_error = exc
            self._fail(f"PVFS startup failed: {exc}")
            if self._record is not None:
                self._mark_record_failed("writer_failure")
            raise


    def recover(self, *, termination_reason: str = "recovery") -> ManagedPvfsSink:
        """Finalize the current segment before allocating its fresh continuation."""
        self._check_available()
        if not self._opened:
            raise ManagedPvfsSinkError("cannot recover a sink that was never opened")
        try:
            self._finalize_segment(termination_reason=termination_reason,
                                   acquisition_state="interrupted")
            managed = managed_file.allocate_continuation(
                self._record, termination_reason=termination_reason,
                schema_hash=self._schema_hash, require_new_file=True,
            )
            self._start_segment(managed)
        except BaseException as exc:
            self._startup_error = exc
            self._fail(str(exc))
            raise
        return self


    def close(self, *, termination_reason: str = "clean") -> None:
        """Close within one shutdown budget; retries preserve earlier failures.

        Native calls in local mode require an outer process supervisor for a
        hard deadline. A standalone caller cannot interrupt a native hang.
        """
        self._guard_callback()
        if self._closed:
            if self._terminal_error is not None:
                raise ManagedPvfsSinkError(self._terminal_error)
            return
        self._operation_deadline(_WRITER_JOIN_TIMEOUT, shutdown=True)
        try:
            if self._reservation is not None:
                self._reservation._handle.close()
                self._reservation = None
            if self._record is not None:
                interrupted = termination_reason == "source_disconnect"
                self._finalize_segment(
                    termination_reason=termination_reason,
                    acquisition_state="interrupted" if interrupted else "complete")
            if self._terminal_error is not None:
                raise ManagedPvfsSinkError(self._terminal_error)
        finally:
            released = (not self.writer_alive and
                        (self._legacy_state is None or self._legacy_state["closed"]))
            self._closed = released
            self._opened = False
            if self._resource_diagnostics is not None:
                self._resource_diagnostics.stop()
            if released:
                self._release_database_context()


    def __enter__(self) -> ManagedPvfsSink:
        return self.open()

    def __exit__(self, *args: object) -> None:
        self.close()

    def bind_error_callback(self, callback: Callable[[dict], None] | None) -> None:
        """Bind the worker-local sink reporter after Morelia reconstruction."""
        self._on_sink_error = callback

    def bind_shutdown_reporter(self, reporter) -> None:
        """Bind the worker-local shutdown reporter after sink reconstruction."""
        self._shutdown_reporter = reporter

    # -- writes -------------------------------------------------------------

    def bind_recording_callback(self, callback) -> None:
        bind_recording(self, callback)

    def write_frame(self, samples, *, timestamp=None, recorded=True) -> None:
        """Append one sample per channel (ordered to match ``channels``)."""
        self._check_available()
        if self._channels is None:
            self._resolve_channels_units_rate()
        if isinstance(samples, (str, bytes)):
            raise ValueError("PVFS frame must contain one numeric scalar per channel")
        try:
            samples = tuple(samples)
        except TypeError as exc:
            raise ValueError("PVFS frame must contain one numeric scalar per channel") from exc
        if len(samples) != len(self._channels):
            raise ValueError(
                f"expected {len(self._channels)} channel samples, got {len(samples)}"
            )
        if self._uses_legacy_8274_layout():
            values = [_coerce(v) for v in samples]
        else:
            values = [_normalize_sample(v, preserve_nan=not recorded) for v in samples]
        if not self._opened:
            self.open()
        if self._recording_error is not None:
            raise ManagedPvfsSinkError(
                "PVFS recording acknowledgement failed"
            ) from self._recording_error

        if self._use_writer_process:
            try:
                with self._receipt_condition:
                    self._check_available()
                    if not self.writer_alive:
                        raise ManagedPvfsSinkError("PVFS writer is not alive")
                    sequence = self._accepted_sequence + 1
                    try:
                        self._writer_queue.put_nowait({
                            "kind": "frame", "segment": self._record.output_id,
                            "sequence": sequence, "values": tuple(values), "recorded": bool(recorded),
                        })
                    except Full as exc:
                        raise ManagedPvfsSinkError("PVFS transport frame buffer is full") from exc
                    self._accepted_sequence = sequence
                self._persist_progress()
            except BaseException as exc:
                self._fail(str(exc))
                raise
            return

        try:
            ack = self._append_legacy_writer_frame(
                self._legacy_writer, self._legacy_state,
                {"values": values, "recorded": recorded},
            )
            self._accepted_sequence += 1
            self._deliver_ack(ack)
        except BaseException as exc:
            self._fail(str(exc))
            raise

    def write_row(self, row: dict) -> None:
        """Append one frame from a ``{channel_label: value}`` mapping."""
        self._check_available()
        if self._channels is None:
            self._resolve_channels_units_rate()
        if self._uses_legacy_8274_layout():
            values = [row.get(channel, 0.0) for channel in self._channels]
        else:
            missing = [channel for channel in self._channels if channel not in row]
            if missing:
                raise ValueError("PVFS row is missing required channels: " + ", ".join(missing))
            values = [row[channel] for channel in self._channels]
        self.write_frame(values, timestamp=row.get("timestamp", row.get("time")))

    def flush(self, *args: object) -> None:
        """Persist buffered samples, or accept a timestamp and packet.

        Process checkpoints allow the resource retry window before the normal
        flush budget. A timeout permits cleanup only. Local native calls require
        external process supervision.
        """
        self._check_available()
        if not args:
            budget = _CHECKPOINT_TIMEOUT + (_RESOURCE_RETRY_SECONDS if self._use_writer_process else 0)
            deadline = self._operation_deadline(budget)
            if not self._opened:
                return
            try:
                if self._use_writer_process:
                    checkpoint = self._send_barrier("flush", deadline)
                    self._wait_receipt(
                        lambda: checkpoint in self._checkpoint_receipts and
                        self._delivered_ordinal >= self._checkpoint_receipts[checkpoint], deadline,
                    )
                    self._persist_progress()
                else:
                    self._write_buffer()
                    if time.monotonic() >= deadline:
                        raise ManagedPvfsSinkError("PVFS checkpoint deadline expired")
            except BaseException as exc:
                self._fail(str(exc))
                raise
            return
        if len(args) != 2:
            raise TypeError("flush() expects no args or (timestamp, packet)")
        timestamp, packet = args
        self.write_frame(self._frame_from_packet(packet), timestamp=timestamp,
                         recorded=not getattr(packet, "is_missing_sample", False))

    # -- introspection / reconstruction ------------------------------------

    def get_dict(self) -> dict:
        """Kwargs to reconstruct this sink. Carries the current segment identity.

        A reconstructing worker that opens this descriptor allocates a linked
        continuation (never a destructive reopen). ``path`` is the component-0
        base path; the continuation name is derived from it by the allocator.
        """
        if self._record is not None:
            output_id = self._record.output_id
            logical_sink_id = self._record.logical_sink_id
            segment_index = self._record.segment_index
        else:
            output_id = self._output_id
            logical_sink_id = self._logical_sink_id
            segment_index = self._segment_index
        return {
            "path": str(self._base_path),
            "dataflow_id": self._dataflow_id,
            "channels": list(self._channels) if self._channels is not None else None,
            "units": list(self._units) if self._units is not None else None,
            "sample_rate": self._sample_rate,
            "output_id": output_id,
            "logical_sink_id": logical_sink_id,
            "segment_index": segment_index,
            "session_id": self._session_id,
            "device_id": self._device_id,
            "sink_id": self._sink_id,
            "schema_hash": self._schema_hash,
            "observe_on_scheduler": self.observe_on_scheduler,
            "use_writer_process": self._use_writer_process,
            "device_preferences": self._device_preferences,
        }

    def component_metadata(self) -> dict:
        """Channel/device metadata the packet-18 PVFS merger needs per component.

        Returned per-segment so the merger can verify channel identity/order,
        units, sample rate, and pinned device preferences match across linked
        components before a format-aware read-and-rewrite merge.
        """
        return {
            "logical_sink_id": self._logical_sink_id
            if self._record is None
            else self._record.logical_sink_id,
            "segment_index": self._segment_index
            if self._record is None
            else self._record.segment_index,
            "path": None if self._record is None else self._record.path,
            "channels": list(self._channels) if self._channels is not None else None,
            "units": list(self._units) if self._units is not None else None,
            "sample_rate": self._sample_rate,
            "device_preferences": self._device_preferences,
        }

    # -- segment machinery --------------------------------------------------

    def _start_segment(self, managed: ManagedOutputFile) -> None:
        """Adopt an allocated component: own its path with a fresh PVFS container.

        ``managed`` arrives holding the exclusive ``open(path, "xb")`` claim that
        reserved the path (and refused a foreign file). PVFS writing is owned by
        ``pvfs_tools`` (in-process) or the writer child, both of which create the
        container themselves, so the raw placeholder handle is dropped WITHOUT
        marking the row closed, then the container is created on the now-ours,
        empty path (create-over-empty is non-destructive; create-over-data is not).
        """
        self._record = managed.record
        self._segment_identity = self._record.output_id
        self._loss_counts = (self._record.sample_loss, self._record.byte_loss)
        self._reservation = managed
        managed.release_file_placeholder()
        self._reservation = None
        self._samples_written = 0
        self._persisted_count = -1
        self._forced_termination = False
        self._accepted_sequence = self._receipt_ordinal = self._delivered_ordinal = 0
        self._consumed_sequence = self._last_durable_ordinal = 0
        self._checkpoint_id = 0
        self._checkpoint_receipts = {}
        self._ready = False
        self._terminal_receipt = None
        self._shutdown_deadline = None

        if self._resource_diagnostics is None:
            from app.output.resource_diagnostics import RecordingDiagnostics

            self._resource_diagnostics = RecordingDiagnostics.start(self)
        if self._use_writer_process:
            self._start_writer_process(self._record.path)
        else:
            self._start_in_process(self._record.path)

    def _start_in_process(self, path: str) -> None:
        try:
            self._legacy_writer, self._legacy_state = self._create_legacy_writer({
                "path": path, "channels": self._channels, "units": self._units,
                "rate": self._sample_rate, "preferences": self._device_preferences,
            })
        except Exception as exc:
            self._legacy_writer = getattr(exc, "legacy_writer", None)
            self._legacy_state = getattr(exc, "legacy_state", None)
            self._startup_error = exc
            self._mark_record_failed("writer_failure")
            raise

    @staticmethod
    def _create_legacy_writer(config: dict):
        """Open the legacy writer with a resolved layout, including podless recordings."""
        from Morelia.Stream.sink.pvfs_sink import PvfsSink

        legacy_writer = PvfsSink.__new__(PvfsSink)
        legacy_writer._file_path = config["path"]
        legacy_writer._pod = SimpleNamespace(sample_rate=config["rate"])
        legacy_writer._channels = tuple(config["channels"])
        legacy_writer._units = tuple(config["units"])
        legacy_writer._buffer = [[] for _ in legacy_writer._channels]
        legacy_writer._pvfs_data = None
        legacy_writer._start_time = None
        legacy_writer._samples_written = 0
        legacy_writer._use_writer_process = False
        legacy_writer.observe_on_scheduler = None
        legacy_writer._device_preferences = config.get("preferences")
        legacy_writer._writer_queue = None
        legacy_writer._writer_proc = None
        legacy_writer._writer_stop = None
        state = {"confirmed": 0, "failure": None, "closed": False,
                 "first_recorded": None, "last_recorded": None}
        try:
            legacy_writer.__enter__()
            native = legacy_writer._pvfs_data
            save_catalog = native._save_database

            def checked_save(*args, **kwargs):
                result = save_catalog(*args, **kwargs)
                if result is not True:
                    raise ManagedPvfsSinkError("PVFS catalog save returned false")
                return result

            native._save_database = checked_save
            for name in legacy_writer._channels:
                channel = legacy_writer._pvfs_data._indexed_data_files.get(name)
                if channel is None:
                    raise ManagedPvfsSinkError(f"PVFS channel missing during setup: {name}")
                original = channel.append_block

                def checked(timestamp, values, *, original=original, name=name):
                    result = original(timestamp, values)
                    if result != 0:
                        raise ManagedPvfsSinkError(f"PVFS block write failed: {name}: {result}")
                    return result

                channel.append_block = checked
            native.flush(synchronous=True)
            return legacy_writer, state
        except BaseException as exc:
            # __enter__ failures still leave native resources owned by this writer.
            state["failure"] = f"PVFS startup failed: {type(exc).__name__}: {exc}"
            ManagedPvfsSink._close_legacy_writer(legacy_writer, state)
            error = ManagedPvfsSinkError(state["failure"])
            error.legacy_writer = legacy_writer
            error.legacy_state = state
            raise error from exc

    @staticmethod
    def _append_legacy_writer_frame(legacy_writer, state: dict, command: dict) -> dict | None:
        """Buffer a positional frame and its real-sample bounds until a block is ready."""
        if state["failure"] is not None or state["closed"]:
            raise ManagedPvfsSinkError("PVFS legacy writer is unavailable")
        values = command["values"]
        if len(values) != len(legacy_writer._channels):
            raise ValueError("PVFS frame channel count mismatch")
        position = legacy_writer._samples_written + len(legacy_writer._buffer[0])
        for buffer, value in zip(legacy_writer._buffer, values, strict=True):
            buffer.append(value)
        if command.get("recorded", True):
            if state["first_recorded"] is None:
                state["first_recorded"] = position
            state["last_recorded"] = position
        if len(legacy_writer._buffer[0]) >= max(1, int(legacy_writer._pod.sample_rate)):
            return ManagedPvfsSink._flush_legacy_writer(legacy_writer, state)
        return None

    @staticmethod
    def _flush_legacy_writer(legacy_writer, state: dict) -> dict | None:
        """Check and persist a legacy block before confirming its count and recording bounds."""
        if state["failure"] is not None or state["closed"]:
            raise ManagedPvfsSinkError("PVFS legacy writer is unavailable")
        try:
            lengths = [len(buffer) for buffer in legacy_writer._buffer]
            if len(lengths) != len(legacy_writer._channels) or len(set(lengths)) != 1:
                raise ManagedPvfsSinkError("PVFS channel buffer lengths differ")
            for name in legacy_writer._channels:
                if legacy_writer._pvfs_data._indexed_data_files.get(name) is None:
                    raise ManagedPvfsSinkError(f"PVFS channel missing during write: {name}")
            count = lengths[0]
            legacy_writer._write_buffer_to_pvfs()
            for channel in legacy_writer._pvfs_data._indexed_data_files.values():
                channel.flush(synchronous=True)
            legacy_writer._pvfs_data.flush(synchronous=True)
            state["confirmed"] += count
            if not count:
                return None
            origin = legacy_writer._start_time.to_seconds()
            rate = legacy_writer._pod.sample_rate

            def recorded_time(position):
                if position is None:
                    return None
                return datetime.fromtimestamp(origin + position / rate, UTC).isoformat()

            ack = {"confirmed": state["confirmed"],
                   "first_sample_at": recorded_time(state["first_recorded"]),
                   "last_sample_at": recorded_time(state["last_recorded"])}
            state["first_recorded"] = state["last_recorded"] = None
            return ack
        except BaseException as exc:
            # An append may already have reached another channel. Never replay it.
            state["failure"] = f"PVFS block flush failed: {type(exc).__name__}: {exc}"
            raise

    @staticmethod
    def _close_legacy_writer(legacy_writer, state: dict) -> dict:
        """Release native ownership with strict close checking, retaining any earlier failure."""
        ack = None
        if not state["closed"]:
            if state["failure"] is None:
                with suppress(Exception):
                    ack = ManagedPvfsSink._flush_legacy_writer(legacy_writer, state)
            try:
                native = legacy_writer._pvfs_data
                if native is not None and state["failure"] is not None:
                    # Failed persistence is terminal; cleanup must not retry the flush.
                    native.flush = lambda synchronous=True: None
                if native is not None:
                    for channel in native._indexed_data_files.values():
                        if channel is not None:
                            channel.close()
                container = None if native is None else native._pvfs
                if native is None or (native.close() is True and
                                      (container is None or container._wrapper is None)):
                    state["closed"] = True
                else:
                    state["failure"] = state["failure"] or "PVFS native close returned false"
            except Exception as exc:
                state["failure"] = state["failure"] or (
                    f"PVFS native close failed: {type(exc).__name__}: {exc}"
                )
        return {"confirmed": state["confirmed"], "native_closed": state["closed"],
                "failure": state["failure"], "ack": ack}

    def _deliver_ack(self, ack: dict | None) -> None:
        """Keep native confirmation separate from notification delivery."""
        if ack is None:
            return
        self._samples_written = ack["confirmed"]
        if get_ident() == self._owner_thread:
            self._persist_progress()
        callback = getattr(self, "_recording_callback", None)
        if callback is not None:
            try:
                self._callback_context.active = True
                callback({"source_id": self._device_id, "sink_id": self._sink_id,
                          "output_id": self._segment_identity,
                          "event_type": ("pvfs_progress" if ack["first_sample_at"] is None
                                         else "recording_bounds"),
                          "confirmed_samples": self._samples_written,
                          "first_sample_at": ack["first_sample_at"],
                          "last_sample_at": ack["last_sample_at"]})
            except BaseException as exc:
                self._recording_error = exc
                self._fail(f"PVFS recording acknowledgement failed: {exc}")
                raise
            finally:
                self._callback_context.active = False
            if self._recording_error is not None:
                raise ManagedPvfsSinkError("PVFS recording callback delivery failed")

    def _persist_progress(self):
        if self._record is not None and self._samples_written > self._persisted_count:
            from app.repositories.output_files import record_pvfs_progress

            count = self._samples_written
            record_pvfs_progress(self._segment_identity, count)
            self._persisted_count = count

    def _guard_callback(self):
        if getattr(self._callback_context, "active", False):
            self._recording_error = ManagedPvfsSinkError(
                "PVFS lifecycle call from recording callback")
            self._fail(str(self._recording_error))
            raise self._recording_error

    def _check_available(self):
        self._guard_callback()
        if self._writer_queue is not None and self._writer_queue.transport_error is not None:
            self._fail("PVFS transport failed: " + self._writer_queue.transport_error)
        if self._terminal_error is not None or self._closed or self._startup_error is not None:
            raise ManagedPvfsSinkError(
                "PVFS sink is unavailable: " + (self._terminal_error or "closed"))

    def _fail(self, reason):
        with self._receipt_condition:
            self._terminal_error = self._terminal_error or str(reason)
            self._receipt_condition.notify_all()
        if self._resource_diagnostics is not None:
            self._resource_diagnostics.notify()
        if not self._error_reported and self._on_sink_error is not None:
            self._error_reported = True
            self._report_close_failure(self._terminal_error)

    def _operation_deadline(self, budget, *, shutdown=False):
        deadline = time.monotonic() + budget
        shared = getattr(self._shutdown_reporter, "shutdown_deadline", None)
        if shared is not None and shared > 0:
            deadline = min(deadline, shared)
        if shutdown:
            if self._shutdown_deadline is None:
                self._shutdown_deadline = deadline
            deadline = min(deadline, self._shutdown_deadline)
        return deadline

    def _wait_receipt(self, predicate, deadline):
        with self._receipt_condition:
            while not predicate():
                if self._terminal_error is not None:
                    raise ManagedPvfsSinkError(self._terminal_error)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._fail("PVFS receipt or recording delivery deadline expired")
                    raise ManagedPvfsSinkError(self._terminal_error)
                self._receipt_condition.wait(min(remaining, 0.1))
            if time.monotonic() >= deadline:
                self._fail("PVFS operation deadline expired")
                raise ManagedPvfsSinkError(self._terminal_error)
            if self._terminal_error is not None:
                raise ManagedPvfsSinkError(self._terminal_error)

    def _send_barrier(self, kind, deadline):
        with self._receipt_condition:
            self._checkpoint_id += 1
            checkpoint = self._checkpoint_id
            self._writer_queue.put({"kind": kind, "segment": self._record.output_id,
                                    "last_sequence": self._accepted_sequence,
                                    "frame_count": self._accepted_sequence,
                                    "checkpoint_id": checkpoint, "deadline": deadline},
                                   timeout=max(0, deadline - time.monotonic()))
            return checkpoint


    def _start_writer_process(self, path: str) -> None:
        context = mp.get_context("spawn")
        identity = {"device_id": self._device_id, "sink_id": self._sink_id,
                    "dataflow_id": self._dataflow_id, "output_id": self._record.output_id}
        self._writer_queue = DiagnosticQueue(
            "pvfs_samples", ctx=context,
            maxsize=max(1, math.ceil(self._sample_rate * _WRITER_BUFFER_SECONDS)),
            resource_retry_seconds=_RESOURCE_RETRY_SECONDS, **identity,
        )
        self._writer_recording_queue = DiagnosticQueue("pvfs_recording_ack", ctx=context, **identity)
        self._writer_proc = context.Process(target=_pvfs_writer_target, args=(
            self._writer_queue, self._writer_recording_queue,
            {"path": path, "channels": tuple(self._channels), "units": tuple(self._units),
             "rate": float(self._sample_rate), "preferences": self._device_preferences},
            self._record.output_id, self._shutdown_reporter,
        ))
        deadline = self._operation_deadline(_WRITER_JOIN_TIMEOUT)
        self._writer_proc.start()
        self._recording_thread = Thread(target=self._relay_recording, daemon=True)
        self._recording_thread.start()
        try:
            self._wait_receipt(lambda: self._ready, deadline)
        except BaseException:
            self._stop_writer_process(deadline=deadline)
            raise

    def _relay_recording(self):
        """Validate child receipts and deliver notifications even while acquisition is idle."""
        queue = self._writer_recording_queue
        while True:
            try:
                receipt = queue.get(timeout=0.1)
            except Empty:
                if self._writer_queue is not None and self._writer_queue.transport_error is not None:
                    self._fail("PVFS transport failed: " + self._writer_queue.transport_error)
                    return
                if self._writer_proc is not None and not self._writer_proc.is_alive():
                    self._fail("PVFS writer exited without terminal receipt")
                    return
                continue
            except (EOFError, OSError, ValueError):
                return
            try:
                self._validate_receipt(receipt)
                if receipt["kind"] == "written":
                    self._deliver_ack(receipt)
                with self._receipt_condition:
                    self._delivered_ordinal = receipt["ordinal"]
                    self._receipt_condition.notify_all()
                if receipt["kind"] in {"closed", "failed"}:
                    return
            except BaseException as exc:
                self._fail(f"PVFS receipt delivery failed: {exc}")
                return

    def _validate_receipt(self, receipt):
        base = {"kind", "segment", "ordinal", "consumed", "confirmed", "last_sequence"}
        extras = {"ready": set(), "written": {"first_sample_at", "last_sample_at"},
                  "flushed": {"checkpoint_id"},
                  "closed": {"native_closed", "final_durable_ordinal"},
                  "failed": {"error", "native_closed"}}
        with self._receipt_condition:
            kind = receipt.get("kind") if isinstance(receipt, dict) else None
            if kind not in extras or set(receipt) != base | extras[kind]:
                raise ManagedPvfsSinkError("PVFS receipt schema mismatch")
            if receipt["segment"] != self._segment_identity:
                raise ManagedPvfsSinkError("PVFS receipt segment mismatch")
            if any(type(receipt[key]) is not int for key in
                   ("ordinal", "consumed", "confirmed", "last_sequence")):
                raise ManagedPvfsSinkError("PVFS receipt counters are invalid")
            if receipt["ordinal"] != self._receipt_ordinal + 1:
                raise ManagedPvfsSinkError("PVFS receipt ordinal gap")
            count = receipt["confirmed"]
            if kind != "written" and count != self._samples_written:
                raise ManagedPvfsSinkError("PVFS missing persistence receipt")
            if not (self._samples_written <= count <= receipt["consumed"] <= self._accepted_sequence
                    and receipt["consumed"] >= self._consumed_sequence
                    and receipt["last_sequence"] == count):
                raise ManagedPvfsSinkError("PVFS receipt sample count mismatch")
            if kind == "ready":
                if self._ready or receipt["ordinal"] != 1 or count or receipt["consumed"]:
                    raise ManagedPvfsSinkError("PVFS invalid startup receipt")
                self._ready = True
            elif kind == "written":
                if (not self._ready or count <= self._samples_written or
                        count != receipt["consumed"]):
                    raise ManagedPvfsSinkError("PVFS nonadvancing persistence receipt")
                first, last = receipt["first_sample_at"], receipt["last_sample_at"]
                if (first is None) != (last is None):
                    raise ManagedPvfsSinkError("PVFS recording bounds are incomplete")
                if (first is not None and
                        datetime.fromisoformat(first) > datetime.fromisoformat(last)):
                    raise ManagedPvfsSinkError("PVFS recording bounds are reversed")
                self._last_durable_ordinal = receipt["ordinal"]
            elif kind in {"flushed", "closed"}:
                if count != self._samples_written or count != receipt["consumed"]:
                    raise ManagedPvfsSinkError("PVFS barrier missing persistence receipt")
                if kind == "flushed":
                    checkpoint = receipt["checkpoint_id"]
                    if (type(checkpoint) is not int or checkpoint in self._checkpoint_receipts
                            or not 0 < checkpoint <= self._checkpoint_id):
                        raise ManagedPvfsSinkError("PVFS checkpoint identity mismatch")
                    self._checkpoint_receipts[checkpoint] = receipt["ordinal"]
                else:
                    if (receipt["native_closed"] is not True or count != self._accepted_sequence
                            or receipt["final_durable_ordinal"] != self._last_durable_ordinal):
                        raise ManagedPvfsSinkError("PVFS final persistence evidence mismatch")
                    self._terminal_receipt = receipt
            else:
                self._terminal_receipt = receipt
                self._fail("PVFS writer failed: " + str(receipt["error"]))
            self._receipt_ordinal = receipt["ordinal"]
            self._consumed_sequence = receipt["consumed"]
            self._samples_written = count
            self._receipt_condition.notify_all()


    def _finalize_segment(self, *, termination_reason: str, acquisition_state: str) -> None:
        """Release ownership, verify the catalog, and persist the final segment outcome."""
        deadline = self._operation_deadline(_WRITER_JOIN_TIMEOUT, shutdown=True)
        if self._use_writer_process:
            failure = self._stop_writer_process(deadline=deadline)
            released = not self.writer_alive
        elif self._legacy_writer is not None:
            if time.monotonic() >= deadline:
                self._fail("PVFS cleanup deadline expired with native ownership retained")
                raise ManagedPvfsSinkError(self._terminal_error)
            if self._terminal_error is not None:
                self._legacy_state["failure"] = (
                    self._legacy_state["failure"] or self._terminal_error)
            failure = self._finalize_in_process(termination_reason)
            released = self._legacy_state["closed"]
        else:
            failure, released = self._terminal_error, True
        failure = self._terminal_error or failure
        if time.monotonic() >= deadline:
            failure = failure or "PVFS shutdown deadline expired"
        if failure is None and released:
            try:
                self._verify_finalized_container(self._record.path)
                if time.monotonic() >= deadline:
                    raise ManagedPvfsSinkError("PVFS catalog verification deadline expired")
                self._emit_shutdown_action(
                    ShutdownPhase.SINKS_FINALIZING, "pvfs_catalog_verified",
                    outcome=ShutdownOutcome.COMPLETED)
            except BaseException as exc:
                failure = f"PVFS finalized container verification failed: {exc}"
                self._emit_shutdown_failure("pvfs_catalog_verification_failed", exc)
        if not released:
            failure = failure or "PVFS ownership release remains unconfirmed"
        if failure is not None:
            self._fail(failure)
            acquisition_state = "interrupted"
            termination_reason = "forced" if self._forced_termination else "writer_failure"
            self._emit_shutdown_failure("sink_close_failed", ManagedPvfsSinkError(failure))
        self._persist_progress()
        with transaction():
            if released:
                self._record.status = "closed"
            self._record.termination_reason = termination_reason
            self._record.acquisition_state = acquisition_state
            self._record.byte_offset = (os.path.getsize(self._record.path)
                                        if os.path.exists(self._record.path) else 0)
        if failure is not None:
            raise ManagedPvfsSinkError(failure)


    def _finalize_in_process(self, termination_reason: str) -> str | None:
        result = self._close_legacy_writer(self._legacy_writer, self._legacy_state)
        self._samples_written = result["confirmed"]
        self._pvfs_close_failed = not result["native_closed"]
        try:
            self._deliver_ack(result["ack"])
        except Exception as exc:
            return result["failure"] or f"PVFS recording acknowledgement failed: {exc}"
        if self._recording_error is not None:
            return result["failure"] or "PVFS recording acknowledgement failed"
        return result["failure"]

    def _verify_finalized_container(self, path: str) -> None:
        """Verify the embedded catalog without opening the container for writing."""
        from pvfs_tools.Core.pvfs_binding import PvfsFile

        pvfs_file = None
        container_path = Path(path)
        catalog_path = container_path.with_name(
            f".{container_path.name}.verify-{uuid4().hex}.db3"
        )
        try:
            pvfs_file = PvfsFile.open(str(container_path))
            embedded_files = set(pvfs_file.get_file_list())
            if "experiment.db3" not in embedded_files:
                raise ManagedPvfsSinkError("embedded experiment.db3 is missing")

            pvfs_file.extract("experiment.db3", str(catalog_path))
            catalog_uri = f"{catalog_path.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(catalog_uri, uri=True)
            try:
                rows = connection.execute(
                    "SELECT name FROM experiment_channel_information_table"
                ).fetchall()
            finally:
                connection.close()
            embedded_channels = {str(row[0]) for row in rows}
            missing_channels = [
                channel for channel in self._channels if channel not in embedded_channels
            ]
            if missing_channels:
                raise ManagedPvfsSinkError(
                    "finalized catalog missing expected channels: "
                    + ", ".join(missing_channels)
                )
        finally:
            if pvfs_file is not None:
                pvfs_file.close()
            try:
                catalog_path.unlink(missing_ok=True)
            except OSError:
                _log.warning(
                    "pvfs_verification_catalog_cleanup_failed",
                    path=str(catalog_path),
                )

    def _report_close_failure(self, reason: str) -> None:
        event = {
            "source_id": self._device_id or "unknown-source",
            "sink_id": self._sink_id or "unknown-sink",
            "sink_class": type(self).__name__,
            "output_id": self._segment_identity,
            "confirmed_samples": self._samples_written,
            "failure_kind": "sink_close",
            "exception_type": "ManagedPvfsSinkError",
            "message": reason[:500],
            "state": "terminal",
            "last_success_seq": self._samples_written or None,
            "timestamp_ns": time.time_ns(),
            "sample_loss": self._loss_counts[0],
            "byte_loss": self._loss_counts[1],
        }
        _log.error(
            "pvfs_close_failed",
            dataflow_id=self._dataflow_id,
            device_id=self._device_id,
            sink_id=self._sink_id,
            output_id=self._segment_identity,
            samples_written=self._samples_written,
            reason=reason,
        )
        if self._on_sink_error is not None:
            try:
                self._on_sink_error(event)
            except Exception:
                _log.warning(
                    "pvfs_close_failure_callback_failed",
                    dataflow_id=self._dataflow_id,
                    sink_id=self._sink_id,
                )

    def _stop_writer_process(self, *, deadline=None) -> str | None:
        """Drain ordered work or terminate within the remaining shutdown budget."""
        emit_queue_diagnostic("pvfs_queue_stop_requested", device_id=self._device_id,
                              sink_id=self._sink_id, dataflow_id=self._dataflow_id,
                              last_attempted_sequence=self._accepted_sequence)
        if deadline is None:
            deadline = self._operation_deadline(_WRITER_JOIN_TIMEOUT, shutdown=True)
        proc = self._writer_proc
        if proc is None:
            return self._terminal_error
        if proc.pid is None:
            self._writer_proc = None
            return self._terminal_error
        reserve = min(_WRITER_TERMINATE_TIMEOUT, max(0, deadline - time.monotonic()) / 2)
        graceful_deadline = deadline - reserve
        try:
            if proc.is_alive() and self._ready and self._terminal_receipt is None:
                self._send_barrier("close", graceful_deadline)
                self._wait_receipt(
                    lambda: self._terminal_receipt is not None and
                    self._delivered_ordinal >= self._terminal_receipt["ordinal"],
                    graceful_deadline,
                )
        except BaseException as exc:
            self._fail(str(exc))
        proc.join(timeout=max(0, graceful_deadline - time.monotonic()))
        if proc.is_alive():
            self._forced_termination = True
            self._fail("PVFS writer exceeded its shutdown deadline")
            proc.terminate()
            proc.join(timeout=max(0, deadline - time.monotonic()))
        if proc.is_alive():
            self._fail("PVFS writer ownership release remains unconfirmed")
            return self._terminal_error
        if proc.exitcode != 0:
            self._fail(f"PVFS writer exited with code {proc.exitcode}")
        if self._terminal_receipt is None or self._terminal_receipt["kind"] != "closed":
            self._fail("PVFS writer did not supply clean terminal evidence")
        emit_queue_diagnostic("pvfs_queue_stop_summary", device_id=self._device_id,
                              sink_id=self._sink_id, dataflow_id=self._dataflow_id,
                              last_attempted_sequence=self._accepted_sequence,
                              samples_written=self._samples_written,
                              forced_termination=self._forced_termination,
                              writer_pid=proc.pid, writer_exitcode=proc.exitcode)
        if self._recording_thread is not None:
            self._recording_thread.join(timeout=max(0, deadline - time.monotonic()))
            if self._recording_thread.is_alive():
                self._recording_error = RuntimeError("PVFS recording delivery deadline expired")
                self._fail(str(self._recording_error))
        for queue in (self._writer_queue, self._writer_recording_queue):
            if queue is not None:
                queue.cancel_pending_writes()
                queue.cancel_join_thread()
                queue.close()
        self._writer_proc = None
        self._writer_queue = None
        self._writer_recording_queue = None
        return self._terminal_error


    def _emit_shutdown_action(
        self, phase, action, *, outcome=ShutdownOutcome.ACKNOWLEDGED, reason=None,
    ):
        if self._shutdown_reporter is None:
            return
        self._shutdown_reporter.emit(
            phase,
            action,
            outcome=outcome,
            reason=reason,
            sink_id=self._sink_id,
            output_id=None if self._record is None else self._record.output_id,
            actor=ShutdownActor.SINK,
        )

    def _emit_shutdown_failure(self, action, exc):
        self._emit_shutdown_action(
            ShutdownPhase.PHASE_FAILED,
            action,
            outcome=ShutdownOutcome.FAILED,
            reason=str(exc),
        )

    def _write_buffer(self) -> None:
        self._deliver_ack(self._flush_legacy_writer(self._legacy_writer, self._legacy_state))

    def _frame_from_packet(self, packet: object) -> list[float]:
        """Read physical packet positions in the configured channel order."""
        if self._channels is None:
            self._resolve_channels_units_rate()
        if getattr(packet, "is_missing_sample", False):
            return [float("nan")] * len(self._channels)
        if self._uses_legacy_8274_layout():
            return [_coerce(getattr(packet, _LEGACY_8274_PACKET_FIELD_FOR_LABEL.get(label, label),
                                    0.0)) for label in self._channels]
        device_type = type(self._pod).__name__ if self._pod is not None else type(packet).__name__
        expected = {"Pod8206HR": 3, "DataPacket8206HR": 3,
                    "Pod8401HR": 4, "DataPacket8401HR": 4}.get(device_type)
        if expected is not None and len(self._channels) != expected:
            raise ValueError(
                f"PVFS {device_type} requires {expected} physical channels; "
                f"configured {len(self._channels)}"
            )
        frame = []
        for index, label in enumerate(self._channels):
            field = f"ch{index}"
            try:
                value = getattr(packet, field)
            except AttributeError as exc:
                raise ValueError(
                    f"PVFS packet is missing required field {field} for {label}"
                ) from exc
            try:
                frame.append(_normalize_sample(value))
            except ValueError as exc:
                raise ValueError(
                    f"PVFS packet field {field} for {label} must be a numeric scalar"
                ) from exc
        return frame

    def _uses_legacy_8274_layout(self) -> bool:
        return type(self._pod).__name__ == "Pod8274D" or self._channels == [
            "length_in_bytes", "data",
        ]

    def _mark_record_failed(self, termination_reason: str) -> None:
        if self._record is None:
            return
        with transaction():
            if self._ownership_released():
                self._record.status = "closed"
            self._record.termination_reason = termination_reason
            self._record.acquisition_state = "interrupted"

    # -- channel / unit / rate resolution ----------------------------------

    def _resolve_channels_units_rate(self) -> None:
        if self._sample_rate is None:
            self._sample_rate = getattr(self._pod, "sample_rate", None)
        if self._channels is None and self._pod is not None:
            try:
                channels, units = _channels_units_for_pod(self._pod)
            except Exception:
                self._release_database_context()
                raise
            self._channels = list(channels)
            if self._units is None:
                self._units = list(units)
        if self._channels is not None and self._units is None:
            self._units = ["uV"] * len(self._channels)
        if not self._channels or self._sample_rate is None:
            self._release_database_context()
            raise ManagedPvfsSinkError(
                "PVFS sink requires explicit channels+sample_rate or a pod to derive them"
            )
        if len(self._units) != len(self._channels):
            self._release_database_context()
            raise ManagedPvfsSinkError(
                f"units length {len(self._units)} != channels length {len(self._channels)}"
            )
        if any(not isinstance(name, str) or not name.strip() for name in self._channels):
            self._release_database_context()
            raise ManagedPvfsSinkError("PVFS channel names must be nonempty strings")
        try:
            self._sample_rate = float(self._sample_rate)
        except (TypeError, ValueError) as exc:
            self._release_database_context()
            raise ManagedPvfsSinkError("PVFS sample rate must be finite and positive") from exc
        if not math.isfinite(self._sample_rate) or self._sample_rate <= 0:
            self._release_database_context()
            raise ManagedPvfsSinkError("PVFS sample rate must be finite and positive")

    def _release_database_context(self) -> None:
        if self._database_context is not None:
            self._database_context.pop()
            self._database_context = None


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


def _channels_units_for_pod(pod: object) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Derive PVFS channel labels + units from a POD, mirroring Morelia's PvfsSink.

    Duck-typed on the class name so tests need no real Morelia device; the
    Pod8401HR preamp channel map is consulted through Morelia only when a real
    preamp is present, otherwise the default A-D labels are used.
    """
    name = type(pod).__name__
    if name == "Pod8206HR":
        channels = ("EEG1", "EEG2", "EEG3/EMG")
        return channels, ("uV",) * len(channels)
    if name == "Pod8401HR":
        preamp = getattr(pod, "preamp", None)
        positions = ("A", "B", "C", "D")
        if preamp is None:
            return positions, ("uV",) * len(positions)
        from Morelia.Devices import Pod8401HR

        mapping = Pod8401HR.get_channel_map_for_preamp_device(preamp)
        if mapping is None or any(position not in mapping for position in positions):
            raise ManagedPvfsSinkError(f"PVFS cannot resolve 8401HR preamp {preamp!r}")
        channels = tuple(mapping[position] for position in positions)
        return channels, ("uV",) * len(channels)
    if name == "Pod8274D":
        return ("length_in_bytes", "data"), ("", "")
    raise ManagedPvfsSinkError(
        f"cannot derive PVFS channels for pod of type {name!r}; pass channels explicitly"
    )


def _coerce(value: object) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    if result != result or result in (float("inf"), float("-inf")):  # NaN/inf guard
        return 0.0
    return result


def _normalize_sample(value: object, *, preserve_nan: bool = False) -> float:
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        raise ValueError("PVFS sample must be a numeric scalar")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("PVFS sample must be a numeric scalar") from exc
    if math.isnan(result) and preserve_nan:
        return result
    return result if math.isfinite(result) else 0.0


def _ensure_database_context():
    from flask import has_app_context

    if has_app_context():
        return None

    context = create_database_app().app_context()
    context.push()
    return context
