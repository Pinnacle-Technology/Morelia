import json
import logging
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import structlog

from app.diagnostic_events import sanitize_evidence

SAFE_LOG_FIELDS = frozenset(
    {
        # Core event metadata.
        "event",
        "level",
        "logger",
        "timestamp",
        # Correlation identifiers shared by Flask and watchdog processes.
        "request_id",
        "session_id",
        "dataflow_id",
        "command_id",
        "recovery_id",
        "watchdog_id",
        "operation_id",
        "incident_id",
        "gap_id",
        "shutdown_id",
        # HTTP summaries. Never add headers, query strings, or request bodies.
        "http_method",
        "http_route",
        "status_code",
        "duration_ms",
        # Bounded operational metadata.
        "component",
        "action",
        "command",
        "outcome",
        "reason",
        "error_code",
        "error_type",
        "attempt",
        "stream_status",
        "comms_status",
        "recovery_stage",
        "policy_mode",
        "phase",
        # Process/runtime identity for supervision diagnostics.
        "runtime_id",
        "report_id",
        "port",
        "pid",
        "watchdog_pid",
        "watchdog_state",
        "watchdog_status",
        "probed_runtime_id",
        "probed_dataflow_id",
        "probed_watchdog_id",
        "dead_runtime_id",
        "active_ownership",
        # Failure diagnostics. Exception type/message/traceback come from our
        # own raise sites, not payload data; without these a child that dies
        # logs only a bare header line (undiagnosable — see packet 6 hardware
        # checkpoint, 2026-07-15). Raw samples and payloads remain forbidden.
        "error",
        "message",
        "error_message",
        "traceback",
        "exception",
        # Scientific data summaries. Raw samples and payloads are forbidden.
        "sample_count",
        "byte_count",
        "sequence_number",
        # Watchdog status-report summaries. These are normalized, bounded
        # operational values — never raw device samples or command payloads.
        "device_id",
        "stream_index",
        "worker_status",
        "heartbeat_status",
        "heartbeat_age_seconds",
        "source_read_state",
        "source_read_error_type",
        "source_read_error_message",
        "source_read_consecutive_failures",
        "first_packet_timeout_seconds",
        "first_packet_remaining_seconds",
        "failure_count",
        "failure_threshold",
        "recovery_attempt",
        # Cross-layer confirmation and bounded timing fields. These make one
        # operation traceable from the control plane through runtime host and
        # watchdog without admitting payloads, samples, secrets, or headers.
        "layer",
        "actor",
        "actor_pid",
        "emitted_at_ns",
        "elapsed_ms",
        "elapsed_seconds",
        "timeout_seconds",
        "terminal_phase",
        "forced_termination",
        "worker_exitcode",
        "action_count",
        "action_seq",
        "missing_phases",
        "device_count",
        "sink_id",
        "output_id",
        "disconnect_elapsed_seconds",
        "disconnect_recording_continued",
        "max_heartbeat_age_seconds",
        "transport",
        "hardware_id",
        "hardware_present",
        "evidence",
        "suppressed_count",
        "previous_outcome",
    }
)

_LAYER_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")


class SessionDiagnosticJsonlHandler(logging.Handler):
    """Append already-redacted records to a session/layer JSONL file."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        layer: str,
        default_context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self._root = Path(root)
        self._layer = _LAYER_PATTERN.sub("-", layer).strip("-.") or "unknown"
        self._default_context = {
            key: value
            for key, value in dict(default_context or {}).items()
            if key in SAFE_LOG_FIELDS and value is not None
        }

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = json.loads(self.format(record))
            for key, value in self._default_context.items():
                payload.setdefault(key, value)
            session_id = payload.get("session_id")
            if session_id is None or isinstance(session_id, bool):
                return
            try:
                session_id = int(session_id)
            except (TypeError, ValueError):
                return
            if session_id <= 0:
                return
            payload["session_id"] = session_id
            payload["layer"] = self._layer
            payload.setdefault("pid", os.getpid())
            folder = self._root / f"session-{session_id}"
            folder.mkdir(parents=True, exist_ok=True)
            line = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            with (folder / f"{self._layer}.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            self.handleError(record)


def retain_safe_log_fields(
    _logger: Any,
    _method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Drop every field that is not explicitly approved for log output."""
    safe = {key: value for key, value in event_dict.items() if key in SAFE_LOG_FIELDS}
    if "evidence" in safe:
        safe["evidence"] = sanitize_evidence(safe["evidence"])
    return safe


def configure_logging(
    config: type[Any],
    *,
    stream: Any = None,
    diagnostic_layer: str = "control-plane",
    diagnostic_context: Mapping[str, Any] | None = None,
) -> None:
    """Configure structlog and standard logging before Flask is created.

    ``stream`` defaults to stdout for the Flask app. Pass ``sys.stderr``
    explicitly for a process whose stdout is a parsed handshake protocol
    (e.g. the runtime host child prints ``PORT:<n>`` / ``READY`` on stdout —
    interleaving log lines there would break the parent's line-by-line read).
    """

    level = config.LOG_LEVEL.upper()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = (
        structlog.processors.JSONRenderer()
        if config.LOG_FORMAT == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ExtraAdder(allow=SAFE_LOG_FIELDS),
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            retain_safe_log_fields,
            renderer,
        ],
    )

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    diagnostic_root = getattr(config, "DIAGNOSTIC_LOG_DIR", None)
    if diagnostic_root:
        diagnostic_formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[
                structlog.stdlib.ExtraAdder(allow=SAFE_LOG_FIELDS),
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                retain_safe_log_fields,
                structlog.processors.JSONRenderer(),
            ],
        )
        diagnostic_handler = SessionDiagnosticJsonlHandler(
            diagnostic_root,
            layer=diagnostic_layer,
            default_context=diagnostic_context,
        )
        diagnostic_handler.setFormatter(diagnostic_formatter)
        root_logger.addHandler(diagnostic_handler)
    root_logger.setLevel(level)

    # Ensure Werkzeug uses the root handler instead of adding its own.
    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.handlers.clear()
    werkzeug_logger.propagate = True

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
