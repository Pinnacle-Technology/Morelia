"""Record hardware decisions and failures without logging every polling tick."""

import hashlib
import json
import logging
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager

import structlog

_log = structlog.get_logger(__name__)
_lock = threading.Lock()
_recent = OrderedDict()
_REPEAT_SECONDS = 60.0
_MAX_KEYS = 256
_MAX_ROWS = 64
_MAX_TEXT = 256
_CONTEXT = frozenset(
    {
        "session_id",
        "runtime_id",
        "watchdog_id",
        "dataflow_id",
        "device_id",
        "stream_index",
        "transport",
        "port",
        "hardware_id",
    }
)
_EVIDENCE = frozenset(
    {
        "rows",
        "target",
        "stage",
        "index",
        "serial",
        "serial_raw",
        "serial_hex",
        "description",
        "description_raw",
        "description_hex",
        "flags",
        "reported_open",
        "type",
        "id",
        "location",
        "address",
        "hardware_id",
        "physical_id",
        "aliases",
        "transport",
        "backend",
        "identity_conflict",
        "identity_evidence",
        "expected_count",
        "returned_count",
        "failed_index",
        "present",
        "cached",
        "cache_age_ms",
        "ttl_seconds",
        "match_count",
        "unidentified_count",
        "baudrate",
        "read_timeout_ms",
        "write_timeout_ms",
        "flow_control",
        "handle_retained",
        "cleanup_pending",
        "python_executable",
        "driver_module",
        "platform",
        "omitted_count",
        "presence_source",
        "windows_rows",
        "windows_match_count",
        "instance_id",
        "vid",
        "pid",
    }
)


def _safe(value, depth=0):
    if depth > 4:
        return "<depth limit>"
    if isinstance(value, dict):
        return {key: _safe(item, depth + 1) for key, item in value.items() if key in _EVIDENCE}
    if isinstance(value, (list, tuple)):
        omitted = 0
        if (
            value
            and isinstance(value[-1], dict)
            and set(value[-1]) == {"omitted_count"}
            and isinstance(value[-1]["omitted_count"], int)
        ):
            omitted = max(0, value[-1]["omitted_count"])
            value = value[:-1]
        result = [_safe(item, depth + 1) for item in value[:_MAX_ROWS]]
        omitted += max(0, len(value) - _MAX_ROWS)
        if omitted:
            result.append({"omitted_count": omitted})
        return result
    if isinstance(value, str):
        return value if len(value) <= _MAX_TEXT else value[:_MAX_TEXT] + "<truncated>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return f"<{type(value).__name__}>"


def descriptor_evidence(index, info):
    """Keep raw identity bytes and driver fields, excluding native handle objects."""
    row = {"index": index}
    if not isinstance(info, dict):
        return {**row, "type": type(info).__name__}
    for key in ("serial", "description"):
        value = info.get(key)
        row[key] = (
            value.decode("utf-8", errors="backslashreplace") if isinstance(value, bytes) else value
        )
        row[f"{key}_raw"] = (
            repr(value)
            if isinstance(value, (bytes, str)) or value is None
            else f"<{type(value).__name__}>"
        )
        row[f"{key}_hex"] = value.hex() if isinstance(value, bytes) else None
    row.update({key: info.get(key) for key in ("flags", "type", "id")})
    row["location"] = info.get("location", info.get("locid"))
    flags = info.get("flags")
    row["reported_open"] = bool(flags & 1) if isinstance(flags, int) else None
    return _safe(row)


def sanitize_evidence(value):
    """Apply hardware metadata limits at the shared log-output boundary."""
    return _safe(value)


def _fingerprint(evidence):
    # Cache age changes on every tick without changing the observed hardware.
    stable = {
        key: value for key, value in evidence.items() if key not in {"cached", "cache_age_ms"}
    }
    return hashlib.sha256(
        json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _record(action, context, evidence, started, error):
    now = time.monotonic()
    safe = _safe(evidence)
    outcome = "failed" if error is not None else "completed"
    error_type = type(error).__name__ if error is not None else None
    error_message = str(error)[:2048] if error is not None else None
    signature = (
        outcome, error_type, getattr(error, "code", None), error_message, _fingerprint(safe)
    )
    key = (action, *(str(context.get(name, "")) for name in sorted(_CONTEXT)))
    with _lock:
        previous = _recent.pop(key, None)
        if previous and previous[0] == signature and now - previous[1] < _REPEAT_SECONDS:
            _recent[key] = (previous[0], previous[1], previous[2] + 1)
            return
        _recent[key] = (signature, now, 0)
        while len(_recent) > _MAX_KEYS:
            _recent.popitem(last=False)
    emit = _log.warning if error is not None else _log.info
    emit(
        "hardware operation",
        component="hardware_diagnostics",
        action=action,
        outcome=outcome,
        duration_ms=round((now - started) * 1000, 3),
        evidence=safe,
        suppressed_count=previous[2] if previous else 0,
        previous_outcome=previous[0][0] if previous else None,
        error_type=error_type,
        error_code=getattr(error, "code", None),
        error_message=error_message,
        exc_info=(type(error), error, error.__traceback__) if error is not None else None,
        **context,
    )


@contextmanager
def diagnostic_operation(action, **context):
    """Log an operation's evidence and exception while preserving its result."""
    inherited = structlog.contextvars.get_contextvars()
    context = {
        key: _safe(value) for key, value in {**inherited, **context}.items() if key in _CONTEXT
    }
    context["operation_id"] = inherited.get("operation_id") or uuid.uuid4().hex
    started = time.monotonic()
    evidence = {}
    error = None
    with structlog.contextvars.bound_contextvars(**context):
        try:
            yield evidence
        except BaseException as exc:
            error = exc
            raise
        finally:
            try:
                _record(action, context, evidence, started, error)
            except Exception:
                # Diagnostics must not interrupt acquisition or replace its error.
                logging.getLogger(__name__).debug(
                    "Hardware diagnostic emission failed", exc_info=True
                )
