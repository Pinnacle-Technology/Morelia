"""File-authoritative session-template library.

The TOML file is the only definition source. SQLite stores only registry
metadata used to identify, observe, and reconcile that file.
"""

from __future__ import annotations

import builtins
import hashlib
import json
import os
import re
import tempfile
import threading
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import current_app
from sqlalchemy.exc import IntegrityError, OperationalError

from app.database import transaction
from app.domain.errors import (
    DeviceTemplateNotFound,
    InvalidSessionEntry,
    SessionTemplateNameExists,
    SessionTemplateNotFound,
    SessionTemplateReconciliationRetry,
    SessionTemplateRunBlocked,
    SessionTemplateStateConflict,
)
from app.models.session import Session
from app.models.session_template import SessionTemplate
from app.repositories.session_templates import SessionTemplateRepository
from app.repositories.sessions import (
    NO_RUNS,
    SessionRepository,
    SessionRunRef,
    TemplateRunHistory,
)
from app.services import device_templates
from app.services.session_config import _policy_value, _resolve_sinks


CANONICAL_HASH_VERSION = "session-template-v1"
DEPENDENCY_FINGERPRINT_VERSION = "session-template-dependency-v1"
_RECONCILIATION_ATTEMPTS = 3
_reconciliation_lock = threading.RLock()
_repository = SessionTemplateRepository()
_FLOW_FIELDS = {
    "device_template_path",
    "device_template_content_hash",
    "hardware_id",
    "nickname",
    "sinks",
    "sink_type",
    "sink_location",
    "sink_parameters",
}
_CONTENT_FIELDS = {"policy", "device_flows"}
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_HARDWARE_ID_PATTERN = re.compile(r"^[0-9a-zA-Z]{1,10}$")


def _normalize_name(name: str) -> str:
    """Normalize a user-facing name without treating an extension as identity."""

    if not isinstance(name, str):
        raise ValueError("session template name must be a string")
    normalized = name.strip()
    for suffix in (".toml", ".json"):
        if normalized.lower().endswith(suffix):
            normalized = normalized[: -len(suffix)].rstrip()
            break
    if not normalized:
        raise ValueError("session template name is required")
    return normalized


def _filename_stem(name: str) -> str:
    """Convert a validated display name into a safe Windows-compatible stem."""

    invalid = set('<>:"/\\|?*')
    stem = "".join("_" if char in invalid or ord(char) < 32 else char for char in name).strip(" .")
    return stem or "session-template"


def _library_dir() -> Path:
    """Return and initialize the configured flat session-template directory."""

    configured = Path(current_app.config["SESSION_TEMPLATE_DIR"])
    directory = configured if configured.is_absolute() else Path(current_app.instance_path) / configured
    directory.mkdir(parents=True, exist_ok=True)
    return directory.resolve()


def _direct_file(path: Path) -> Path | None:
    """Accept only direct TOML children of the configured template directory."""

    library = _library_dir()
    try:
        resolved = path.resolve()
    except OSError:
        return None
    if resolved.parent != library or not resolved.is_file() or resolved.suffix.lower() != ".toml":
        return None
    return resolved


def _iter_library_files() -> list[Path]:
    """List direct template files in deterministic case-insensitive order."""

    library = _library_dir()
    files: list[Path] = []
    for candidate in library.iterdir():
        resolved = _direct_file(candidate)
        if resolved is not None:
            files.append(resolved)
    return sorted(files, key=lambda path: path.name.lower())


def _template_path(name: str) -> Path:
    return _library_dir() / f"{_filename_stem(_normalize_name(name))}.toml"


def _relative_path(path: Path) -> str:
    resolved = _direct_file(path)
    if resolved is None:
        raise SessionTemplateNotFound(path.name)
    return resolved.name


def _filesystem_identity(path: Path) -> str | None:
    """Return the platform file identity used as the strongest rename signal."""

    try:
        stat = path.stat()
    except OSError:
        return None
    return f"{stat.st_dev}:{stat.st_ino}"


def _canonical_flow(raw_flow: Any, *, resolve_reference: bool) -> dict[str, Any]:
    """Validate and normalize one device flow for hashing or persisted TOML."""

    if not isinstance(raw_flow, Mapping):
        raise InvalidSessionEntry("device_flows", "each flow must be a mapping")
    unknown = set(raw_flow) - _FLOW_FIELDS
    if unknown:
        raise InvalidSessionEntry(
            ",".join(sorted(unknown)),
            f"unknown field(s): {', '.join(sorted(unknown))}",
        )
    path = raw_flow.get("device_template_path")
    if not isinstance(path, str) or not path.strip():
        raise InvalidSessionEntry("device_template_path", "is required")

    template = device_templates.get_by_path(path) if resolve_reference else None
    if resolve_reference and template is None:
        raise DeviceTemplateNotFound(path)

    expected_hash = raw_flow.get("device_template_content_hash")
    if expected_hash is None and template is not None:
        expected_hash = template.content_hash
    if expected_hash is not None:
        if not isinstance(expected_hash, str) or not _HASH_PATTERN.fullmatch(expected_hash):
            raise InvalidSessionEntry("device_template_content_hash", "must be a SHA-256 hex digest")

    sinks = _resolve_sinks(raw_flow) if resolve_reference else _canonical_file_sinks(raw_flow)
    canonical: dict[str, Any] = {
        "device_template_path": template.file_path if template is not None else path.strip(),
        "sinks": sinks,
    }
    if expected_hash is not None:
        canonical["device_template_content_hash"] = expected_hash
    if raw_flow.get("hardware_id") is not None:
        hardware_id = raw_flow["hardware_id"]
        if not isinstance(hardware_id, str) or not _HARDWARE_ID_PATTERN.fullmatch(hardware_id.strip()):
            raise InvalidSessionEntry(
                "hardware_id", "must be 1-8 ASCII letters or digits"
            )
        canonical["hardware_id"] = hardware_id.strip()
    if raw_flow.get("nickname") is not None:
        nickname = raw_flow["nickname"]
        if not isinstance(nickname, str) or not nickname.strip():
            raise InvalidSessionEntry("nickname", "must be a non-empty string")
        canonical["nickname"] = nickname.strip()
    return canonical


def _canonical_file_sinks(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize already-filed sink data without checking live output paths."""

    if "sinks" in entry:
        raw_sinks = entry["sinks"]
        if not isinstance(raw_sinks, builtins.list) or not raw_sinks:
            raise InvalidSessionEntry("sinks", "must contain at least one sink")
    elif "sink_type" in entry:
        raw_sinks = [
            {
                key: entry[key]
                for key in ("sink_type", "sink_location", "sink_parameters")
                if key in entry
            }
        ]
    else:
        raise InvalidSessionEntry("sinks", "is required")

    canonical: list[dict[str, Any]] = []
    for index, raw_sink in enumerate(raw_sinks):
        if not isinstance(raw_sink, Mapping):
            raise InvalidSessionEntry(f"sinks[{index}]", "must be a mapping")
        sink_type = raw_sink.get("sink_type")
        if not isinstance(sink_type, str) or not sink_type.strip():
            raise InvalidSessionEntry(f"sinks[{index}].sink_type", "is required")
        sink_name = raw_sink.get("sink_name") or sink_type.strip().lower()
        if not isinstance(sink_name, str) or not sink_name.strip():
            raise InvalidSessionEntry(f"sinks[{index}].sink_name", "is required")
        params = raw_sink.get("sink_parameters") or {}
        if not isinstance(params, Mapping):
            raise InvalidSessionEntry(f"sinks[{index}].sink_parameters", "must be a mapping")
        item: dict[str, Any] = {
            "sink_name": sink_name.strip(),
            "sink_type": sink_type.strip().lower(),
        }
        if raw_sink.get("sink_location") is not None:
            item["sink_location"] = str(raw_sink["sink_location"])
        item["sink_parameters"] = dict(params)
        canonical.append(item)
    return canonical


def _canonicalize(raw_content: Mapping[str, Any], *, resolve_references: bool = True) -> dict[str, Any]:
    """Build the versioned semantic representation used for execution and hashing."""

    if not isinstance(raw_content, Mapping):
        raise ValueError("session template content must be a mapping")
    unknown = set(raw_content) - _CONTENT_FIELDS
    if unknown:
        raise ValueError(f"unknown session template fields: {', '.join(sorted(unknown))}")
    raw_flows = raw_content.get("device_flows")
    if not isinstance(raw_flows, builtins.list) or not raw_flows:
        raise ValueError("session template device_flows must be a non-empty list")
    return {
        "policy": _policy_value(raw_content.get("policy")).value,
        "device_flows": [
            _canonical_flow(flow, resolve_reference=resolve_references) for flow in raw_flows
        ],
    }


def _content_hash(content: Mapping[str, Any]) -> str:
    """Hash canonical configuration independently of TOML formatting and key order."""

    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    version = CANONICAL_HASH_VERSION.encode()
    return hashlib.sha256(version + b"\0" + encoded).hexdigest()


def _toml_key(key: str) -> str:
    return key if re.fullmatch(r"[A-Za-z0-9_-]+", key) else json.dumps(key)


def _toml_value(value: Any) -> str:
    """Serialize one supported canonical value into deterministic TOML syntax."""

    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return repr(value)
    if isinstance(value, Mapping):
        items = ", ".join(f"{_toml_key(str(key))} = {_toml_value(item)}" for key, item in value.items())
        return "{ " + items + " }"
    if isinstance(value, builtins.list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    raise ValueError(f"unsupported TOML value: {value!r}")


def _to_toml(content: Mapping[str, Any]) -> str:
    """Serialize canonical template content without adding registry metadata."""

    lines = [f"policy = {_toml_value(content['policy'])}"]
    for flow in content["device_flows"]:
        lines.extend(("", "[[device_flows]]"))
        for key, value in flow.items():
            lines.append(f"{_toml_key(key)} = {_toml_value(value)}")
    return "\n".join(lines) + "\n"


def _write_atomic(path: Path, content: str) -> None:
    """Replace one safe flat-folder TOML file through an fsynced temporary file."""

    library = _library_dir()
    safe_path = _direct_file(path) if path.exists() else path.resolve()
    if safe_path is None or safe_path.parent != library or safe_path.suffix.lower() != ".toml":
        raise SessionTemplateNotFound(path.name)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{safe_path.name}.", suffix=".tmp", dir=library)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, safe_path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _load_library_file(path: Path) -> dict[str, Any]:
    """Parse and canonicalize one safe library file without resolving live devices."""

    safe_path = _direct_file(path)
    if safe_path is None:
        raise SessionTemplateNotFound(path.name)
    try:
        parsed = tomllib.loads(safe_path.read_text(encoding="utf-8"))
        return _canonicalize(parsed, resolve_references=False)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        raise ValueError(f"invalid session template file {safe_path.name!r}: {exc}") from exc


@dataclass(frozen=True)
class _FileObservation:
    path: Path
    relative_path: str
    content: dict[str, Any] | None
    observed_hash: str | None
    filesystem_identity: str | None
    warnings: tuple[str, ...]


def _stat_signature(path: Path) -> tuple[int, int, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _snapshot_library() -> list[_FileObservation]:
    """Read one stable, deterministic snapshot without mutating registry state."""

    observations: list[_FileObservation] = []
    for path in _iter_library_files():
        safe_path = _direct_file(path)
        if safe_path is None:
            continue
        before = _stat_signature(safe_path)
        if before is None:
            continue
        warnings: list[str] = []
        try:
            content = _load_library_file(safe_path)
            observed_hash = _content_hash(content)
        except (OSError, ValueError, SessionTemplateNotFound) as exc:
            content = None
            observed_hash = None
            warnings.append(str(exc))
        after = _stat_signature(safe_path)
        if after != before:
            content = None
            observed_hash = None
            warnings.append(f"template file {safe_path.name!r} changed during reconciliation")
        observations.append(
            _FileObservation(
                path=safe_path,
                relative_path=safe_path.name,
                content=content,
                observed_hash=observed_hash,
                filesystem_identity=_filesystem_identity(safe_path),
                warnings=tuple(warnings),
            )
        )
    return sorted(observations, key=lambda item: item.relative_path.lower())


@dataclass
class SessionTemplateFile:
    metadata: SessionTemplate | None
    content: dict[str, Any] | None
    reference_warnings: list[str]
    observation: _FileObservation | None = None
    derived_state: str | None = None
    derived_lineage_parent_id: str | None = None
    derived_duplicate_of_template_id: str | None = None
    derived_allowed_actions: list[str] | None = None
    # None means "this route did not join session history", NOT "no runs" —
    # never started is TemplateRunHistory(0, None). Only catalog_with_run_history
    # fills this in; the registry itself holds no run history.
    run_history: TemplateRunHistory | None = None

    @property
    def template_id(self) -> str | None:
        return self.metadata.template_id if self.metadata is not None else None

    @property
    def id(self) -> str | None:
        return self.template_id

    @property
    def relative_path(self) -> str:
        if self.metadata is not None:
            return self.metadata.relative_path
        if self.observation is None:
            raise RuntimeError("template resource has neither metadata nor observation")
        return self.observation.relative_path

    @property
    def name(self) -> str:
        return Path(self.relative_path).stem

    @property
    def registered_hash(self) -> str | None:
        return self.metadata.registered_hash if self.metadata is not None else None

    @property
    def observed_hash(self) -> str | None:
        if self.observation is not None:
            return self.observation.observed_hash
        return self.metadata.observed_hash if self.metadata is not None else None

    @property
    def content_hash(self) -> str:
        return self.observed_hash or self.registered_hash or ""

    @property
    def filesystem_identity(self) -> str | None:
        if self.observation is not None:
            return self.observation.filesystem_identity
        return self.metadata.filesystem_identity if self.metadata is not None else None

    @property
    def state(self) -> str:
        if self.derived_state is not None:
            return self.derived_state
        if self.metadata is None:
            return "DISCOVERED"
        if self.metadata.lifecycle_state == "REPLACED":
            return "ARCHIVED"
        return self.metadata.state

    @property
    def lifecycle_state(self) -> str | None:
        if self.metadata is None:
            return None
        return "ARCHIVED" if self.metadata.lifecycle_state == "REPLACED" else self.metadata.lifecycle_state

    @property
    def integrity_state(self) -> str | None:
        return self.metadata.integrity_state if self.metadata is not None else None

    @property
    def allowed_actions(self) -> list[str]:
        if self.derived_allowed_actions is not None:
            return self.derived_allowed_actions
        actions = {
            "DISCOVERED": ["register"],
            "ACTIVE": ["archive"],
            "CHANGED": ["accept_change", "archive"],
        }.get(self.state, [])
        if self.state == "ACTIVE":
            dependency_blockers = _dependency_run_blockers(self.content or {})
            if dependency_blockers and all(
                blocker["code"] == "device_template_changed"
                for blocker in dependency_blockers
            ):
                return ["refresh_dependency_revision", *actions]
        return actions

    @property
    def lineage_parent_id(self) -> str | None:
        if self.derived_lineage_parent_id is not None:
            return self.derived_lineage_parent_id
        return self.metadata.lineage_parent_id if self.metadata is not None else None

    @property
    def duplicate_of_template_id(self) -> str | None:
        return self.derived_duplicate_of_template_id

    @property
    def run_count(self) -> int | None:
        return self.run_history.run_count if self.run_history is not None else None

    @property
    def latest_session(self) -> SessionRunRef | None:
        return self.run_history.latest if self.run_history is not None else None

    @property
    def created_at(self):
        return self.metadata.created_at if self.metadata is not None else None

    @property
    def updated_at(self):
        return self.metadata.updated_at if self.metadata is not None else None

    @property
    def reference(self) -> str:
        return f"session-templates/{self.relative_path}"

    @property
    def source(self) -> str:
        return "file"

    @property
    def warnings(self) -> list[str]:
        return self.reference_warnings

    @property
    def run_blockers(self) -> list[dict[str, Any]]:
        return _run_blockers_for(self)

    @property
    def runnable(self) -> bool:
        return not self.run_blockers


_STATE_RECOVERY_ACTIONS = {
    "DISCOVERED": "register_template",
    "PENDING": "retry_reconciliation",
    "CHANGED": "accept_session_template_revision",
    "MISSING": "restore_or_resolve_session_template",
    "INVALID": "repair_session_template",
    "DUPLICATE": "open_registered_original",
    "AMBIGUOUS_RENAME": "resolve_session_template_rename",
    "ARCHIVED": "create_new_session_template_revision",
}


def _state_run_blocker(template: SessionTemplateFile) -> dict[str, Any] | None:
    if template.state == "ACTIVE" and template.content is not None:
        return None
    return {
        "code": "session_template_state",
        "message": f"Session template {template.name!r} is not runnable from state {template.state}.",
        "flow_index": None,
        "device_template_path": None,
        "expected_hash": None,
        "actual_hash": None,
        "recovery_action": _STATE_RECOVERY_ACTIONS.get(template.state, "review_session_template"),
    }


def _dependency_run_blockers(content: Mapping[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for flow_index, flow in enumerate(content.get("device_flows", [])):
        if not isinstance(flow, Mapping):
            continue
        path = flow.get("device_template_path")
        expected_hash = flow.get("device_template_content_hash")
        base = {
            "flow_index": flow_index,
            "device_template_path": path if isinstance(path, str) else None,
            "expected_hash": expected_hash if isinstance(expected_hash, str) else None,
        }
        try:
            current = device_templates.get_by_path(path) if isinstance(path, str) else None
        except (DeviceTemplateNotFound, ValueError):
            blockers.append({
                **base,
                "code": "device_template_invalid",
                "message": f"Flow {flow_index + 1}'s device template is invalid. Repair it before starting a session.",
                "actual_hash": None,
                "recovery_action": "repair_device_template",
            })
            continue

        if current is not None:
            if expected_hash == current.content_hash:
                continue
            blockers.append({
                **base,
                "code": "device_template_changed",
                "message": f"Flow {flow_index + 1}'s device template changed after this session template was saved. Review and accept the current dependency revision before starting.",
                "actual_hash": current.content_hash,
                "recovery_action": "refresh_dependency_revision",
            })
            continue

        matches = (
            device_templates.find_by_content_hash(expected_hash)
            if isinstance(expected_hash, str) and expected_hash
            else []
        )
        if len(matches) == 1:
            blocker = {
                **base,
                "code": "device_template_moved",
                "message": f"Flow {flow_index + 1} references an old device-template path. Update the session template to the new path before starting.",
                "actual_hash": matches[0].content_hash,
                "recovery_action": "refresh_dependency_path",
            }
        elif len(matches) > 1:
            blocker = {
                **base,
                "code": "device_template_ambiguous",
                "message": f"Flow {flow_index + 1} matches multiple device templates. Select one exact dependency before starting.",
                "actual_hash": expected_hash,
                "recovery_action": "resolve_device_template_ambiguity",
            }
        else:
            blocker = {
                **base,
                "code": "device_template_missing",
                "message": f"Flow {flow_index + 1}'s device template cannot be found. Restore it or update the session template before starting.",
                "actual_hash": None,
                "recovery_action": "restore_device_template",
            }
        blockers.append(blocker)
    return blockers


def _run_blockers_for(template: SessionTemplateFile) -> list[dict[str, Any]]:
    state_blocker = _state_run_blocker(template)
    if state_blocker is not None:
        return [state_blocker]
    return _dependency_run_blockers(template.content or {})


def _require_runnable(template: SessionTemplateFile) -> None:
    blockers = template.run_blockers
    if blockers:
        raise SessionTemplateRunBlocked(
            f"Session template {template.name!r} is blocked and cannot start a run.",
            template.state,
            template.allowed_actions,
            blockers,
        )


def _attach_reference_warnings(template: SessionTemplateFile) -> SessionTemplateFile:
    """Add non-fatal warnings for missing or changed device-template dependencies."""

    warnings = builtins.list(template.reference_warnings)
    for index, flow in enumerate((template.content or {}).get("device_flows", []), start=1):
        if not isinstance(flow, Mapping):
            continue
        try:
            _, flow_warnings = resolve_device_template_reference(flow)
            warnings.extend(f"flow {index}: {warning}" for warning in flow_warnings)
        except DeviceTemplateNotFound:
            warnings.append(
                f"flow {index}: device template path {flow.get('device_template_path')!r} cannot be resolved"
            )
        except ValueError as exc:
            warnings.append(f"flow {index}: {exc}")
    template.reference_warnings[:] = warnings
    return template


# DISCOVERED, DUPLICATE, AMBIGUOUS_RENAME, and unregistered INVALID are derived
# from the current folder snapshot and never receive durable registry IDs.
# PENDING, ACTIVE, and ARCHIVED are public lifecycle states. REPLACED remains
# an internal tombstone so an accepted revision can keep its run provenance
# while releasing its path/hash uniqueness to the new ACTIVE row; it is never
# returned as a catalog resource.
# UNKNOWN, MATCHED, CHANGED, MISSING, and INVALID are durable integrity states.
_ARCHIVED_STATES = {"ARCHIVED"}


def _is_registered_owner(row: SessionTemplate) -> bool:
    return row.lifecycle_state != "REPLACED"


def _row_order(row: SessionTemplate) -> tuple[str, str]:
    created_at = row.created_at.isoformat() if row.created_at is not None else ""
    return created_at, row.template_id


def _observed_axes(row: SessionTemplate, observation: _FileObservation) -> tuple[str, str]:
    """Derive durable lifecycle/integrity axes from one trusted row and file observation."""

    if row.lifecycle_state == "PENDING":
        if observation.observed_hash == row.registered_hash:
            return "ACTIVE", "MATCHED"
        return "PENDING", "UNKNOWN"
    if observation.observed_hash is None:
        return row.lifecycle_state, "INVALID"
    integrity = "MATCHED" if observation.observed_hash == row.registered_hash else "CHANGED"
    return row.lifecycle_state, integrity


def _missing_axes(row: SessionTemplate) -> tuple[str, str]:
    if row.lifecycle_state == "PENDING":
        return "PENDING", "UNKNOWN"
    return row.lifecycle_state, "MISSING"


def _reconcile_row(
    row: SessionTemplate,
    observation: _FileObservation,
    *,
    lifecycle_state: str,
    integrity_state: str,
) -> SessionTemplate:
    return _repository.reconcile(
        row.template_id,
        relative_path=observation.relative_path,
        registered_hash=row.registered_hash,
        observed_hash=observation.observed_hash,
        filesystem_identity=observation.filesystem_identity,
        lifecycle_state=lifecycle_state,
        integrity_state=integrity_state,
        lineage_parent_id=row.lineage_parent_id,
    )


def _dependency_fingerprints(content: Mapping[str, Any]) -> list[dict[str, str]]:
    """Resolve dependency identities without copying any device-template definition."""

    dependencies: dict[str, dict[str, str]] = {}
    for flow in content.get("device_flows", []):
        if not isinstance(flow, Mapping):
            continue
        try:
            dependency, _ = resolve_device_template_reference(flow)
        except (DeviceTemplateNotFound, ValueError):
            continue
        payload = json.dumps(
            {
                "version": DEPENDENCY_FINGERPRINT_VERSION,
                "relative_path": dependency.file_path,
                "resolved_hash": dependency.content_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        dependencies[dependency.file_path] = {
            "relative_path": dependency.file_path,
            "resolved_hash": dependency.content_hash,
            "fingerprint": hashlib.sha256(payload).hexdigest(),
        }
    return [dependencies[path] for path in sorted(dependencies)]


def _sync_dependencies(template_id: str, content: Mapping[str, Any]) -> None:
    desired = _dependency_fingerprints(content)
    current = [
        (item.relative_path, item.resolved_hash, item.fingerprint)
        for item in _repository.list_dependencies(template_id)
    ]
    target = [(item["relative_path"], item["resolved_hash"], item["fingerprint"]) for item in desired]
    if current != target:
        _repository.replace_dependencies(template_id, desired)


def _reconcile_snapshot(observations: list[_FileObservation]) -> None:
    """Reconcile durable owners; unregistered observations remain derived in memory."""

    observations_by_path = {item.relative_path: item for item in observations}
    with transaction():
        owners = sorted(
            (row for row in _repository.list() if _is_registered_owner(row)),
            key=_row_order,
        )
        claimed_paths: set[str] = set()
        # Registered path always wins, even when its file is invalid or changed.
        for owner in owners:
            observation = observations_by_path.get(owner.relative_path)
            if observation is None:
                continue
            lifecycle_state, integrity_state = _observed_axes(owner, observation)
            owner = _reconcile_row(
                owner,
                observation,
                lifecycle_state=lifecycle_state,
                integrity_state=integrity_state,
            )
            if lifecycle_state == "ACTIVE" and integrity_state == "MATCHED" and observation.content:
                _sync_dependencies(owner.template_id, observation.content)
            claimed_paths.add(observation.relative_path)

        # Missing owners next use filesystem identity, then a unique hash. A
        # candidate row from an earlier scan is replaced by the preserved ID.
        for owner in owners:
            if owner.relative_path in claimed_paths:
                continue
            available = [item for item in observations if item.relative_path not in claimed_paths]
            identity_matches = [
                item
                for item in available
                if owner.filesystem_identity is not None
                and item.filesystem_identity == owner.filesystem_identity
            ]
            hash_matches = [
                item
                for item in available
                if item.observed_hash is not None and item.observed_hash == owner.registered_hash
            ]
            selected = identity_matches[0] if len(identity_matches) == 1 else None
            if selected is None and not identity_matches and len(hash_matches) == 1:
                selected = hash_matches[0]
            if selected is not None:
                lifecycle_state, integrity_state = _observed_axes(owner, selected)
                owner = _reconcile_row(
                    owner,
                    selected,
                    lifecycle_state=lifecycle_state,
                    integrity_state=integrity_state,
                )
                if lifecycle_state == "ACTIVE" and integrity_state == "MATCHED" and selected.content:
                    _sync_dependencies(owner.template_id, selected.content)
                claimed_paths.add(selected.relative_path)
                continue
            lifecycle_state, integrity_state = _missing_axes(owner)
            _repository.reconcile(
                owner.template_id,
                relative_path=owner.relative_path,
                registered_hash=owner.registered_hash,
                observed_hash=None,
                filesystem_identity=owner.filesystem_identity,
                lifecycle_state=lifecycle_state,
                integrity_state=integrity_state,
                lineage_parent_id=owner.lineage_parent_id,
            )


def _state_warning(state: str, relative_path: str) -> str | None:
    """Return the primary operator guidance for a non-runnable state."""

    messages = {
        "PENDING": f"template creation for {relative_path!r} has not converged",
        "MISSING": f"registered template file {relative_path!r} is missing",
        "DUPLICATE": "duplicate template configuration; open the registered original",
        "AMBIGUOUS_RENAME": "several files could be the renamed registered template",
        "CHANGED": "observed configuration differs from the trusted registered revision",
    }
    return messages.get(state)


def _catalog_from_snapshot(observations: list[_FileObservation]) -> builtins.list[SessionTemplateFile]:
    """Join reconciled metadata to immutable disk observations for API consumption."""

    observations_by_path = {item.relative_path: item for item in observations}
    # Superseded rows remain in SQLite solely to preserve source-template IDs
    # already copied into past runs. They are not current template resources.
    rows = [row for row in _repository.list() if row.lifecycle_state != "REPLACED"]
    result: list[SessionTemplateFile] = []
    claimed_paths: set[str] = set()
    duplicate_targets: dict[str, str | None] = {}
    for row in rows:
        observation = observations_by_path.get(row.relative_path)
        if observation is not None:
            claimed_paths.add(observation.relative_path)
            if observation.observed_hash is not None:
                duplicate_targets.setdefault(observation.observed_hash, row.template_id)
        warnings = builtins.list(observation.warnings) if observation is not None else []
        if observation is None and row.lifecycle_state == "ARCHIVED":
            warnings.append(f"registered template file {row.relative_path!r} is missing")
        if (
            observation is not None
            and row.lifecycle_state == "ARCHIVED"
            and row.observed_hash is not None
            and row.observed_hash != row.registered_hash
        ):
            warnings.append("archived template file differs from its registered revision")
        state_warning = _state_warning(row.state, row.relative_path)
        if state_warning is not None:
            warnings.append(state_warning)
        result.append(
            _attach_reference_warnings(
                SessionTemplateFile(
                    row,
                    observation.content if observation is not None else None,
                    warnings,
                    observation=observation,
                )
            )
        )

    missing_owners = [
        row
        for row in rows
        if row.integrity_state == "MISSING"
    ]
    ambiguous_by_path: dict[str, str] = {}
    for owner in missing_owners:
        matches = [
            item
            for item in observations
            if item.relative_path not in claimed_paths and item.observed_hash == owner.registered_hash
        ]
        if len(matches) > 1:
            for match in matches:
                ambiguous_by_path.setdefault(match.relative_path, owner.template_id)

    ambiguous_owner_ids = set(ambiguous_by_path.values())
    for resource in result:
        if resource.state == "MISSING":
            resource.derived_allowed_actions = (
                ["resolve_rename"] if resource.template_id in ambiguous_owner_ids else []
            )

    for observation in observations:
        if observation.relative_path in claimed_paths:
            continue
        warnings = builtins.list(observation.warnings)
        lineage_parent_id = ambiguous_by_path.get(observation.relative_path)
        duplicate_of_template_id: str | None = None
        if observation.observed_hash is None:
            state = "INVALID"
        elif lineage_parent_id is not None:
            state = "AMBIGUOUS_RENAME"
        elif observation.observed_hash in duplicate_targets:
            state = "DUPLICATE"
            duplicate_of_template_id = duplicate_targets[observation.observed_hash]
        else:
            state = "DISCOVERED"
            duplicate_targets[observation.observed_hash] = None
        state_warning = _state_warning(state, observation.relative_path)
        if state_warning is not None:
            warnings.append(state_warning)
        result.append(
            _attach_reference_warnings(
                SessionTemplateFile(
                    None,
                    observation.content,
                    warnings,
                    observation=observation,
                    derived_state=state,
                    derived_lineage_parent_id=lineage_parent_id,
                    derived_duplicate_of_template_id=duplicate_of_template_id,
                )
            )
        )
    return sorted(result, key=lambda item: (item.relative_path.lower(), item.template_id or ""))


def _observe(path: Path) -> SessionTemplateFile:
    safe_path = _direct_file(path)
    if safe_path is None:
        raise SessionTemplateNotFound(path.name)
    relative_path = safe_path.name
    return next(
        row for row in catalog() if row.relative_path == relative_path
    )


def catalog() -> builtins.list[SessionTemplateFile]:
    """Reconcile a stable folder snapshot and return current registry resources."""

    with _reconciliation_lock:
        for attempt in range(_RECONCILIATION_ATTEMPTS):
            observations = _snapshot_library()
            try:
                _reconcile_snapshot(observations)
                return _catalog_from_snapshot(observations)
            except IntegrityError as exc:
                if "unique constraint failed: session_templates" not in str(exc).lower():
                    raise
                if attempt + 1 == _RECONCILIATION_ATTEMPTS:
                    raise SessionTemplateReconciliationRetry() from exc
            except OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                if attempt + 1 == _RECONCILIATION_ATTEMPTS:
                    raise SessionTemplateReconciliationRetry() from exc
    raise RuntimeError("template reconciliation exhausted without a result")


def catalog_with_run_history() -> builtins.list[SessionTemplateFile]:
    """The catalog, each revision joined with the runs it has produced.

    The join is over *recorded provenance*, not a foreign key: a session copies
    its source template id at creation (see ``SessionSchema``), so an archived
    or edited revision keeps the runs it already started. That is the point — a
    template's run count is history, and history does not change when the file
    does. Superseded revisions stay internal and are not catalog rows.

    An unregistered file has no id for a session to have recorded, so it reports
    zero runs rather than an unknown count.
    """
    history = SessionRepository().run_history_by_source_template()
    rows = catalog()
    for row in rows:
        row.run_history = history.get(row.template_id, NO_RUNS) if row.template_id else NO_RUNS
    return rows


def list() -> builtins.list[SessionTemplateFile]:  # noqa: A001
    return catalog()


def get_by_name(name: str) -> SessionTemplateFile | None:
    """Prefer the active revision when several historical rows share a display name."""

    normalized = _normalize_name(name)
    matches = [template for template in catalog() if template.name == normalized]
    return (
        min(
            matches,
            key=lambda item: (
                item.state != "ACTIVE",
                item.created_at.isoformat() if item.created_at is not None else "",
                item.template_id or "",
            ),
        )
        if matches
        else None
    )


def get_by_reference(reference: str) -> SessionTemplateFile | None:
    relative_path = _relative_reference(reference)
    if relative_path is None:
        return None
    matches = [row for row in catalog() if row.relative_path == relative_path]
    return min(matches, key=lambda row: row.template_id or "") if matches else None


def get_by_id(template_id: str) -> SessionTemplateFile | None:
    current = next((row for row in catalog() if row.template_id == template_id), None)
    if current is not None:
        return current
    historical = _repository.get_by_id(template_id)
    if historical is None or historical.lifecycle_state != "REPLACED":
        return None
    return SessionTemplateFile(
        historical,
        None,
        ["A newer active revision superseded this historical revision."],
    )


@dataclass(frozen=True)
class RunTemplateResolution:
    """One registered revision, freshly reread, ready to freeze into a session.

    ``content`` is the canonical semantic object a session snapshot stores, and
    ``content_hash`` is recomputed from it here — not copied from the registry —
    so the caller can prove the snapshot/hash invariant holds for the bytes that
    were on disk at this instant.
    """

    template: SessionTemplateFile
    content: dict[str, Any]
    content_hash: str


def resolve_for_run(template_id: str) -> RunTemplateResolution:
    """Resolve a runnable revision and reread its file once, safely.

    Read-only wrapper over the existing primitives: it reconciles through
    ``catalog()``, then rereads the single registered file under the
    reconciliation lock with the same stat-signature guard ``_snapshot_library``
    uses, so a file edited mid-request aborts instead of being captured
    half-written. It never writes registry state and never re-runs a full
    catalog reconcile for the reread.

    Canonicalization is ``resolve_references=False`` because that is the form
    every disk observation — and therefore both ``registered_hash`` and
    ``observed_hash`` — is computed over. Resolving references here would
    produce a hash that cannot be checked against the registry and would fail a
    run for a missing *device* template rather than for template drift.
    """

    template = get_by_id(template_id)
    if template is None:
        raise SessionTemplateNotFound(template_id)

    _require_runnable(template)

    with _reconciliation_lock:
        path = _direct_file(_library_dir() / template.relative_path)
        if path is None:
            raise SessionTemplateNotFound(template.relative_path)
        before = _stat_signature(path)
        if before is None:
            raise SessionTemplateNotFound(template.relative_path)
        try:
            content = _load_library_file(path)
        except (OSError, ValueError) as exc:
            raise SessionTemplateStateConflict(
                f"Session template {template.name!r} could not be reread: {exc}",
                template.state,
                template.allowed_actions,
            ) from exc
        content_hash = _content_hash(content)
        if _stat_signature(path) != before:
            raise SessionTemplateStateConflict(
                f"Session template {template.name!r} changed while the run was being created.",
                template.state,
                template.allowed_actions,
            )

    if content_hash != template.registered_hash:
        raise SessionTemplateStateConflict(
            f"Session template {template.name!r} no longer matches its registered revision.",
            template.state,
            template.allowed_actions,
        )

    return RunTemplateResolution(
        template=template,
        content=content,
        content_hash=content_hash,
    )


def resolve_device_template_reference(flow: Mapping[str, Any]):
    """Resolve a device-template path/hash reference and return warnings."""

    path = flow.get("device_template_path")
    if not isinstance(path, str) or not path.strip():
        raise DeviceTemplateNotFound(str(path))
    expected_hash = flow.get("device_template_content_hash")
    template = device_templates.get_by_path(path)
    warnings: list[str] = []
    if template is not None:
        if expected_hash and expected_hash != template.content_hash:
            warnings.append(
                f"device template changed at {template.file_path}: expected {expected_hash}, found {template.content_hash}"
            )
        return template, warnings
    if not isinstance(expected_hash, str) or not expected_hash:
        raise DeviceTemplateNotFound(path)
    matches = device_templates.find_by_content_hash(expected_hash)
    if len(matches) == 1:
        warnings.append(f"device template path missing; using hash match {matches[0].file_path}")
        return matches[0], warnings
    if len(matches) > 1:
        raise ValueError(f"device template path {path!r} is missing and its hash matches multiple files")
    raise DeviceTemplateNotFound(path)


def _existing_for_path(path: Path) -> SessionTemplateFile | None:
    """Return the observed template at a path or reject an unsafe collision."""

    if not path.exists():
        return None
    safe_path = _direct_file(path)
    if safe_path is None:
        raise SessionTemplateNameExists(path.stem)
    return _observe(safe_path)


def _create_pending(path: Path, registered_hash: str) -> SessionTemplate:
    """Record intent before crossing the non-atomic database/file boundary."""

    with transaction():
        return _repository.create(
            relative_path=path.name,
            registered_hash=registered_hash,
        )


def create(name: str, raw_content: Mapping[str, Any]) -> SessionTemplateFile:
    """Create one file-backed template and converge its pending metadata to active."""

    normalized_name = _normalize_name(name)
    canonical = _canonicalize(raw_content)
    content_hash = _content_hash(canonical)
    path = _template_path(normalized_name)
    existing = _existing_for_path(path)
    if existing is not None:
        if existing.content_hash == content_hash and existing.content is not None:
            return existing
        raise SessionTemplateNameExists(normalized_name)

    duplicate = next(
        (
            template
            for template in catalog()
            if template.template_id is not None
            and template.registered_hash == content_hash
        ),
        None,
    )
    if duplicate is not None:
        raise SessionTemplateNameExists(duplicate.name)

    try:
        _create_pending(path, content_hash)
    except IntegrityError as exc:
        raise SessionTemplateNameExists(normalized_name) from exc
    _write_atomic(path, _to_toml(canonical))
    return _observe(path)


def register_discovered(reference: str) -> SessionTemplateFile:
    """Promote one unique discovered file to a trusted active template."""

    template = get_by_reference(reference)
    if template is None:
        raise SessionTemplateNotFound(reference)
    if template.state == "ACTIVE":
        return template
    if template.state != "DISCOVERED" or template.observed_hash is None:
        raise ValueError(f"template {reference!r} cannot be registered from state {template.state}")
    try:
        with transaction():
            row = _repository.create(
                relative_path=template.relative_path,
                registered_hash=template.observed_hash,
                observed_hash=template.observed_hash,
                filesystem_identity=template.filesystem_identity,
                lifecycle_state="ACTIVE",
                integrity_state="MATCHED",
            )
            if template.content is not None:
                _sync_dependencies(row.template_id, template.content)
            template_id = row.template_id
    except IntegrityError as exc:
        raise SessionTemplateNameExists(template.name) from exc
    registered = get_by_id(template_id)
    if registered is None:
        raise SessionTemplateNotFound(template_id)
    return registered


def archive(template_id: str) -> SessionTemplateFile:
    """Retire an active or changed template without modifying its TOML file."""

    template = get_by_id(template_id)
    if template is None:
        raise SessionTemplateNotFound(template_id)
    if template.state in _ARCHIVED_STATES:
        return template
    if template.state not in {"ACTIVE", "CHANGED", "MISSING"}:
        raise ValueError(f"template {template_id!r} cannot be archived from state {template.state}")
    with transaction():
        row = _repository.get_by_id(template_id)
        if row is None:
            raise SessionTemplateNotFound(template_id)
        _repository.transition(
            template_id,
            lifecycle_state="ARCHIVED",
            integrity_state=row.integrity_state,
        )
    archived = get_by_id(template_id)
    if archived is None:
        raise SessionTemplateNotFound(template_id)
    return archived


def accept_change(template_id: str) -> SessionTemplateFile:
    """Replace a changed revision with a new active ID while retaining history."""

    template = get_by_id(template_id)
    if template is None:
        raise SessionTemplateNotFound(template_id)
    if template.state != "CHANGED" or template.observed_hash is None:
        raise ValueError(f"template {template_id!r} cannot accept change from state {template.state}")
    original_path = template.relative_path
    with transaction():
        row = _repository.get_by_id(template_id)
        if row is None:
            raise SessionTemplateNotFound(template_id)
        _repository.reconcile(
            template_id,
            relative_path=row.relative_path,
            registered_hash=row.registered_hash,
            observed_hash=row.observed_hash,
            filesystem_identity=row.filesystem_identity,
            lifecycle_state="REPLACED",
            integrity_state="UNKNOWN",
            lineage_parent_id=row.lineage_parent_id,
        )
        accepted = _repository.create(
            relative_path=original_path,
            registered_hash=template.observed_hash,
            observed_hash=template.observed_hash,
            filesystem_identity=template.filesystem_identity,
            lifecycle_state="ACTIVE",
            integrity_state="MATCHED",
            lineage_parent_id=template_id,
        )
        if template.content is not None:
            _sync_dependencies(accepted.template_id, template.content)
        accepted_id = accepted.template_id
    result = get_by_id(accepted_id)
    if result is None:
        raise SessionTemplateNotFound(accepted_id)
    return result


def refresh_dependency_revision(template_id: str) -> SessionTemplateFile:
    """Pin current linked device-template hashes as a new session revision."""

    with _reconciliation_lock:
        template = get_by_id(template_id)
        if template is None:
            raise SessionTemplateNotFound(template_id)
        blockers = template.run_blockers
        if template.state != "ACTIVE" or not blockers or any(
            blocker["code"] != "device_template_changed" for blocker in blockers
        ):
            raise ValueError(
                f"template {template_id!r} has no device-template revisions that can be refreshed"
            )

        content = {
            "policy": template.content["policy"],
            "device_flows": [dict(flow) for flow in template.content["device_flows"]],
        }
        for blocker in blockers:
            flow_index = blocker["flow_index"]
            path = blocker["device_template_path"]
            try:
                current = device_templates.get_by_path(path)
            except (DeviceTemplateNotFound, ValueError) as exc:
                raise ValueError(f"device template {path!r} is no longer available") from exc
            if current is None:
                raise ValueError(f"device template {path!r} is no longer available")
            content["device_flows"][flow_index]["device_template_content_hash"] = current.content_hash

        path = _direct_file(_library_dir() / template.relative_path)
        if path is None:
            raise SessionTemplateNotFound(template.relative_path)
        if template.observed_hash != template.registered_hash:
            raise ValueError(
                f"template {template_id!r} changed while its dependency revisions were being reviewed"
            )
        _write_atomic(path, _to_toml(content))
        return accept_change(template_id)


def resolve_ambiguous_rename(template_id: str, selected_relative_path: str) -> SessionTemplateFile:
    """Preserve the old ID on the selected rename and mark other candidates duplicate."""

    catalog_rows = catalog()
    owner = next((row for row in catalog_rows if row.template_id == template_id), None)
    if owner is None:
        raise SessionTemplateNotFound(template_id)
    selected_path = _relative_reference(selected_relative_path)
    selected = next(
        (
            row
            for row in catalog_rows
            if row.relative_path == selected_path
            and row.state == "AMBIGUOUS_RENAME"
            and row.lineage_parent_id == template_id
        ),
        None,
    )
    if (
        owner.state not in {"MISSING", *_ARCHIVED_STATES}
        or selected is None
        or selected.observed_hash is None
    ):
        raise ValueError("selected file is not an ambiguous rename candidate for this template")
    selected_hash = selected.observed_hash
    selected_identity = selected.filesystem_identity
    with transaction():
        source = _repository.get_by_id(template_id)
        if source is None:
            raise SessionTemplateNotFound(selected_relative_path)
        _repository.reconcile(
            template_id,
            relative_path=selected.relative_path,
            registered_hash=source.registered_hash,
            observed_hash=selected_hash,
            filesystem_identity=selected_identity,
            lifecycle_state=source.lifecycle_state,
            integrity_state="MATCHED",
            lineage_parent_id=source.lineage_parent_id,
        )
        if selected.content is not None and source.lifecycle_state == "ACTIVE":
            _sync_dependencies(template_id, selected.content)
    result = get_by_id(template_id)
    if result is None:
        raise SessionTemplateNotFound(template_id)
    return result


def _relative_reference(reference: str) -> str | None:
    """Normalize a client reference while rejecting traversal and nested paths."""

    if not isinstance(reference, str):
        return None
    relative = reference.strip().replace("\\", "/")
    if relative.startswith("session-templates/"):
        relative = relative[len("session-templates/") :]
    if not relative or relative.startswith("/") or ".." in Path(relative).parts:
        return None
    if len(Path(relative).parts) != 1:
        return None
    if Path(relative).suffix.lower() != ".toml":
        relative = f"{relative}.toml"
    return relative


def _library_file_for_reference(reference: str) -> Path | None:
    relative = _relative_reference(reference)
    if relative is None:
        return None
    candidate = _library_dir() / relative
    return _direct_file(candidate)


def resolve_for_plan(reference: str) -> "_PlanTemplate":
    """Return runnable content only when the reconciled template is active."""

    if not isinstance(reference, str) or not reference.strip():
        raise SessionTemplateNotFound(reference if isinstance(reference, str) else "")
    template = get_by_reference(reference) if _looks_like_library_reference(reference) else get_by_name(reference)
    if template is None:
        raise SessionTemplateNotFound(reference)
    _require_runnable(template)
    return _PlanTemplate(name=template.name, content=template.content, reference=template.reference)


def _looks_like_library_reference(reference: str) -> bool:
    normalized = reference.replace("\\", "/")
    return normalized.startswith("session-templates/") or Path(normalized).suffix.lower() == ".toml"


class _PlanTemplate:
    __slots__ = ("name", "content", "reference")

    def __init__(self, *, name: str, content: Mapping[str, Any], reference: str):
        self.name = name
        self.content = dict(content)
        self.reference = reference


def read_source(reference: str) -> str:
    """Read one direct flat-library TOML file for the repair editor."""

    template = get_by_reference(reference)
    path = _library_file_for_reference(reference)
    if template is None or path is None:
        raise SessionTemplateNotFound(reference)
    try:
        if path.stat().st_size > 1_000_000:
            raise ValueError("session template TOML must be no larger than 1,000,000 bytes")
        return path.read_text(encoding="utf-8")
    except UnicodeError as exc:
        raise ValueError("session template source must be valid UTF-8 text") from exc
    except OSError as exc:
        raise SessionTemplateNotFound(reference) from exc


def repair_source(reference: str, source: str) -> SessionTemplateFile:
    """Validate and atomically replace one INVALID flat-library TOML file."""

    if not isinstance(source, str) or not source.strip():
        raise ValueError("session template TOML is required")
    if len(source) > 1_000_000:
        raise ValueError("session template TOML must be no larger than 1,000,000 characters")
    validate_toml(source)

    with _reconciliation_lock:
        template = get_by_reference(reference)
        path = _library_file_for_reference(reference)
        if template is None or path is None:
            raise SessionTemplateNotFound(reference)
        if template.state != "INVALID":
            raise ValueError(f"template {reference!r} can only be repaired while it is INVALID")
        _write_atomic(path, source)
        repaired = get_by_reference(reference)
    if repaired is None:
        raise SessionTemplateNotFound(reference)
    return repaired


def delete(name: str) -> None:
    """Delete an explicitly named template file and its corresponding metadata."""

    with _reconciliation_lock:
        template = get_by_reference(name) or get_by_name(name)
        if template is None:
            raise SessionTemplateNotFound(name)
        # A MISSING resource has durable registry metadata but no file to
        # unlink. If the file reappears before this lock is acquired, catalog()
        # changes the state and the normal explicit-file delete path applies.
        if template.state != "MISSING":
            path = _library_file_for_reference(template.relative_path)
            if path is None:
                raise SessionTemplateNotFound(name)
            path.unlink()
        with transaction():
            row = _repository.get_by_id(template.template_id)
            if row is not None:
                _repository.delete(row)


def import_config(source: Mapping[str, Any], *, name: str | None = None) -> SessionTemplateFile:
    """Import user-authored configuration through the normal safe create workflow."""

    if not isinstance(source, Mapping):
        raise ValueError("session template source must be a mapping")
    data = dict(source)
    template_name = name if name is not None else data.pop("name", None)
    if template_name is None:
        raise ValueError("session template name is required")
    data.pop("name", None)
    return create(template_name, data)


def validate_toml(source: str) -> dict[str, Any]:
    """Parse and semantically validate an in-memory session-template draft.

    Validation resolves device-template references but performs no writes.  The
    returned object is the same canonical content that ``create`` persists.
    """

    if not isinstance(source, str) or not source.strip():
        raise ValueError("session template TOML is required")
    try:
        parsed = tomllib.loads(source)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"invalid session template TOML: {exc}") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("session template TOML must parse to a mapping")
    return _canonicalize(parsed)


def import_toml(source: str, *, name: str) -> SessionTemplateFile:
    """Create a registered session template from validated TOML text."""

    return create(name, validate_toml(source))


def create_from_session(
    session: Session,
    name: str,
    *,
    include_hardware_id: bool = False,
) -> SessionTemplateFile:
    """Export a session snapshot as a reusable file-backed template."""

    from app.services.session_config import _template_for_export
    from app.services import device_configs

    device_flows: list[dict[str, Any]] = []
    for index, entry in enumerate(session.device_flows or [], start=1):
        template = _template_for_export(session, entry, index)
        flow: dict[str, Any] = {
            "device_template_path": template.file_path,
            "device_template_content_hash": template.content_hash,
            "sinks": entry["sinks"],
        }
        if entry.get("nickname") is not None:
            flow["nickname"] = entry["nickname"]
        if include_hardware_id and entry.get("device_config_id") is not None:
            config = device_configs.get_by_id(int(entry["device_config_id"]))
            if config is not None:
                flow["hardware_id"] = config.hardware_id
        device_flows.append(flow)

    policy = session.policy.value if hasattr(session.policy, "value") else str(session.policy)
    return create(name, {"policy": policy, "device_flows": device_flows})
