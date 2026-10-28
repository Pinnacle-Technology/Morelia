"""Resolve connection intent to an exact, current endpoint without opening hardware."""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Callable
from copy import deepcopy

from app.diagnostic_events import diagnostic_operation
from app.discovery.windows_usb_presence import (
    list_present_native_endpoints as list_windows_usb_endpoints,
)
from app.domain.connections import normalize_connection, normalize_resolved_connection
from app.domain.enums import DeviceType
from app.services.device_ports import PortResolutionError

NATIVE_DEVICE_TYPES = frozenset({DeviceType.POD8206HR.value, DeviceType.POD8401HR.value})


class ConnectionResolutionError(PortResolutionError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class EndpointInventoryCache:
    """Share short-lived presence observations within one runtime."""

    def __init__(
        self, *, ttl_seconds: float = 1.0,
        clock: Callable[[], float] | None = None,
        lock_timeout_seconds: float = 1.0,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock if clock is not None else time.monotonic
        self._lock_timeout_seconds = lock_timeout_seconds
        self._locks = {transport: threading.Lock() for transport in ("vcp", "d2xx", "windows_usb")}
        self._snapshots: dict[str, tuple[float, list[dict[str, object]]]] = {}

    def get(self, transport: str) -> list[dict[str, object]]:
        with diagnostic_operation("inventory_cache", transport=transport) as evidence:
            evidence.update(stage="cache_lock", ttl_seconds=self._ttl_seconds)
            lock = self._locks[transport]
            # A stalled scan must not indefinitely hold up other watchers.
            if not lock.acquire(timeout=self._lock_timeout_seconds):
                raise TimeoutError(f"Timed out waiting for {transport} inventory")
            try:
                now = self._clock()
                snapshot = self._snapshots.get(transport)
                if snapshot is not None and now - snapshot[0] < self._ttl_seconds:
                    evidence.update(stage="complete", cached=True,
                                    cache_age_ms=round((now - snapshot[0]) * 1000, 3),
                                    rows=snapshot[1], returned_count=len(snapshot[1]))
                    return deepcopy(snapshot[1])
                self._snapshots.pop(transport, None)
                evidence.update(stage="enumerate", cached=False, cache_age_ms=0)
                if transport == "windows_usb":
                    rows = list_windows_usb_endpoints()
                else:
                    rows = list_vcp_endpoints() if transport == "vcp" else list_native_endpoints()
                # Slow scans must not extend the freshness of their observations.
                self._snapshots[transport] = (now, deepcopy(rows))
                evidence.update(stage="complete", rows=rows, returned_count=len(rows))
                return deepcopy(rows)
            finally:
                lock.release()


def list_vcp_endpoints() -> list[dict[str, object]]:
    from serial.tools.list_ports import comports

    from app.discovery.windows_ftdi_identity import annotate_endpoints

    return annotate_endpoints([
        {
            "hardware_id": p.serial_number,
            "address": p.device,
            "transport": "vcp",
            "backend": "pyserial",
            "vid": p.vid,
            "pid": p.pid,
        }
        for p in comports()
    ])


def list_native_endpoints(*, reconcile: bool = True) -> list[dict[str, object]]:
    # Honor MORELIA_SRC using the existing lazy import boundary.
    from app.discovery.pod_scan import _import_devices_module

    _import_devices_module()
    from app.discovery.ftdi_transport import list_d2xx_devices
    from app.discovery.windows_ftdi_identity import annotate_endpoints

    endpoints = [
        {
            "hardware_id": item["serial"],
            "address": item["serial"],
            "transport": "d2xx",
            "backend": "ftd2xx",
            "label": item.get("description") or item["serial"],
            "flags": item.get("flags", 0),
            "type": item.get("type"),
            "id": item.get("id"),
            "index": item.get("index"),
            "location": item.get("location"),
        }
        for item in list_d2xx_devices(native_only=True)
    ]
    return annotate_endpoints(endpoints) if reconcile else endpoints


def _unique_match(rows: list[dict[str, object]], hardware_id: str) -> dict | None:
    matches = [
        row
        for row in rows
        if row.get("hardware_id") == hardware_id or hardware_id in row.get("aliases", ())
    ]
    if len(matches) > 1 or any(row.get("identity_conflict") for row in matches):
        raise ConnectionResolutionError(
            "identity_ambiguous",
            f"Ambiguous hardware_id {hardware_id!r}: conflicting identity evidence or endpoints",
        )
    if matches and not matches[0].get("address"):
        raise ConnectionResolutionError("address_missing", "Matching endpoint has no address")
    return matches[0] if matches else None


def resolve_connection(
    hardware_id: str, connection: dict | None, device_type, *,
    inventory: EndpointInventoryCache | None = None,
) -> dict:
    """AUTO chooses VCP first; explicit intent never switches transports.

    Enumeration errors propagate as typed diagnostics, never as an empty inventory.
    Device indexes, descriptions, and stale configured addresses are never fallbacks.
    """
    if not isinstance(hardware_id, str) or not hardware_id or hardware_id != hardware_id.strip():
        raise ConnectionResolutionError("identity_missing", "An exact hardware_id is required")
    intent = normalize_connection(connection)
    preference = intent["preference"]
    model = getattr(device_type, "value", device_type)
    choices = ("vcp", "d2xx") if preference == "auto" else (preference,)
    for transport in choices:
        if transport == "d2xx" and model not in NATIVE_DEVICE_TYPES:
            raise ConnectionResolutionError(
                "transport_unsupported", f"Native D2XX streaming is not supported for {model!r}"
            )
        try:
            rows = inventory.get(transport) if inventory is not None else (
                list_vcp_endpoints() if transport == "vcp" else list_native_endpoints()
            )
        except Exception as exc:
            raise ConnectionResolutionError(
                "backend_unavailable"
                if isinstance(exc, (ImportError, OSError))
                else "enumeration_failed",
                f"Cannot enumerate {transport} endpoints: {exc}",
            ) from exc
        endpoint = _unique_match(rows, hardware_id)
        if endpoint is None:
            continue
        resolved = {
            "transport": transport,
            "backend": "pyserial" if transport == "vcp" else "ftd2xx",
            "address": endpoint["address"],
            "hardware_id": hardware_id,
            "baudrate": intent["baudrate"],
            "flow_control": intent.get("flow_control")
            or ("none" if transport == "vcp" else "rts_cts"),
            "read_timeout_ms": intent["read_timeout_ms"],
            "write_timeout_ms": intent["write_timeout_ms"],
            "selection_reason": "auto_vcp_first"
            if preference == "auto" and transport == "vcp"
            else "auto_native_only"
            if preference == "auto"
            else "explicit_preference",
        }
        if endpoint.get("physical_id"):
            resolved["physical_id"] = endpoint["physical_id"]
        return resolved
    message = (
        f"No serial port found for hardware_id {hardware_id!r}"
        if preference == "vcp"
        else f"No {preference} endpoint found for exact hardware_id {hardware_id!r}"
    )
    raise ConnectionResolutionError("not_found", message)


def native_connection_present(
    resolved: dict, *, inventory: EndpointInventoryCache | None = None,
) -> bool:
    """Locate the native device, using Windows USB presence for blank D2XX entries."""
    with diagnostic_operation("native_presence", transport="d2xx",
                              port=resolved.get("address") if isinstance(resolved, dict) else None,
                              hardware_id=resolved.get("hardware_id") if isinstance(resolved, dict) else None) as evidence:
        return _native_connection_present(resolved, inventory=inventory, evidence=evidence)


def _native_connection_present(resolved, *, inventory, evidence):
    frozen = normalize_resolved_connection(resolved)
    evidence.update(stage="enumerate", target=frozen, present=None, presence_source="d2xx")
    if frozen["transport"] != "d2xx":
        raise ValueError("Native presence requires a frozen D2XX binding")
    try:
        rows = inventory.get("d2xx") if inventory is not None else list_native_endpoints()
    except Exception as exc:
        raise ConnectionResolutionError("enumeration_failed", f"Native presence observation failed: {exc}") from exc
    serial = frozen["address"]
    logical = frozen["hardware_id"]
    matches = [row for row in rows if row.get("address") == serial]
    related = [row for row in rows if row.get("hardware_id") == logical
               or logical in row.get("aliases", ()) or row.get("address") == serial]
    evidence.update(stage="match_identity", rows=rows, returned_count=len(rows),
                    match_count=len(matches), unidentified_count=sum(
                        not row.get("address") or not row.get("hardware_id") for row in rows))
    if len(matches) > 1 or any(
        row.get("identity_conflict")
        or row.get("physical_id", row.get("address")) != serial
        or row.get("backend") != frozen["backend"]
        for row in related
    ):
        raise ConnectionResolutionError("identity_ambiguous", "Frozen native physical identity has conflicting evidence")
    if matches:
        evidence.update(stage="complete", present=True)
        return True
    if any(not row.get("address") or not row.get("hardware_id") for row in rows):
        if sys.platform == "win32":
            return _windows_native_present(serial, inventory=inventory, evidence=evidence)
        raise ConnectionResolutionError("identity_unavailable", "Native inventory contains unidentified interfaces; presence is unknown")
    evidence.update(stage="complete", present=False)
    return False


def _windows_native_present(serial, *, inventory, evidence):
    evidence.update(stage="windows_usb_presence", presence_source="windows_pnp")
    try:
        rows = (
            inventory.get("windows_usb") if inventory is not None else list_windows_usb_endpoints()
        )
    except Exception as exc:
        raise ConnectionResolutionError(
            "enumeration_failed", f"Windows USB presence observation failed: {exc}",
        ) from exc
    # Windows device instance IDs are case-insensitive. The D2XX open selector stays exact.
    matches = [row for row in rows if (row.get("address") or "").casefold() == serial.casefold()]
    evidence.update(windows_rows=rows, windows_match_count=len(matches))
    if len(matches) > 1:
        raise ConnectionResolutionError(
            "identity_ambiguous", "Multiple present USB devices match the native serial",
        )
    if not matches and any(not row.get("address") for row in rows):
        raise ConnectionResolutionError(
            "identity_unavailable",
            "Windows USB inventory contains an unsupported or unidentified FTDI device",
        )
    present = bool(matches)
    evidence.update(stage="complete", present=present)
    return present


def validate_current_connection(resolved: dict) -> dict:
    """Refresh an immutable run's address within its frozen transport/backend.

    This is read-only enumeration, suitable before open and recovery. It never
    chooses another transport and never modifies a persisted runtime manifest.
    """
    frozen = normalize_resolved_connection(resolved)
    if frozen["transport"] == "d2xx":
        if not native_connection_present(frozen):
            raise ConnectionResolutionError("not_found", f"Native device {frozen['address']!r} is absent")
        return frozen
    try:
        rows = list_vcp_endpoints() if frozen["transport"] == "vcp" else list_native_endpoints()
    except Exception as exc:
        raise ConnectionResolutionError(
            "backend_unavailable"
            if isinstance(exc, (ImportError, OSError))
            else "enumeration_failed",
            f"Cannot enumerate frozen {frozen['transport']} connection: {exc}",
        ) from exc
    endpoint = _unique_match(rows, frozen["hardware_id"])
    if endpoint is None:
        raise ConnectionResolutionError(
            "not_found", f"Hardware {frozen['hardware_id']!r} is absent"
        )
    if frozen.get("physical_id") and endpoint.get("physical_id") != frozen["physical_id"]:
        raise ConnectionResolutionError("identity_ambiguous", "Physical identity evidence changed")
    return {
        **frozen,
        "address": endpoint["address"],
        **({"physical_id": endpoint["physical_id"]} if endpoint.get("physical_id") else {}),
    }
