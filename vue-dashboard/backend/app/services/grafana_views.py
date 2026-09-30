from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from app.database import db
from app.domain.enums import DeviceType
from app.models.runtime_manifest import RuntimeManifest
from app.services import device_configs, sessions

_CHANNELS = {
    DeviceType.POD8206HR.value: ("CH0", "CH1", "CH2", "TTL1", "TTL2", "TTL3", "TTL4"),
    DeviceType.POD8401HR.value: (
        "CHA",
        "CHB",
        "CHC",
        "CHD",
        "aEXT0",
        "aEXT1",
        "TTL1",
        "TTL2",
        "TTL3",
        "TTL4",
    ),
}
_SCOPE = "device_filters"
_NOTICE = (
    "M4 device/filter view. Data is selected by device and display filters "
    "and is not isolated to this session."
)
_DATA_DETAIL_OPTIONS = ("raw", "auto", "10ms", "25ms", "50ms", "100ms", "250ms", "500ms", "1s")
_DEFAULT_DATA_DETAIL = "250ms"
_TTL_OPTIONS = ("TTL1", "TTL2", "TTL3", "TTL4")
_CH_OPTIONS = ("CH0", "CH1", "CH2")
_NO_TTL = "__none__"
_NO_CH = "__none__"


class InvalidGrafanaSelection(ValueError):
    """The caller selected a target or channel outside the server allowlist."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ARG002
        return None


def _base_response(*, state: str, message: str | None, retry_after: int) -> dict:
    return {
        "state": state,
        "scope": _SCOPE,
        "notice": _NOTICE,
        "targets": [],
        "selected_target_id": None,
        "data_detail_options": list(_DATA_DETAIL_OPTIONS),
        "selected_data_detail": _DEFAULT_DATA_DETAIL,
        "ttl_options": list(_TTL_OPTIONS),
        "selected_ttl": list(_TTL_OPTIONS),
        "ch_options": list(_CH_OPTIONS),
        "selected_ch": list(_CH_OPTIONS),
        "embed_url": None,
        "open_url": None,
        "retry_after_seconds": retry_after,
        "message": message,
    }


def _safe_http_url(value: object) -> str | None:
    text = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(text)
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return text


def _default_health_probe(url: str, timeout: float) -> bool:
    opener = build_opener(_NoRedirect())
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with opener.open(request, timeout=timeout) as response:
            if not 200 <= int(response.status) < 300:
                return False
            # Bound the response read and accept Grafana's normal health JSON.
            payload = response.read(4097)
            if len(payload) > 4096:
                return False
            if not payload:
                return True
            decoded = json.loads(payload)
            return isinstance(decoded, Mapping)
    except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError):
        return False


def _pending_identity(config) -> tuple[str, str]:
    """Mirror manifest identity before a scheduled run has resolved one."""

    raw_type = getattr(config.device_type, "value", config.device_type)
    device_type = str(raw_type)
    source = getattr(config, "source_template", None)
    if isinstance(source, str) and source.strip():
        return device_type, source.strip()
    return device_type, device_type


def _manifest_identities(session_id: int) -> list[tuple[str, str] | None]:
    """Read the latest immutable runtime identity; never create a manifest."""

    row = db.session.scalars(
        db.select(RuntimeManifest)
        .where(RuntimeManifest.session_id == session_id)
        .order_by(RuntimeManifest.id.desc())
    ).first()
    if row is None or not isinstance(row.content, Mapping):
        return []
    flows = row.content.get("device_flows")
    if not isinstance(flows, list):
        return []
    identities: list[tuple[str, str] | None] = []
    for flow in flows:
        if not isinstance(flow, Mapping):
            identities.append(None)
            continue
        device_id = str(flow.get("device_id") or "")
        device_type = device_id.split(":", 1)[0]
        name = str(flow.get("name") or "").strip()
        identities.append((device_type, name) if device_type and name else None)
    return identities


def _configured_destination(config: Mapping) -> tuple[str, str, str, str] | None:
    values = tuple(
        str(config.get(key) or "").strip()
        for key in (
            "GRAFANA_INFLUX_URL",
            "GRAFANA_INFLUX_ORG",
            "GRAFANA_INFLUX_BUCKET",
            "GRAFANA_INFLUX_MEASUREMENT",
        )
    )
    return values if all(values) else None


def _sink_destination(parameters: Mapping) -> tuple[str, str, str, str]:
    return (
        str(parameters.get("url") or "").strip(),
        str(parameters.get("org") or "").strip(),
        str(parameters.get("bucket") or "").strip(),
        str(parameters.get("measurement") or "default-measurement").strip(),
    )


def _targets(session, expected_destination: tuple[str, str, str, str]) -> list[dict]:
    targets: list[dict] = []
    frozen_identities = _manifest_identities(session.id)
    flows = session.device_flows if isinstance(session.device_flows, list) else []
    for flow_index, flow in enumerate(flows):
        if not isinstance(flow, Mapping):
            continue
        raw_config_id = flow.get("device_config_id")
        try:
            config_id = int(raw_config_id)
        except (TypeError, ValueError):
            continue
        identity = frozen_identities[flow_index] if flow_index < len(frozen_identities) else None
        if identity is None:
            device = device_configs.get_by_id(config_id)
            if device is None:
                continue
            device_type, device_name = _pending_identity(device)
        else:
            device_type, device_name = identity
        channels = _CHANNELS.get(device_type)
        if channels is None:
            continue
        sinks = flow.get("sinks")
        if not isinstance(sinks, list):
            continue
        for sink_index, sink in enumerate(sinks):
            if not isinstance(sink, Mapping) or sink.get("sink_type") != "influx":
                continue
            parameters = sink.get("sink_parameters")
            if not isinstance(parameters, Mapping):
                continue
            if _sink_destination(parameters) != expected_destination:
                continue
            label = str(flow.get("nickname") or f"Device flow {flow_index + 1}").strip()
            targets.append(
                {
                    "id": f"{flow_index}:{sink_index}",
                    "label": label,
                    "device": device_name,
                    "channels": list(channels),
                }
            )
    return targets


def _panel_urls(
    *,
    base_url: str,
    uid: str,
    slug: str,
    panel_id: int,
    bucket: str,
    measurement: str,
    device: str,
    data_detail: str,
    show_ttl: tuple[str, ...],
    show_ch: tuple[str, ...],
) -> tuple[str, str]:
    variables = {
        "orgId": "1",
        "from": "now-30s",
        "to": "now",
        "refresh": "500ms",
        "var-bucket": bucket,
        "var-measurement": measurement,
        "var-device": device,
        "var-data_detail": data_detail,
        "var-show_ttl": list(show_ttl) or [_NO_TTL],
        "var-show_ch": list(show_ch) or [_NO_CH],
    }
    embed_query = urlencode(
        {**variables, "panelId": str(panel_id), "kiosk": ""}, doseq=True
    )
    open_query = urlencode({**variables, "viewPanel": str(panel_id)}, doseq=True)
    return (
        f"{base_url}/d-solo/{uid}/{slug}?{embed_query}",
        f"{base_url}/d/{uid}/{slug}?{open_query}",
    )


def _m4_controls(
    data_detail: str | None,
    show_ttl: list[str] | tuple[str, ...] | None,
    show_ch: list[str] | tuple[str, ...] | None,
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    selected_detail = data_detail or _DEFAULT_DATA_DETAIL
    if selected_detail not in _DATA_DETAIL_OPTIONS:
        raise InvalidGrafanaSelection("Unsupported Grafana data detail.")

    requested_ttl = tuple(show_ttl) if show_ttl is not None else _TTL_OPTIONS
    if _NO_TTL in requested_ttl:
        if len(requested_ttl) != 1:
            raise InvalidGrafanaSelection("No-TTL cannot be combined with TTL channels.")
        requested_ttl = ()
    if any(value not in _TTL_OPTIONS for value in requested_ttl):
        raise InvalidGrafanaSelection("Unsupported Grafana TTL channel.")
    selected_ttl = tuple(value for value in _TTL_OPTIONS if value in requested_ttl)

    requested_ch = tuple(show_ch) if show_ch is not None else _CH_OPTIONS
    if _NO_CH in requested_ch:
        if len(requested_ch) != 1:
            raise InvalidGrafanaSelection("No-CH cannot be combined with CH channels.")
        requested_ch = ()
    if any(value not in _CH_OPTIONS for value in requested_ch):
        raise InvalidGrafanaSelection("Unsupported Grafana CH channel.")
    selected_ch = tuple(value for value in _CH_OPTIONS if value in requested_ch)
    return selected_detail, selected_ttl, selected_ch


def describe(
    session_id: int,
    *,
    config: Mapping,
    target_id: str | None = None,
    data_detail: str | None = None,
    show_ttl: list[str] | tuple[str, ...] | None = None,
    show_ch: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Return the M4 comparison graph descriptor without mutating run state."""

    session = sessions.get(session_id)
    retry_after = max(1, int(config.get("GRAFANA_RETRY_AFTER_SECONDS", 10)))
    base_url = _safe_http_url(config.get("GRAFANA_PUBLIC_URL"))
    expected_destination = _configured_destination(config)
    uid = str(config.get("GRAFANA_DASHBOARD_UID") or "").strip()
    slug = str(config.get("GRAFANA_DASHBOARD_SLUG") or "").strip()
    try:
        panel_id = int(config.get("GRAFANA_PANEL_ID", 2))
    except (TypeError, ValueError):
        panel_id = 0
    if not base_url or expected_destination is None or not uid or not slug or panel_id < 1:
        return _base_response(
            state="not_configured",
            message="The M4 comparison Grafana view is not configured by this deployment.",
            retry_after=retry_after,
        )

    targets = _targets(session, expected_destination)
    if not targets:
        return _base_response(
            state="not_applicable",
            message=(
                "This run has no Influx destination connected to the M4 comparison "
                "Grafana datasource."
            ),
            retry_after=retry_after,
        )

    selected = targets[0]
    if target_id is not None:
        selected = next((item for item in targets if item["id"] == target_id), None)
        if selected is None:
            raise InvalidGrafanaSelection("Unknown Grafana target for this session.")
    selected_data_detail, selected_ttl, selected_ch = _m4_controls(
        data_detail, show_ttl, show_ch
    )

    response = _base_response(state="ready", message=None, retry_after=retry_after)
    response.update(
        {
            "targets": targets,
            "selected_target_id": selected["id"],
            "selected_data_detail": selected_data_detail,
            "selected_ttl": list(selected_ttl),
            "selected_ch": list(selected_ch),
        }
    )

    health_url = _safe_http_url(config.get("GRAFANA_HEALTH_URL"))
    if health_url is None:
        health_url = f"{base_url}/api/health"
    timeout = max(0.05, float(config.get("GRAFANA_HEALTH_TIMEOUT_SECONDS", 1.0)))
    probe: Callable[[str, float], bool] = _default_health_probe
    configured_probe = config.get("GRAFANA_HEALTH_PROBE")
    if bool(config.get("TESTING")) and callable(configured_probe):
        probe = configured_probe
    try:
        available = bool(probe(health_url, timeout))
    except Exception:
        available = False
    if not available:
        response.update(
            {
                "state": "unavailable",
                "message": "Grafana is currently unavailable. Acquisition is unaffected.",
            }
        )
        return response

    embed_url, open_url = _panel_urls(
        base_url=base_url,
        uid=uid,
        slug=slug,
        panel_id=panel_id,
        bucket=expected_destination[2],
        measurement=expected_destination[3],
        device=selected["device"],
        data_detail=selected_data_detail,
        show_ttl=selected_ttl,
        show_ch=selected_ch,
    )
    response.update({"embed_url": embed_url, "open_url": open_url})
    return response
