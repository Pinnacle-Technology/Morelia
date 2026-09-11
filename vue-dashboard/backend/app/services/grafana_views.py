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
