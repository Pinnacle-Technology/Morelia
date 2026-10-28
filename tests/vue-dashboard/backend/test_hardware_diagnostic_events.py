import io
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace

import pytest
import structlog
from app import diagnostic_events
from app.discovery import ftdi_transport, windows_ftdi_identity
from app.logging_config import configure_logging
from app.services.connections import (
    ConnectionResolutionError,
    EndpointInventoryCache,
    native_connection_present,
)

from . import test_ftdi_transport
from .test_presence_inventory_cache_contract import flow

native = test_ftdi_transport.native


@pytest.fixture
def diagnostic_log(tmp_path):
    diagnostic_events._recent.clear()
    configure_logging(
        SimpleNamespace(
            LOG_LEVEL="INFO", LOG_FORMAT="json", DIAGNOSTIC_LOG_DIR=tmp_path
        ),
        stream=io.StringIO(),
        diagnostic_layer="watchdog-driver",
        diagnostic_context={"session_id": 73, "runtime_id": "runtime-test"},
    )
    path = tmp_path / "session-73" / "watchdog-driver.jsonl"
    yield (
        lambda: (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )
    )
    diagnostic_events._recent.clear()


def test_raw_blank_inventory_and_failed_decision_reach_session_file(
    monkeypatch, diagnostic_log
):
    from app.services import connections

    # Exercise the D2XX-only error without consulting this test host's USB devices.
    monkeypatch.setattr(connections, "sys", SimpleNamespace(platform="linux"))
    raw = {
        "serial": b"",
        "description": b"",
        "flags": 1,
        "type": 3,
        "id": 0,
        "location": 0,
        "handle": object(),
        "payload": "DO-NOT-LOG",
    }
    calls = []

    def detail(index, update):
        calls.append((index, update))
        return raw

    monkeypatch.setattr(
        ftdi_transport,
        "ftd",
        SimpleNamespace(
            createDeviceInfoList=lambda: 1,
            getDeviceInfoDetail=detail,
        ),
    )
    monkeypatch.setattr(windows_ftdi_identity, "annotate_endpoints", lambda rows: rows)
    with (
        structlog.contextvars.bound_contextvars(device_id="device-A", stream_index=2),
        pytest.raises(ConnectionResolutionError, match="unidentified"),
    ):
        native_connection_present(
            flow("d2xx", "TARGET").connection, inventory=EndpointInventoryCache()
        )
    events = diagnostic_log()
    inventory = next(event for event in events if event["action"] == "native_inventory")
    presence = next(event for event in events if event["action"] == "native_presence")
    (row,) = inventory["evidence"]["rows"]
    assert row["serial_raw"] == "b''"
    assert row["serial_hex"] == ""
    assert row["reported_open"] is True
    assert row["id"] == row["location"] == 0
    assert inventory["evidence"]["expected_count"] == 1
    assert presence["port"] == "TARGET"
    assert presence["error_code"] == "identity_unavailable"
    assert presence["evidence"]["unidentified_count"] == 1
    assert presence["evidence"]["match_count"] == 0
    assert "ConnectionResolutionError" in presence["exception"]
    assert inventory["operation_id"] == presence["operation_id"]
    assert all(
        event["session_id"] == 73 and event["runtime_id"] == "runtime-test"
        for event in events
    )
    assert all(
        event["device_id"] == "device-A" and event["stream_index"] == 2
        for event in events
    )
    assert "DO-NOT-LOG" not in json.dumps(events)
    assert "handle" not in row
    assert calls == [(0, False)]


def test_partial_inventory_retains_rows_and_original_exception(
    monkeypatch, diagnostic_log
):
    failure = OSError("USB changed during enumeration")

    def detail(index, update):
        if index == 1:
            raise failure
        return {"serial": b"B", "description": b"POD", "flags": 0}

    monkeypatch.setattr(
        ftdi_transport,
        "ftd",
        SimpleNamespace(
            createDeviceInfoList=lambda: 2,
            getDeviceInfoDetail=detail,
        ),
    )
    with pytest.raises(OSError) as raised:
        ftdi_transport.list_d2xx_devices()
    assert raised.value is failure
    (event,) = diagnostic_log()
    assert event["outcome"] == "failed"
    assert event["evidence"]["stage"] == "get_device_info_detail"
    assert event["evidence"]["failed_index"] == 1
    assert event["evidence"]["rows"][0]["serial_hex"] == "42"
    assert event["evidence"]["expected_count"] == 2
    assert "USB changed during enumeration" in event["exception"]


def test_repeated_events_are_summarized_and_changes_emit_immediately(
    monkeypatch, diagnostic_log
):
    now = [100.0]
    monkeypatch.setattr(diagnostic_events.time, "monotonic", lambda: now[0])

    def record(present):
        with diagnostic_events.diagnostic_operation("presence", port="A") as evidence:
            evidence["present"] = present

    record(True)
    record(True)
    record(True)
    now[0] = 159.9
    record(False)
    events = diagnostic_log()
    assert len(events) == 2
    assert events[-1]["suppressed_count"] == 2
    assert events[-1]["evidence"]["present"] is False
    record(False)
    now[0] = 220
    record(False)
    assert len(diagnostic_log()) == 3
    assert diagnostic_log()[-1]["suppressed_count"] == 1


def test_evidence_is_bounded_and_filters_payloads(diagnostic_log):
    with diagnostic_events.diagnostic_operation(
        "inventory", password="DO-NOT-LOG"
    ) as evidence:
        evidence.update(
            payload="DO-NOT-LOG",
            rows=[
                {
                    "serial": "A" * 500,
                    "samples": ["DO-NOT-LOG"],
                    "handle": "DO-NOT-LOG",
                }
            ]
            * 100,
        )
    (event,) = diagnostic_log()
    assert "DO-NOT-LOG" not in json.dumps(event)
    assert len(event["evidence"]["rows"]) == 65
    assert event["evidence"]["rows"][-1] == {"omitted_count": 36}
    assert event["evidence"]["rows"][0]["serial"].endswith("<truncated>")


def test_logging_failure_does_not_replace_hardware_failure(monkeypatch):
    def broken(*args):
        raise OSError("diagnostics unavailable")

    monkeypatch.setattr(diagnostic_events, "_record", broken)
    original = ValueError("hardware failed")
    with (
        pytest.raises(ValueError) as raised,
        diagnostic_events.diagnostic_operation("open"),
    ):
        raise original
    assert raised.value is original


def test_native_open_and_close_failures_include_stage_and_cleanup(
    native, diagnostic_log
):
    _ftd, handle = native
    failure = OSError("configuration failed")
    handle.setBaudRate.side_effect = failure
    with pytest.raises(OSError) as raised:
        ftdi_transport.D2XXPortIO("000123A")
    assert raised.value is failure
    event = diagnostic_log()[-1]
    assert event["action"] == "native_open"
    assert event["evidence"]["stage"] == "configure_handle"
    assert event["evidence"]["rows"][0]["serial"] == "000123A"
    assert event["evidence"]["handle_retained"] is False
    assert event["evidence"]["cleanup_pending"] is False

    handle.setBaudRate.side_effect = None
    port = ftdi_transport.D2XXPortIO("000123A")
    handle.close.side_effect = OSError("close failed")
    try:
        with pytest.raises(ftdi_transport.NativeHandleCleanupError):
            port.close_serial_port()
        event = diagnostic_log()[-1]
        assert event["action"] == "native_close"
        assert event["evidence"]["handle_retained"] is True
        assert event["evidence"]["cleanup_pending"] is True
        assert "close failed" in event["exception"]
    finally:
        handle.close.side_effect = None
        port.close_serial_port()
    assert diagnostic_log()[-1]["outcome"] == "completed"
    assert diagnostic_log()[-1]["previous_outcome"] == "failed"


def test_cache_hits_do_not_create_a_new_inventory_or_log_each_tick(
    monkeypatch, diagnostic_log
):
    from app.services import connections

    calls = []
    monkeypatch.setattr(
        connections, "list_native_endpoints", lambda: calls.append(True) or []
    )
    cache = EndpointInventoryCache(clock=lambda: 100.0)
    for _ in range(10):
        assert cache.get("d2xx") == []
    assert calls == [True]
    events = diagnostic_log()
    assert len(events) == 1
    assert events[0]["evidence"]["cached"] is False


def test_error_chain_and_failed_to_successful_transition_are_logged(diagnostic_log):
    for _ in range(2):
        with (
            pytest.raises(RuntimeError),
            diagnostic_events.diagnostic_operation("resolve", port="A"),
        ):
            try:
                raise OSError("driver unavailable")
            except OSError as cause:
                raise RuntimeError("presence unavailable") from cause
    with diagnostic_events.diagnostic_operation("resolve", port="A") as evidence:
        evidence["present"] = True
    events = diagnostic_log()
    assert len(events) == 2
    assert "OSError: driver unavailable" in events[0]["exception"]
    assert "RuntimeError: presence unavailable" in events[0]["exception"]
    assert events[1]["previous_outcome"] == "failed"
    assert events[1]["suppressed_count"] == 1


def test_shared_log_filter_sanitizes_evidence_from_other_callers(diagnostic_log):
    structlog.get_logger("other_component").info(
        "diagnostic",
        evidence={
            "stage": "read",
            "password": "DO-NOT-LOG",
            "rows": [{"serial": "A", "samples": ["DO-NOT-LOG"]}],
        },
    )
    (event,) = diagnostic_log()
    assert event["evidence"] == {"stage": "read", "rows": [{"serial": "A"}]}
    assert "DO-NOT-LOG" not in json.dumps(event)


def test_parallel_devices_keep_separate_operation_context(diagnostic_log):
    barrier = Barrier(2)

    def observe(serial):
        with diagnostic_events.diagnostic_operation("presence", device_id=serial) as evidence:
            barrier.wait(timeout=5)
            with diagnostic_events.diagnostic_operation("inventory") as nested:
                nested["serial"] = serial
            evidence["present"] = True

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(observe, ["A", "B"]))
    events = diagnostic_log()
    assert len(events) == 4
    operation_ids = set()
    for serial in ("A", "B"):
        device_events = [event for event in events if event["device_id"] == serial]
        assert len(device_events) == 2
        assert device_events[0]["operation_id"] == device_events[1]["operation_id"]
        assert device_events[0]["evidence"]["serial"] == serial
        operation_ids.add(device_events[0]["operation_id"])
    assert len(operation_ids) == 2


def test_repeat_tracking_has_a_fixed_capacity(monkeypatch, diagnostic_log):
    monkeypatch.setattr(diagnostic_events, "_MAX_KEYS", 3)
    for serial in ("A", "B", "C", "D", "A"):
        with diagnostic_events.diagnostic_operation("presence", port=serial):
            pass
    assert len(diagnostic_events._recent) == 3
    assert len(diagnostic_log()) == 5
