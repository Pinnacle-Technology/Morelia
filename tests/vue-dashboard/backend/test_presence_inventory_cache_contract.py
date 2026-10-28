from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock
from types import SimpleNamespace

import pytest
from app.runtime_child.device_connection import (
    current_connection_address,
    current_connection_present,
)
from app.runtime_child.morelia import MoreliaRuntime
from app.services import connections


class Clock:
    now = 100.0

    def __call__(self):
        return self.now


def flow(transport, serial="SERIAL", *, physical_id=None):
    address = (physical_id or serial) if transport == "d2xx" else "COM7"
    binding = {
        "transport": transport,
        "backend": "ftd2xx" if transport == "d2xx" else "pyserial",
        "hardware_id": serial,
        "address": address,
        "baudrate": 115200,
        "flow_control": "rts_cts" if transport == "d2xx" else "none",
        "read_timeout_ms": 250,
        "write_timeout_ms": 500,
        "selection_reason": "explicit_preference",
    }
    if physical_id is not None:
        binding["physical_id"] = physical_id
    return SimpleNamespace(
        connection=binding, hardware_id=serial, port=address, parameters={},
        device_id=f"pod8206hr:{serial}:{transport}", name="pod8206hr",
    )


def endpoint(candidate, **changes):
    return {**candidate.connection, **changes}


def observe(candidate, inventory):
    return current_connection_present(candidate, "pod8206hr", inventory=inventory)


def install_inventory(monkeypatch, transport, enumerate_endpoints):
    name = "list_native_endpoints" if transport == "d2xx" else "list_vcp_endpoints"
    monkeypatch.setattr(connections, name, enumerate_endpoints)


@pytest.mark.parametrize("transport", ["d2xx", "vcp"])
def test_presence_observes_expiry_and_reappearance(monkeypatch, transport):
    clock = Clock()
    inventory = connections.EndpointInventoryCache(clock=clock)
    candidate = flow(transport)
    rows = [endpoint(candidate)]
    calls = []

    def enumerate_endpoints():
        calls.append(clock.now)
        return list(rows)

    install_inventory(monkeypatch, transport, enumerate_endpoints)
    assert observe(candidate, inventory) is True
    rows.clear()
    clock.now = 100.999
    assert observe(candidate, inventory) is True
    assert len(calls) == 1
    clock.now = 101.0
    assert observe(candidate, inventory) is False
    assert len(calls) == 2
    rows.append(endpoint(candidate))
    clock.now = 102.0
    assert observe(candidate, inventory) is True
    assert len(calls) == 3


def _concurrent_scans(monkeypatch, ttl_seconds):
    inventory = connections.EndpointInventoryCache(ttl_seconds=ttl_seconds, clock=Clock())
    candidates = [flow("d2xx", f"NATIVE{index}") for index in range(12)] + [flow("vcp")]
    calls = {"d2xx": 0, "vcp": 0}
    entered = {"d2xx": Event(), "vcp": Event()}
    release = Event()
    lock = Lock()
    ready = Barrier(len(candidates) + 1)

    def enumerate_endpoints(transport):
        with lock:
            calls[transport] += 1
        entered[transport].set()
        assert release.wait(5), "test did not release hardware enumeration"
        return [endpoint(item) for item in candidates if item.connection["transport"] == transport]

    for transport in calls:
        install_inventory(monkeypatch, transport, lambda transport=transport: enumerate_endpoints(transport))

    def check(candidate):
        ready.wait(timeout=5)
        return observe(candidate, inventory)

    with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
        futures = [executor.submit(check, candidate) for candidate in candidates]
        try:
            ready.wait(timeout=5)
            assert entered["d2xx"].wait(5)
            assert entered["vcp"].wait(5), "FTDI scan blocked the independent VCP inventory"
        finally:
            release.set()
        assert all(future.result(timeout=5) is True for future in futures)
    return calls


def _assert_shared(calls):
    assert calls == {"d2xx": 1, "vcp": 1}, "presence checks repeated a transport scan"


def test_concurrent_mixed_watchers_share_one_scan_per_transport(monkeypatch):
    _assert_shared(_concurrent_scans(monkeypatch, ttl_seconds=1.0))


def test_scan_sharing_check_rejects_disabled_cache(monkeypatch):
    calls = _concurrent_scans(monkeypatch, ttl_seconds=0.0)
    assert calls == {"d2xx": 12, "vcp": 1}
    with pytest.raises(AssertionError, match="repeated a transport scan"):
        _assert_shared(calls)


@pytest.mark.parametrize("transport", ["d2xx", "vcp"])
@pytest.mark.parametrize("error_class", [OSError, RuntimeError])
def test_enumeration_failure_remains_unknown_then_recovers(monkeypatch, transport, error_class):
    clock = Clock()
    inventory = connections.EndpointInventoryCache(clock=clock)
    candidate = flow(transport)
    state = [None]

    def enumerate_endpoints():
        if state[0] is not None:
            raise state[0]
        return [endpoint(candidate)]

    install_inventory(monkeypatch, transport, enumerate_endpoints)
    assert observe(candidate, inventory) is True
    state[0] = error_class("inventory unavailable")
    clock.now += 1.0
    expected = "backend_unavailable" if transport == "vcp" and error_class is OSError else "enumeration_failed"
    for _ in range(2):
        with pytest.raises(connections.ConnectionResolutionError) as error:
            observe(candidate, inventory)
        assert error.value.code == expected
    state[0] = None
    clock.now += 1.0
    assert observe(candidate, inventory) is True


@pytest.mark.parametrize("transport", ["d2xx", "vcp"])
@pytest.mark.parametrize("defect", ["duplicate", "conflict"])
def test_cached_inventory_preserves_ambiguous_identity_errors(monkeypatch, transport, defect):
    candidate = flow(transport)
    rows = [endpoint(candidate)]
    if defect == "duplicate":
        rows.append(endpoint(candidate))
    else:
        rows[0]["identity_conflict"] = True
    install_inventory(monkeypatch, transport, lambda: rows)
    inventory = connections.EndpointInventoryCache(clock=Clock())
    for _ in range(2):
        with pytest.raises(connections.ConnectionResolutionError) as error:
            observe(candidate, inventory)
        assert error.value.code == "identity_ambiguous"


def test_blank_native_descriptor_does_not_prove_absence(monkeypatch):
    install_inventory(monkeypatch, "d2xx", lambda: [{"address": "", "hardware_id": ""}])
    def unavailable():
        raise OSError("Windows presence query unavailable")
    monkeypatch.setattr(connections, "list_windows_usb_endpoints", unavailable)
    inventory = connections.EndpointInventoryCache(clock=Clock())
    with pytest.raises(connections.ConnectionResolutionError) as error:
        observe(flow("d2xx"), inventory)
    assert error.value.code == ("enumeration_failed" if connections.sys.platform == "win32" else "identity_unavailable")


def test_cached_endpoint_aliases_cannot_be_changed_by_a_reader(monkeypatch):
    rows = [{"address": "PHYSICAL", "hardware_id": "PHYSICAL", "aliases": ["LOGICAL-A"]}]
    install_inventory(monkeypatch, "d2xx", lambda: rows)
    inventory = connections.EndpointInventoryCache(clock=Clock())
    first = inventory.get("d2xx")
    first[0]["aliases"].append("WRONG")
    first[0]["address"] = "WRONG"
    first.clear()
    expected = [{"address": "PHYSICAL", "hardware_id": "PHYSICAL", "aliases": ["LOGICAL-A"]}]
    assert inventory.get("d2xx") == expected
    cached = inventory.get("d2xx")
    cached[0]["aliases"].append("WRONG")
    assert inventory.get("d2xx") == expected


def test_slow_scan_does_not_extend_the_freshness_window(monkeypatch):
    clock = Clock()
    inventory = connections.EndpointInventoryCache(clock=clock)
    candidate = flow("vcp")
    calls = []

    def enumerate_endpoints():
        calls.append(True)
        clock.now += 2.0
        return [endpoint(candidate)]

    install_inventory(monkeypatch, "vcp", enumerate_endpoints)
    assert observe(candidate, inventory) is True
    assert observe(candidate, inventory) is True
    assert len(calls) == 2


@pytest.mark.parametrize("transport", ["d2xx", "vcp"])
def test_busy_inventory_wait_is_bounded_and_does_not_report_absence(monkeypatch, transport):
    inventory = connections.EndpointInventoryCache(clock=Clock(), lock_timeout_seconds=0.05)
    candidate = flow(transport)
    entered = Event()
    release = Event()

    def enumerate_endpoints():
        entered.set()
        assert release.wait(5)
        return [endpoint(candidate)]

    install_inventory(monkeypatch, transport, enumerate_endpoints)
    with ThreadPoolExecutor(max_workers=1) as executor:
        active = executor.submit(observe, candidate, inventory)
        try:
            assert entered.wait(5)
            with pytest.raises(connections.ConnectionResolutionError):
                observe(candidate, inventory)
        finally:
            release.set()
        assert active.result(timeout=5) is True


@pytest.mark.parametrize("transport", ["d2xx", "vcp"])
def test_missing_frozen_transport_never_falls_back(monkeypatch, transport):
    other = "vcp" if transport == "d2xx" else "d2xx"
    calls = []
    install_inventory(monkeypatch, transport, list)

    def forbidden_fallback():
        calls.append(other)
        return [endpoint(flow(other))]

    install_inventory(monkeypatch, other, forbidden_fallback)
    assert observe(flow(transport), connections.EndpointInventoryCache(clock=Clock())) is False
    assert calls == []


def test_native_presence_uses_frozen_serial_without_logical_alias(monkeypatch):
    candidate = flow("d2xx", "LOGICAL-A", physical_id="PHYSICAL")
    rows = [{"address": "PHYSICAL", "hardware_id": "PHYSICAL", "backend": "ftd2xx"}]
    install_inventory(monkeypatch, "d2xx", lambda: rows)
    assert observe(candidate, connections.EndpointInventoryCache(clock=Clock())) is True


def test_vcp_recovery_refreshes_address_despite_presence_cache(monkeypatch):
    candidate = flow("vcp")
    rows = [endpoint(candidate)]
    calls = []

    def enumerate_endpoints():
        calls.append(True)
        return list(rows)

    install_inventory(monkeypatch, "vcp", enumerate_endpoints)
    inventory = connections.EndpointInventoryCache(clock=Clock())
    assert observe(candidate, inventory) is True
    rows[:] = [endpoint(candidate, address="COM22")]
    assert current_connection_address(candidate, "pod8206hr") == "COM22"
    assert candidate.connection["address"] == "COM7"
    assert len(calls) == 2
    assert observe(candidate, inventory) is True
    assert len(calls) == 2


def test_runtime_owns_presence_cache_but_recovery_remains_fresh(monkeypatch, app):
    candidates = [flow("d2xx"), flow("vcp")]
    calls = {"d2xx": 0, "vcp": 0}
    rows = {item.connection["transport"]: [endpoint(item)] for item in candidates}

    def enumerate_endpoints(transport):
        calls[transport] += 1
        return list(rows[transport])

    for transport in calls:
        install_inventory(monkeypatch, transport, lambda transport=transport: enumerate_endpoints(transport))
    manifest = SimpleNamespace(device_flows=candidates)
    first = MoreliaRuntime(manifest=manifest, on_report=lambda report: None)
    second = MoreliaRuntime(manifest=manifest, on_report=lambda report: None)
    assert first._presence_inventory is not second._presence_inventory
    with app.app_context():
        for _ in range(2):
            assert first._stream_port_present(0) is True
            assert first._stream_port_present(1) is True
        assert calls == {"d2xx": 1, "vcp": 1}
        rows["vcp"] = [endpoint(candidates[1], address="COM31")]
        assert first._resolve_stream_port(1) == "COM31"
        assert calls == {"d2xx": 1, "vcp": 2}
        assert second._stream_port_present(0) is True
        assert second._stream_port_present(1) is True
        assert calls == {"d2xx": 2, "vcp": 3}
