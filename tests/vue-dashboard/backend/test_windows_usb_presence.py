import ctypes
from types import SimpleNamespace

import pytest
from app.discovery import windows_usb_presence
from app.services import connections

from . import test_hardware_diagnostic_events as diagnostic_tests
from . import test_native_recovery_timing as timing
from .test_presence_inventory_cache_contract import Clock, flow

clock = timing.clock
diagnostic_log = diagnostic_tests.diagnostic_log

# Session 72 retained these identities while AB0MZ1B2 was unplugged.
PEERS = [
    "AB0MZ1AC",
    "AB0MZ1AD",
    "AB0MZ1AE",
    "AB0MZ1AF",
    "AB0MZ1AN",
    "AB0MZ1AV",
    "AB0MZ1AY",
    "AB0MZ1AZ",
    "AB0MZ1B3",
    "AB0MZ4ZY",
    "AB0MZ501",
    "AB0MZBJB",
    "AB0MZBJL",
]
TARGET = "AB0MZ1B2"


@pytest.fixture
def usb_inventory(monkeypatch):
    monkeypatch.setattr(connections, "sys", SimpleNamespace(platform="win32"))
    instances = [rf"USB\VID_0403&PID_6001\{serial}" for serial in PEERS]
    monkeypatch.setattr(
        windows_usb_presence, "_present_usb_instance_ids", lambda: list(instances)
    )
    monkeypatch.setattr(
        connections,
        "list_native_endpoints",
        lambda: (
            [
                {"address": "", "hardware_id": "", "flags": 1, "backend": "ftd2xx"}
                for _ in range(12)
            ]
            + [{"address": "AB0MZBJL", "hardware_id": "AB0MZBJL", "backend": "ftd2xx"}]
        ),
    )
    return instances


def test_twelve_blank_peers_do_not_hide_unplug_and_replug(usb_inventory):
    now = Clock()
    cache = connections.EndpointInventoryCache(clock=now)
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    assert connections.native_connection_present(binding, inventory=cache) is False
    usb_inventory.append(rf"USB\VID_0403&PID_6001\{TARGET}")
    now.now += 1
    assert connections.native_connection_present(binding, inventory=cache) is True
    usb_inventory.pop()
    now.now += 1
    assert connections.native_connection_present(binding, inventory=cache) is False


def test_windows_evidence_reaches_session_log(usb_inventory, diagnostic_log):
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    assert connections.native_connection_present(binding) is False
    events = diagnostic_log()
    presence = next(event for event in events if event["action"] == "native_presence")
    windows = next(event for event in events if event["action"] == "windows_usb_inventory")
    assert presence["outcome"] == "completed"
    assert presence["evidence"]["present"] is False
    assert presence["evidence"]["presence_source"] == "windows_pnp"
    assert presence["evidence"]["windows_match_count"] == 0
    assert presence["evidence"]["unidentified_count"] == 12
    assert len(presence["evidence"]["windows_rows"]) == 13
    assert windows["evidence"]["rows"][0]["instance_id"] == usb_inventory[0]
    assert windows["operation_id"] == presence["operation_id"]


def test_absent_native_device_waits_then_reconnects(usb_inventory, monkeypatch, clock):
    subject, monitor = timing.make_watcher(monkeypatch, clock)
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    cache = connections.EndpointInventoryCache(clock=lambda: clock[0])
    monkeypatch.setattr(
        monitor,
        "is_stream_port_present",
        lambda index: connections.native_connection_present(binding, inventory=cache),
    )
    monitor.openable = False
    for timestamp in (100, 131, 3600):
        clock[0] = timestamp
        result = subject._try_reconnect()
        assert result["action"] == "waiting_for_port"
        assert result["hardware_present"] is False
        assert subject._port_release_started_at is None
    assert monitor.probe_calls == monitor.restart_calls == 0
    usb_inventory.append(rf"USB\VID_0403&PID_6001\{TARGET}")
    clock[0] += 1
    monitor.openable = True
    assert subject._try_reconnect()["action"] == "waiting_for_heartbeat"
    assert monitor.restart_calls == 1


def test_present_only_api_flags_and_buffer_resize(monkeypatch):
    identifiers = [rf"USB\VID_0403&PID_6001\{TARGET}", r"USB\VID_0403&PID_6010\DUAL"]
    encoded = "\0".join(identifiers) + "\0\0"
    reads = []

    def size(output, enumerator, flags):
        assert enumerator == "USB"
        assert flags == 0x101
        ctypes.cast(output, ctypes.POINTER(ctypes.c_ulong))[0] = len(encoded)
        return 0

    def read(enumerator, buffer, length, flags):
        assert enumerator == "USB" and flags == 0x101
        reads.append(True)
        if len(reads) == 1:
            return 0x1A
        for index, value in enumerate(encoded):
            buffer[index] = value
        return 0

    monkeypatch.setattr(
        windows_usb_presence,
        "_configuration_manager",
        lambda: SimpleNamespace(
            CM_Get_Device_ID_List_SizeW=size,
            CM_Get_Device_ID_ListW=read,
        ),
    )
    rows = windows_usb_presence.list_present_native_endpoints()
    assert [row["address"] for row in rows] == [TARGET, "DUALA", "DUALB"]
    assert len(reads) == 2


def test_present_but_busy_keeps_thirty_second_access_limit(
    usb_inventory, monkeypatch, clock
):
    usb_inventory.append(rf"USB\VID_0403&PID_6001\{TARGET}")
    subject, monitor = timing.make_watcher(monkeypatch, clock)
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    monkeypatch.setattr(
        monitor,
        "is_stream_port_present",
        lambda index: connections.native_connection_present(binding),
    )
    monitor.openable = False
    for timestamp in (100, 129.9):
        clock[0] = timestamp
        result = subject._try_reconnect()
        assert result["action"] == "waiting_for_port_release"
        assert result["hardware_present"] is True
    clock[0] = 130
    assert (
        subject._try_reconnect()["initiating_failure_reason"] == "port_release_timeout"
    )
    assert monitor.restart_calls == 0


def test_windows_snapshot_is_shared_and_expired_failure_is_not_reused(
    usb_inventory, monkeypatch
):
    now = Clock()
    cache = connections.EndpointInventoryCache(clock=now)
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    calls = []

    def observe():
        calls.append(True)
        if len(calls) > 1:
            raise OSError("Windows query failed")
        return list(usb_inventory)

    monkeypatch.setattr(windows_usb_presence, "_present_usb_instance_ids", observe)
    for _ in range(12):
        assert connections.native_connection_present(binding, inventory=cache) is False
    assert len(calls) == 1
    now.now += 1
    with pytest.raises(connections.ConnectionResolutionError) as exc:
        connections.native_connection_present(binding, inventory=cache)
    assert exc.value.code == "enumeration_failed"
    assert "windows_usb" not in cache._snapshots


@pytest.mark.parametrize(
    "instance",
    [
        r"USB\VID_0403&PID_9999\UNSUPPORTED",
        r"USB\VID_0403&PID_6001\6&123&0&2",
    ],
)
def test_unidentifiable_windows_parent_cannot_prove_absence(usb_inventory, instance):
    usb_inventory.append(instance)
    binding = flow("d2xx", "AB0MZ1B2A", physical_id=TARGET).connection
    with pytest.raises(connections.ConnectionResolutionError) as exc:
        connections.native_connection_present(binding)
    assert exc.value.code == "identity_unavailable"


@pytest.mark.parametrize("failure_stage", ["size", "read"])
def test_windows_errors_cannot_become_empty_inventory(monkeypatch, failure_stage):
    def size(output, enumerator, flags):
        ctypes.cast(output, ctypes.POINTER(ctypes.c_ulong))[0] = 2
        return 0x33 if failure_stage == "size" else 0

    monkeypatch.setattr(
        windows_usb_presence,
        "_configuration_manager",
        lambda: SimpleNamespace(
            CM_Get_Device_ID_List_SizeW=size,
            CM_Get_Device_ID_ListW=lambda *args: 0x33,
        ),
    )
    with pytest.raises(OSError, match="CONFIGRET"):
        windows_usb_presence.list_present_native_endpoints()


def test_only_usb_parents_map_to_native_selectors(monkeypatch):
    monkeypatch.setattr(
        windows_usb_presence,
        "_present_usb_instance_ids",
        lambda: [
            r"USB\VID_0403&PID_6001\BASEA",
            r"USB\VID_0403&PID_6011\FOUR",
            r"USB\VID_0403&PID_6010&MI_00\6&123&0&0000",
            r"USB\VID_9999&PID_6001\FOREIGN",
        ],
    )
    rows = windows_usb_presence.list_present_native_endpoints()
    assert [row["address"] for row in rows] == [
        "BASEA",
        "FOURA",
        "FOURB",
        "FOURC",
        "FOURD",
    ]
