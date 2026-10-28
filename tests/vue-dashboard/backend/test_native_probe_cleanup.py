"""Real probe/scan/lease ownership with faults only at the native USB boundary."""

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from Morelia.Watchdog.dataflowMonitor import DataFlowMonitor
from Morelia.Watchdog.watchdog import StreamWatcher
from app.discovery import ftdi_transport as transport
from app.discovery import native_scan
from app.discovery.managed_pods import Pod
from app.domain.connections import normalize_connection
from app.watchdog_process.hardware_lease import HardwareLeaseSet
from tests.mocks.packet.MockControlPacket import MockControlPacket


SERIAL = "PROBE-CLEANUP-0001"
PHYSICAL = "USB-PROBE-CLEANUP"
ALIAS = "PROBE-CLEANUP-LOGICAL"
ENDPOINT = {
    "hardware_id": SERIAL, "physical_id": PHYSICAL, "aliases": [ALIAS],
    "address": SERIAL, "transport": "d2xx", "backend": "ftd2xx", "flags": 0,
}


@pytest.mark.parametrize("worker_alive", [None, False, True])
@pytest.mark.parametrize("presence", [False, True, None])
def test_native_recovery_probes_present_serial_after_owner_release(usb, monkeypatch, worker_alive, presence):
    from ftd2xx.ftd2xx import DeviceError

    def unknown_inventory(_index):
        if presence is None:
            raise RuntimeError("Native inventory contains unidentified interfaces")
        return presence

    monitor = DataFlowMonitor(port_resolver=lambda _index: SERIAL,
                              port_presence_checker=unknown_inventory)
    pod = Pod(SERIAL, use_d2xx=True, probe=True)
    worker = SimpleNamespace(is_alive=lambda: worker_alive, exitcode=1) if worker_alive is not None else None
    monitor.flowgraph = SimpleNamespace(_network=[(pod, [])], _workers=[worker])
    monitor.snapshot_config = [{"source": {"source_dict": pod.get_dict()}}]
    restarts = []
    monkeypatch.setattr(monitor, "restart_one_stream",
                        lambda index: restarts.append(index) or {"ok": True})
    monkeypatch.setattr(monitor, "poll_stream_queue_roundtrip", lambda **_kwargs: {"ok": True})
    subject = StreamWatcher(0, monitor, failure_threshold=3, max_heartbeat_age_sec=10,
                            interval_sec=1, timeout_sec=5, recovery_policy="automate",
                            native_transport=True, publish=lambda *_args: None)
    native_open = transport._open_native_serial
    requested = []

    def unavailable(serial):
        requested.append(serial)
        raise DeviceError("DEVICE_NOT_OPENED")

    monkeypatch.setattr(transport, "_open_native_serial", unavailable)
    result = subject._try_reconnect()
    if presence is not True:
        assert result["action"] == ("port_check_failed" if presence is None else "waiting_for_port")
        assert result["hardware_present"] is presence
        assert requested == restarts == []
        assert usb.opens == 0
        assert subject._port_release_started_at is None
        presence = True
        result = subject._try_reconnect()
    assert result["action"] == "waiting_for_port_release"
    assert result["hardware_present"] is True
    assert restarts == []
    assert subject._recovery_attempt_count == 0
    assert requested == ([] if worker is not None else [SERIAL] * 3)

    monkeypatch.setattr(transport, "_open_native_serial", native_open)
    result = subject._try_reconnect()
    if worker is not None:
        assert result["action"] == "waiting_for_port_release"
        assert usb.opens == 0
        assert restarts == []
    else:
        assert result["action"] == "connection_restored"
        assert restarts == [0]
        assert usb.opens == usb.closes == 1
        assert pod._port is None


@pytest.fixture
def usb(monkeypatch):
    state = SimpleNamespace(setup_error=False, close_error=False, opens=0, closes=0,
                            writes=[], pods=[], handles=[])

    class UsbHandle:
        def __init__(self):
            self.pending = b""
            self.closed = False

        def getDeviceInfo(self):
            return {"serial": SERIAL.encode()}

        def setBaudRate(self, _baudrate):
            if state.setup_error:
                raise OSError("injected native setup failure")

        def setDataCharacteristics(self, *_args):
            pass

        def setFlowControl(self, *_args):
            pass

        def setTimeouts(self, *_args):
            pass

        def purge(self, _mask):
            self.pending = b""

        def write(self, message):
            state.writes.append(message)
            assert int(message[1:5], 16) == 8, "Probe must only issue TYPE"
            self.pending += MockControlPacket(8, b"30").to_bytes()
            return len(message)

        def read(self, size):
            result, self.pending = self.pending[:size], self.pending[size:]
            return result

        def close(self):
            state.closes += 1
            if state.close_error:
                raise OSError("injected native close failure")
            assert not self.closed, "Released handle closed twice"
            self.closed = True

    def open_native(serial):
        assert serial == SERIAL
        state.opens += 1
        handle = UsbHandle()
        state.handles.append(handle)
        return handle

    original_init = Pod.__init__
    def record_pod(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        state.pods.append(self)

    monkeypatch.setattr(transport, "_open_native_serial", open_native)
    monkeypatch.setattr(Pod, "__init__", record_pod)
    monkeypatch.setattr(native_scan, "annotate_endpoints", lambda endpoints: endpoints)
    yield state
    state.close_error = False
    transport.retry_pending_cleanup(SERIAL)
    for pod in state.pods:
        pod.cleanup()


def scan(directory):
    return native_scan.scan_endpoints(
        port_lister=lambda: [], native_lister=lambda: [dict(ENDPOINT)],
        directory=directory,
    )


def other_process_lease(directory, hardware_id):
    """Ask an unrelated process to take the real OS file lock."""
    lease_module = Path(sys.modules[HardwareLeaseSet.__module__].__file__)
    code = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location('lease', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
lease = module.HardwareLeaseSet.for_devices([(sys.argv[3], 'OTHER-PORT')], directory=sys.argv[2])
try:
    lease.acquire()
except module.HardwareLeaseBusy:
    print('busy')
else:
    lease.release()
    print('available')
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(lease_module), str(directory), hardware_id],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@pytest.mark.parametrize("failure", ["constructor_setup", "probe_close"])
def test_probe_cleanup_failure_propagates_owner_before_or_after_port_assignment(usb, failure):
    usb.setup_error = failure == "constructor_setup"
    usb.close_error = True
    with pytest.raises(transport.NativeHandleCleanupError) as caught:
        native_scan.probe_endpoint(dict(ENDPOINT), normalize_connection({"preference": "d2xx"}))
    owner = caught.value
    assert transport.pending_cleanup(SERIAL) is owner
    assert owner.serial == SERIAL
    assert usb.opens == 1
    assert not usb.handles[0].closed
    pod, = usb.pods
    assert (pod._port is None) == (failure == "constructor_setup")
    assert bool(usb.writes) == (failure == "probe_close")
    with pytest.raises(transport.NativeHandleCleanupError) as retry:
        pod.close_port()
    assert retry.value is owner
    usb.close_error = False
    pod.close_port()
    assert pod._port is None
    assert usb.handles[0].closed
    assert transport.pending_cleanup(SERIAL) is None


@pytest.mark.parametrize("failure", ["constructor_setup", "probe_close"])
def test_scan_retains_physical_lease_until_native_cleanup_then_allows_reprobe(usb, tmp_path, failure):
    usb.setup_error = failure == "constructor_setup"
    usb.close_error = True
    rows, warnings = scan(tmp_path)
    owner = transport.pending_cleanup(SERIAL)
    assert owner is not None
    assert rows[0].availability == "unopenable"
    assert rows[0].endpoints[0]["error_code"] == "cleanup_pending"
    assert any(warning["code"] == "cleanup_pending" for warning in warnings)
    assert usb.opens == 1
    assert any(retained is owner and lease.acquired
               for retained, lease in native_scan._cleanup_leases.values())
    for identity in (SERIAL, PHYSICAL, ALIAS):
        assert other_process_lease(tmp_path, identity) == "busy"

    for _ in range(2):
        rows, warnings = scan(tmp_path)
        assert rows[0].availability == "unopenable"
        assert any(warning["code"] == "cleanup_pending" for warning in warnings)
        assert transport.pending_cleanup(SERIAL) is owner
        assert usb.opens == 1, "Repeated scan opened a replacement before confirmed close"
    assert other_process_lease(tmp_path, PHYSICAL) == "busy"

    usb.setup_error = usb.close_error = False
    rows, warnings = scan(tmp_path)
    assert not warnings
    assert rows[0].availability == "available"
    assert usb.opens == 2
    assert all(handle.closed for handle in usb.handles)
    assert transport.pending_cleanup(SERIAL) is None
    assert all(retained is not owner for retained, _lease in native_scan._cleanup_leases.values())
    for identity in (SERIAL, PHYSICAL, ALIAS):
        assert other_process_lease(tmp_path, identity) == "available"


def test_constructor_cleanup_failure_preserves_owned_queues_until_handle_release(usb):
    pod = Pod(SERIAL, use_d2xx=True)
    usb.setup_error = usb.close_error = True
    with pytest.raises(transport.NativeHandleCleanupError):
        pod.open_port()
    assert pod._port is None
    assert transport.pending_cleanup(SERIAL) is not None
    with pytest.raises(transport.NativeHandleCleanupError):
        pod.cleanup()
    assert not pod._queues_cleaned
    assert not pod._write_queue._closed
    assert not pod._read_queue._closed
    usb.close_error = False
    pod.cleanup()
    assert pod._queues_cleaned
    assert pod._write_queue._closed
    assert pod._read_queue._closed
    assert transport.pending_cleanup(SERIAL) is None
