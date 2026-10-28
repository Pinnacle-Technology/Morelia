import pytest

from .test_native_recovery_timing import clock, make_watcher


@pytest.mark.parametrize("native", [True, False])
def test_absent_device_waits_without_opening_or_spending_retries(monkeypatch, clock, native):
    subject, monitor = make_watcher(monkeypatch, clock, native=native)
    monitor.present = False
    monitor.openable = False
    for timestamp in (100, 130, 3600):
        clock[0] = timestamp
        result = subject._try_reconnect()
        assert result["action"] == "waiting_for_port"
        assert result["hardware_present"] is False
    assert monitor.probe_calls == monitor.restart_calls == 0
    assert subject._recovery_attempt_count == 0
    assert subject._port_release_started_at is None

    monitor.present = monitor.openable = True
    assert subject._try_reconnect()["action"] == "waiting_for_heartbeat"
    assert monitor.restart_calls == 1


@pytest.mark.parametrize("native,deadline", [(True, 130), (False, 115)])
def test_unplug_at_access_deadline_clears_timer(monkeypatch, clock, native, deadline):
    subject, monitor = make_watcher(monkeypatch, clock, native=native)
    monitor.openable = False
    assert subject._try_reconnect()["action"] == "waiting_for_port_release"
    monitor.present = False
    clock[0] = deadline
    result = subject._try_reconnect()
    assert result["action"] == "waiting_for_port"
    assert result["hardware_present"] is False
    assert subject._port_release_started_at is None

    clock[0] = 3600
    monitor.present = True
    assert subject._try_reconnect()["action"] == "waiting_for_port_release"
    clock[0] += deadline - 100
    result = subject._try_reconnect()
    assert result["initiating_failure_reason"] == "port_release_timeout"
    assert result["hardware_present"] is True


def test_native_inventory_error_is_unknown_even_at_access_deadline(monkeypatch, clock):
    subject, monitor = make_watcher(monkeypatch, clock)
    monitor.openable = False
    assert subject._try_reconnect()["action"] == "waiting_for_port_release"

    def inventory_failed(_index):
        raise RuntimeError("USB inventory unavailable")

    monkeypatch.setattr(monitor, "is_stream_port_present", inventory_failed)
    clock[0] = 130
    result = subject._try_reconnect()
    assert result["action"] == "port_check_failed"
    assert result["hardware_present"] is None
    assert "USB inventory unavailable" in result["error"]
    assert monitor.probe_calls == 1
