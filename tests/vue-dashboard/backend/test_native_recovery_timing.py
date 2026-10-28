import importlib
from contextlib import nullcontext
from types import SimpleNamespace
import threading

import pytest

from Morelia.Watchdog.watchdog import StreamWatcher, Watchdog
from app.runtime_child.driver import RuntimePhase
from app.runtime_child.morelia import MoreliaRuntime, WorkerStartupError
from .test_recovery_shutdown_timeout import Monitor


@pytest.fixture
def clock(monkeypatch):
    now = [100.0]
    module = importlib.import_module("Morelia.Watchdog.watchdog")
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    return now


def make_watcher(monkeypatch, clock, *, native=True, first_packet_timeout=10):
    monitor = Monitor({"ok": True, "teardown_confirmed": True})
    monitor.worker_status = "alive"
    monkeypatch.setattr(monitor, "get_stream_heartbeat", lambda *_args: {
        "state": "missing", "reason": "no_data_seen_yet", "packet_count": 0,
    })

    def ping(**kwargs):
        clock[0] += kwargs["timeout_sec"]
        return {"ok": False}

    monkeypatch.setattr(monitor, "poll_stream_queue_roundtrip", ping)
    subject = StreamWatcher(
        0, monitor, failure_threshold=3, max_heartbeat_age_sec=10,
        first_packet_timeout_sec=first_packet_timeout,
        interval_sec=1, timeout_sec=5, recovery_policy="automate",
        max_auto_restart_attempts=1, native_transport=native, publish=lambda *_args: None,
    )
    return subject, monitor


@pytest.mark.parametrize("native,expected", [(True, "waiting_for_heartbeat"), (False, "needs_action")])
def test_native_worker_survives_the_previous_verification_cutoff(monkeypatch, clock, native, expected):
    subject, monitor = make_watcher(monkeypatch, clock, native=native)
    assert subject._try_reconnect()["action"] == "waiting_for_heartbeat"
    for timestamp in (106, 107):
        clock[0] = timestamp
        result = subject._try_reconnect()
    assert result["action"] == expected
    assert monitor.stop_calls == (0 if native else 1)


@pytest.mark.parametrize("fresh", [False, True])
def test_native_recovery_accepts_late_data_but_has_a_deadline(monkeypatch, clock, fresh):
    subject, monitor = make_watcher(monkeypatch, clock)
    subject._try_reconnect()
    clock[0] = 129.9
    assert subject._try_reconnect()["action"] == "waiting_for_heartbeat"
    assert monitor.stop_calls == 0
    if fresh:
        monkeypatch.setattr(monitor, "get_stream_heartbeat", lambda *_args: {"state": "fresh", "packet_count": 1})
    clock[0] = 130
    result = subject._try_reconnect()
    assert result["action"] == ("connection_restored" if fresh else "needs_action")
    assert monitor.stop_calls == (0 if fresh else 1)
    assert monitor.restart_calls == 1


def test_native_first_packet_and_port_release_allow_thirty_seconds(monkeypatch, clock):
    subject, monitor = make_watcher(monkeypatch, clock)
    clock[0] = 125
    result = subject._watch_stream()
    assert result["failure_reason"] == "first_packet_pending"
    assert result["startup"]["remaining_sec"] == 5
    monitor.openable = False
    assert subject._try_reconnect()["action"] == "waiting_for_port_release"
    clock[0] = 154.9
    assert subject._try_reconnect()["action"] == "waiting_for_port_release"
    clock[0] = 155
    result = subject._try_reconnect()
    assert result["initiating_failure_reason"] == "port_release_timeout"
    assert "30 seconds" in result["error"]
    assert monitor.restart_calls == 0


def test_native_recovery_preserves_a_longer_configured_startup_window(monkeypatch, clock):
    subject, monitor = make_watcher(monkeypatch, clock, first_packet_timeout=45)
    subject._try_reconnect()
    clock[0] = 144.9
    assert subject._try_reconnect()["action"] == "waiting_for_heartbeat"
    assert monitor.stop_calls == 0
    clock[0] = 145
    assert subject._try_reconnect()["action"] == "needs_action"


def test_watchdog_assigns_the_extra_time_only_to_native_streams(monkeypatch):
    subject = Watchdog.__new__(Watchdog)
    subject.dataflow_monitor = SimpleNamespace(
        get_all_streams_index=lambda: [0, 1],
        snapshot_config=[{"source": {"source_dict": {"use_d2xx": native}}} for native in (True, False)],
    )
    subject._stop_event = threading.Event()
    subject._results_lock = threading.Lock()
    subject._stream_results = {}
    subject._stream_watchers = {}
    subject._standalone_devices = []
    subject.failure_threshold = 3
    subject.max_heartbeat_age_sec = subject.first_packet_timeout_sec = 10
    subject.recovery_policy = "automate"
    subject.max_auto_restart_attempts = 3
    monkeypatch.setattr(StreamWatcher, "start", lambda self: None)
    subject.spawn_threads(stream_interval=1, device_interval=1, timeout_sec=5,
                          on_stream_result=None, on_device_result=None)
    assert subject._stream_watchers[0]._first_packet_timeout_sec == 30
    assert subject._stream_watchers[1]._first_packet_timeout_sec == 10


@pytest.mark.parametrize("transport", ["d2xx", "vcp", "legacy_d2xx"])
def test_session_wait_allows_native_data_arriving_after_twenty_five_seconds(monkeypatch, clock, transport):
    flow = SimpleNamespace(device_id="pod", port="test", connection=None if transport == "legacy_d2xx" else {"transport": transport},
                           parameters={"use_d2xx": transport == "legacy_d2xx"})
    runtime = MoreliaRuntime(manifest=SimpleNamespace(device_flows=[flow], dataflow_id="timing"),
                             on_report=lambda *_args: None)
    runtime._phase = RuntimePhase.PREFLIGHT
    runtime._flowgraph = SimpleNamespace(collect=lambda: None)
    runtime._watchdog = object()
    monkeypatch.setattr(runtime, "_hardware_boundary", lambda _step: nullcontext())
    monkeypatch.setattr(runtime, "close", lambda: None)
    monkeypatch.setattr(threading.Thread, "start", lambda self: None)

    class FirstPacket:
        def clear(self):
            pass

        def wait(self, timeout):
            clock[0] += 1
            return self.is_set()

        def is_set(self):
            return clock[0] >= 125

    runtime._startup_event = FirstPacket()
    if transport == "vcp":
        with pytest.raises(WorkerStartupError, match="StartupTimeout"):
            runtime.start()
    else:
        runtime.start()
        assert runtime._startup_failure is None
