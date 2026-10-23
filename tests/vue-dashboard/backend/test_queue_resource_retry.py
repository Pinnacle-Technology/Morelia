import json
import multiprocessing as mp
import pickle
import threading
import time
from queue import Empty, Full

import pytest
from app.runtime_child.queue_diagnostics import DiagnosticQueue

winapi = pytest.importorskip("_winapi")


def resource_error(winerror=1450):
    error = OSError("injected pipe failure")
    if winerror is not None:
        error.winerror = winerror
    return error


def wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        threading.Event().wait(0.01)


def close_queue(queue):
    queue.cancel_pending_writes()
    queue.close()
    queue.join_thread()


def test_retry_retains_order_and_bounds_pending_frames(monkeypatch):
    queue = DiagnosticQueue("test", maxsize=10, resource_retry_seconds=3)
    write = winapi.WriteFile
    blocked = threading.Event()
    resume = threading.Event()
    submitted = []

    def reject_until_resumed(handle, data, **kwargs):
        if handle == queue._writer.fileno():
            if not resume.is_set():
                blocked.set()
                raise resource_error()
            submitted.append(pickle.loads(data))
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject_until_resumed)
    try:
        for frame in range(10):
            queue.put_nowait(frame)
        assert blocked.wait(2)
        with pytest.raises(Full):
            queue.put_nowait(10)
        assert submitted == []
        resume.set()
        assert [queue.get(timeout=5) for _ in range(10)] == list(range(10))
        assert submitted == list(range(10))
        assert queue.transport_error is None
    finally:
        close_queue(queue)


@pytest.mark.parametrize("codes", [(1450,), (109,), (None,), (1450, 8, 5)])
def test_retry_deadline_stops_later_frames(monkeypatch, codes):
    queue = DiagnosticQueue("test", maxsize=10, resource_retry_seconds=0.15)
    write = winapi.WriteFile
    attempts = []

    def reject(handle, data, **kwargs):
        if handle == queue._writer.fileno():
            attempts.append(pickle.loads(data))
            raise resource_error(codes[(len(attempts) - 1) % len(codes)])
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject)
    try:
        queue.put(1)
        queue.put(2)
        wait_for(lambda: queue.transport_error is not None)
        assert "recovery deadline expired" in queue.transport_error
        assert len(attempts) >= 2
        assert set(attempts) == {1}
        with pytest.raises(Empty):
            queue.get(timeout=0.1)
    finally:
        close_queue(queue)


@pytest.mark.parametrize("winerror", [1450, 8, 1816, 109, 5, 99999, None])
def test_submission_errors_retry_without_an_error_code_allowlist(monkeypatch, winerror, capfd):
    queue = DiagnosticQueue("test", resource_retry_seconds=3)
    write = winapi.WriteFile
    attempts = []

    def reject(handle, data, **kwargs):
        if handle == queue._writer.fileno():
            attempts.append(pickle.loads(data))
            if len(attempts) == 1:
                raise resource_error(winerror)
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject)
    try:
        queue.put(1)
        queue.put(2)
        assert [queue.get(timeout=5) for _ in range(2)] == [1, 2]
        assert attempts == [1, 1, 2]
        assert queue.transport_error is None
    finally:
        close_queue(queue)
    events = [json.loads(line) for line in capfd.readouterr().err.splitlines()
              if line.startswith('{"')]
    started = next(event for event in events if event["event"] == "ipc_queue_resource_retry_started")
    assert started["winerror"] == winerror
    assert started["error_message"] == "injected pipe failure"


@pytest.mark.parametrize("error_type", [TypeError, ValueError])
def test_programming_errors_do_not_enter_transport_recovery(monkeypatch, error_type):
    queue = DiagnosticQueue("test", resource_retry_seconds=3)
    write = winapi.WriteFile
    attempts = []

    def reject(handle, data, **kwargs):
        if handle == queue._writer.fileno():
            attempts.append(pickle.loads(data))
            raise error_type("invalid write arguments")
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject)
    try:
        queue.put(1)
        queue.put(2)
        wait_for(lambda: queue.transport_error is not None)
        assert attempts == [1]
        assert "invalid write arguments" in queue.transport_error
    finally:
        close_queue(queue)


@pytest.mark.parametrize("winerror", [1450, 5, None])
def test_accepted_write_is_never_replayed_after_completion_error(monkeypatch, winerror):
    queue = DiagnosticQueue("test", resource_retry_seconds=3)
    write = winapi.WriteFile
    submitted = []

    class FailedCompletion:
        def __init__(self, operation):
            self.operation = operation
            self.event = operation.event

        def cancel(self):
            self.operation.cancel()

        def GetOverlappedResult(self, wait):
            self.operation.GetOverlappedResult(wait)
            raise resource_error(winerror)

    def accept_then_fail_completion(handle, data, **kwargs):
        operation, error = write(handle, data, **kwargs)
        if handle == queue._writer.fileno():
            submitted.append(pickle.loads(data))
            return FailedCompletion(operation), error
        return operation, error

    monkeypatch.setattr(winapi, "WriteFile", accept_then_fail_completion)
    try:
        queue.put(1)
        queue.put(2)
        assert queue.get(timeout=5) == 1
        wait_for(lambda: queue.transport_error is not None)
        with pytest.raises(Empty):
            queue.get(timeout=0.2)
        assert submitted == [1]
    finally:
        close_queue(queue)


def test_cancel_interrupts_resource_wait(monkeypatch):
    queue = DiagnosticQueue("test", resource_retry_seconds=35)
    write = winapi.WriteFile
    blocked = threading.Event()

    def reject(handle, data, **kwargs):
        if handle == queue._writer.fileno():
            blocked.set()
            raise resource_error()
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject)
    queue.put(1)
    assert blocked.wait(2)
    started = time.monotonic()
    close_queue(queue)
    assert time.monotonic() - started < 1
    assert not queue._thread.is_alive()


def _spawn_retry_sender(queue):
    write = winapi.WriteFile
    failed = False

    def reject_once(handle, data, **kwargs):
        nonlocal failed
        if handle == queue._writer.fileno() and not failed:
            failed = True
            raise resource_error()
        return write(handle, data, **kwargs)

    winapi.WriteFile = reject_once
    try:
        queue.put(1)
        queue.put(2)
        queue.close()
        queue.join_thread()
    finally:
        winapi.WriteFile = write


def test_retry_configuration_survives_spawn():
    context = mp.get_context("spawn")
    queue = DiagnosticQueue("test", ctx=context, maxsize=10, resource_retry_seconds=3)
    child = context.Process(target=_spawn_retry_sender, args=(queue,))
    try:
        child.start()
        assert [queue.get(timeout=10) for _ in range(2)] == [1, 2]
        child.join(timeout=5)
        assert child.exitcode == 0
    finally:
        if child.is_alive():
            child.terminate()
            child.join(timeout=5)
        close_queue(queue)


def test_without_retry_the_same_pipe_fault_loses_the_frame(monkeypatch):
    queue = DiagnosticQueue("test")
    write = winapi.WriteFile

    def reject_first_frame(handle, data, **kwargs):
        if handle == queue._writer.fileno() and pickle.loads(data) == 1:
            raise resource_error()
        return write(handle, data, **kwargs)

    monkeypatch.setattr(winapi, "WriteFile", reject_first_frame)
    try:
        queue.put(1)
        queue.put(2)
        assert queue.get(timeout=5) == 2
        with pytest.raises(Empty):
            queue.get(timeout=0.1)
    finally:
        close_queue(queue)


@pytest.mark.parametrize("failure", ["overflow", "deadline", "stop"])
def test_sink_reports_transport_failure_and_shutdown_stays_bounded(
    app, tmp_path, monkeypatch, failure,
):
    from app.output import managed_pvfs_sink as pvfs

    monkeypatch.setattr(pvfs, "_WRITER_BUFFER_SECONDS", 0.1)
    if failure == "deadline":
        monkeypatch.setattr(pvfs, "_RESOURCE_RETRY_SECONDS", 0.15)
    write = winapi.WriteFile
    blocked = threading.Event()
    with app.app_context():
        sink = pvfs.ManagedPvfsSink(
            path=tmp_path / "failed.pvfs", dataflow_id="RUN", channels=["A"],
            sample_rate=10, use_writer_process=True,
        )
        sink.open()
        queue = sink._writer_queue
        writer_handle = queue._writer.fileno()

        def reject(handle, data, **kwargs):
            if handle == writer_handle:
                blocked.set()
                raise resource_error()
            return write(handle, data, **kwargs)

        monkeypatch.setattr(winapi, "WriteFile", reject)
        monkeypatch.setattr(pvfs, "_WRITER_JOIN_TIMEOUT", 1.0)
        try:
            sink.write_frame([1.0])
            assert blocked.wait(2)
            if failure == "overflow":
                with pytest.raises(pvfs.ManagedPvfsSinkError, match="buffer is full"):
                    sink.write_frame([2.0])
                assert sink._accepted_sequence == 1
            elif failure == "deadline":
                wait_for(lambda: queue.transport_error is not None)
                started = time.monotonic()
                with pytest.raises(pvfs.ManagedPvfsSinkError, match="recovery deadline expired"):
                    sink.flush()
                assert time.monotonic() - started < 1
        finally:
            started = time.monotonic()
            with pytest.raises(pvfs.ManagedPvfsSinkError):
                sink.close()
            assert time.monotonic() - started < 3
        wait_for(lambda: not queue._thread.is_alive())
        assert not sink.writer_alive
        assert sink.record.acquisition_state == "interrupted"


def test_real_pvfs_preserves_samples_through_30_second_outage(app, tmp_path, monkeypatch):
    from app.output.managed_pvfs_sink import ManagedPvfsSink
    from app.output.pvfs_merger import _read_channel
    from pvfs_tools.Core.pvfs_data_file import PvfsDataFile

    path = tmp_path / "retained.pvfs"
    write = winapi.WriteFile
    failed_at = None
    rejections = 0
    submitted = []
    total = 12004
    with app.app_context():
        sink = ManagedPvfsSink(path=path, dataflow_id="RUN", channels=["A", "B"],
                               sample_rate=400, use_writer_process=True)
        sink.open()
        writer_handle = sink._writer_queue._writer.fileno()
        assert sink._writer_queue._maxsize == 14000

        def outage(handle, data, **kwargs):
            nonlocal failed_at, rejections
            if handle == writer_handle:
                command = pickle.loads(data)
                if command.get("sequence") == 6:
                    if failed_at is None:
                        failed_at = time.monotonic()
                    if time.monotonic() - failed_at < 30:
                        rejections += 1
                        raise resource_error((1450, 8, 1816, None)[rejections % 4])
                if command.get("kind") == "frame":
                    submitted.append(command["sequence"])
            return write(handle, data, **kwargs)

        monkeypatch.setattr(winapi, "WriteFile", outage)
        try:
            for value in range(1, total + 1):
                sink.write_frame([float(value), float(-value)])
            sink.flush()
            assert sink.record.row_offset == total
        finally:
            sink.close()
        assert failed_at is not None
        assert time.monotonic() - failed_at >= 30
        assert submitted == list(range(1, total + 1))
        assert sink.record.acquisition_state == "complete"
        assert not sink.forced_termination

    reader = PvfsDataFile()
    try:
        assert reader.open(str(path))
        for channel, sign in (("A", 1), ("B", -1)):
            _, values = _read_channel(reader.open_channel(channel))
            assert list(values) == [float(sign * value) for value in range(1, total + 1)]
    finally:
        reader.close()
