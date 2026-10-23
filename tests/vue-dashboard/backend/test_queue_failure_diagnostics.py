import gc
import json
import multiprocessing as mp
import pickle
import threading
import time
import weakref
from queue import Empty

import pytest
from app.runtime_child.queue_diagnostics import DiagnosticQueue, label_queue


def records(captured):
    return [json.loads(line) for line in captured.splitlines() if line.startswith('{"')]


def test_failed_pipe_write_is_labeled_and_next_message_still_delivers(capfd):
    queue = DiagnosticQueue("pvfs_samples", ctx=mp.get_context("spawn"), device_id="DEVICE")
    original = queue._writer.send_bytes
    calls = 0

    def fail_once(data):
        nonlocal calls
        calls += 1
        if calls == 1:
            error = OSError("injected resource failure")
            error.winerror = 1450
            raise error
        original(data)

    queue._writer.send_bytes = fail_once
    queue._reset()
    label_queue(queue, sink_id="DEVICE:pvfs")
    try:
        queue.put({"frame_sequence": 41, "values": [123456789.25]})
        queue.put({"frame_sequence": 42, "values": [2.0]})
        assert queue.get(timeout=5) == {"frame_sequence": 42, "values": [2.0]}
        queue.close()
        queue.join_thread()
        stderr = capfd.readouterr().err
        event, = records(stderr)
        assert event["event"] == "ipc_queue_feeder_error"
        assert event["queue_purpose"] == "pvfs_samples"
        assert event["device_id"] == "DEVICE"
        assert event["sink_id"] == "DEVICE:pvfs"
        assert event["frame_sequence"] == 41
        assert event["send_attempt"] == 1
        assert event["stage"] == "pipe_write"
        assert event["winerror"] == 1450
        assert event["serialized_bytes"] > 0
        assert "123456789.25" not in stderr
        assert "Traceback" in stderr
    finally:
        queue.close()
        queue.join_thread()


def test_serialization_failure_has_no_misleading_send_number(capfd):
    queue = DiagnosticQueue("source_status")
    try:
        queue.put(threading.Lock())
        queue.put({"source_id": "DEVICE"})
        assert queue.get(timeout=5) == {"source_id": "DEVICE"}
        queue.close()
        queue.join_thread()
        event, = records(capfd.readouterr().err)
        assert event["stage"] == "serialization"
        assert event["send_attempt"] is None
    finally:
        queue.close()
        queue.join_thread()


def test_normal_messages_match_standard_queue_and_emit_nothing(capfd):
    payloads = [None, {"values": [1, 2], "timestamp": None}, ("stop", 7)]
    for factory in (mp.Queue, lambda: DiagnosticQueue("test")):
        queue = factory()
        try:
            for value in payloads:
                queue.put(value)
            assert [queue.get(timeout=5) for _ in payloads] == payloads
            with pytest.raises(Empty):
                queue.get_nowait()
        finally:
            queue.close()
            queue.join_thread()
    assert not records(capfd.readouterr().err)


def test_feeder_does_not_keep_queue_alive():
    queue = DiagnosticQueue("test")
    queue.put("test")
    assert queue.get(timeout=5) == "test"
    feeder = queue._thread
    reference = weakref.ref(queue)
    del queue
    gc.collect()
    feeder.join(timeout=5)
    assert reference() is None
    assert not feeder.is_alive()


def test_diagnostic_write_failure_does_not_kill_feeder(monkeypatch):
    import app.runtime_child.queue_diagnostics as diagnostics

    queue = DiagnosticQueue("test")
    monkeypatch.setattr(diagnostics.os, "write", lambda *args: (_ for _ in ()).throw(OSError()))
    try:
        queue.put(threading.Lock())
        queue.put("still delivered")
        assert queue.get(timeout=5) == "still delivered"
    finally:
        queue.close()
        queue.join_thread()


def test_message_metadata_limits_and_excludes_values():
    from app.runtime_child.queue_diagnostics import _message_fields

    assert _message_fields(b"x" * 65537) == {"message_metadata": "omitted_large_payload"}
    assert _message_fields(pickle.dumps({"values": [123], "frame_sequence": 9})) == {
        "message_type": "dict", "frame_sequence": 9,
    }


def _spawn_sender(queue):
    queue.put(threading.Lock())
    queue.put({"source_id": "CHILD"})
    queue.close()
    queue.join_thread()


def test_queue_identity_survives_spawn(capfd):
    context = mp.get_context("spawn")
    queue = DiagnosticQueue("source_status", ctx=context, dataflow_id="RUN")
    queue_id = queue._diagnostic_identity["queue_id"]
    child = context.Process(target=_spawn_sender, args=(queue,))
    try:
        child.start()
        assert queue.get(timeout=15) == {"source_id": "CHILD"}
        child.join(timeout=15)
        assert child.exitcode == 0
        event, = records(capfd.readouterr().err)
        assert event["queue_id"] == queue_id
        assert event["dataflow_id"] == "RUN"
        assert event["producer_pid"] == child.pid
    finally:
        if child.is_alive():
            child.terminate()
            child.join(timeout=5)
        queue.close()
        queue.join_thread()


def test_real_pvfs_writer_rejects_sequence_gap_and_preserves_confirmed_samples(app, tmp_path, capfd):
    from app.output.managed_pvfs_sink import ManagedPvfsSink, ManagedPvfsSinkError
    from app.output.pvfs_merger import _read_channel
    from pvfs_tools.Core.pvfs_data_file import PvfsDataFile

    path = tmp_path / "injected-write.pvfs"
    with app.app_context():
        sink = ManagedPvfsSink(path=path, dataflow_id="RUN", channels=["EEG1"],
                               sample_rate=4, use_writer_process=True,
                               device_id="DEVICE", sink_id="DEVICE:pvfs")
        sink.open()
        queue = sink._writer_queue
        send = queue._writer.send_bytes
        # Keep the original drop path as a negative control for resource retries.
        queue._resource_retry_seconds = 0

        def fail_sixth_frame(data):
            if pickle.loads(data).get("sequence") == 6:
                error = OSError("injected resource failure")
                error.winerror = 1450
                raise error
            return send(data)

        queue._writer.send_bytes = fail_sixth_frame
        queue._reset()
        try:
            for value in range(1, 5):
                sink.write_frame([float(value)])
            sink.flush()
            assert sink.record.row_offset == 4
            with pytest.raises(ManagedPvfsSinkError, match="sequence"):
                for value in range(5, 8):
                    sink.write_frame([float(value)])
                sink.flush()
        finally:
            with pytest.raises(ManagedPvfsSinkError):
                sink.close()
        assert sink.record.acquisition_state == "interrupted"
        assert sink.record.row_offset == 4

    events = records(capfd.readouterr().err)
    failure, = [r for r in events if r["event"] == "ipc_queue_feeder_error"]
    gap, = [r for r in events if r["event"] == "pvfs_writer_sequence_gap"]
    summary, = [r for r in events if r["event"] == "pvfs_queue_stop_summary"]
    assert failure["sequence"] == 6
    assert failure["device_id"] == "DEVICE"
    assert gap["expected_sequence"] == 6
    assert gap["received_sequence"] == 7
    assert summary["samples_written"] == 4
    assert summary["forced_termination"] is False
    reader = PvfsDataFile()
    try:
        assert reader.open(str(path))
        _, values = _read_channel(reader.open_channel("EEG1"))
        assert values == pytest.approx([1.0, 2.0, 3.0, 4.0])
    finally:
        reader.close()
