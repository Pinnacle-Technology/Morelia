"""Replay one omitted wire packet per 256 through COM parsing and PVFS."""

import io
import math
import multiprocessing as mp
import tempfile
import unittest
from collections import deque
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import reactivex as rx

from Morelia.Devices import Pod8206HR
from Morelia.Devices.SerialPorts.SerialComm import PortIO
from Morelia.packet.data import DataPacket8206HR
from Morelia.Stream.source import _stream_from_pod_device
from Morelia.Stream.timing import timestamp_with_counters
from Morelia.Stream.sink.pvfs_sink import PvfsSink, _PVFS_AVAILABLE
from tests.mocks.packet.data.MockDataPacket8206HR import MockDataPacket8206HR


class ScheduledSerial:
    """Serial behavior with a simulated clock, complete packets and USB pauses."""
    def __init__(self, clock, events):
        self.clock = clock
        self.events = deque(events)
        self.buffer = bytearray()
        self.timeout = 0.2
        self.is_open = True

    def close(self):
        self.is_open = False

    @property
    def in_waiting(self):
        while self.events and self.events[0][0] <= self.clock.now:
            _, raw = self.events.popleft()
            self.buffer.extend(raw)
        return len(self.buffer)

    def read(self, count):
        if self.in_waiting < count:
            deadline = self.clock.now + self.timeout
            self.clock.now = min(deadline, self.events[0][0]) if self.events else deadline
            _ = self.in_waiting
        result = bytes(self.buffer[:count])
        del self.buffer[:count]
        return result


def replay_to_pvfs(path, results):
    # Native PVFS handles are confined to the child process, including readback.
    try:
        from pvfs_tools.Core.pvfs_data_file import PvfsDataFile
        clock = SimpleNamespace(now=0.0)
        events = []
        for i in range(1025):
            if i % 256 == 255:
                continue
            pause = (0.21 if i >= 300 else 0) + (0.21 if i >= 700 else 0)
            raw = MockDataPacket8206HR(packet_number=i, ch0=32767, ch1=32768, ch2=32000).to_bytes()
            events.append((i / 1000 + pause, raw))
        pod = Pod8206HR.__new__(Pod8206HR)
        pod._sample_rate = (1000,)
        pod._preamp_gain = 10
        pod._port = PortIO.__new__(PortIO)
        pod._port._serial_inst = ScheduledSerial(clock, events)
        received, errors = [], []
        output = io.StringIO()
        with PvfsSink(str(path), pod) as sink:
            def accept(item):
                timestamp, packet = item
                received.append(packet.ch0)
                sink.flush(timestamp, packet)
            with patch.object(Pod8206HR, "__enter__", lambda self: self), \
                 patch.object(Pod8206HR, "__exit__", return_value=False), \
                 patch.object(Pod8206HR, "close_port"), \
                 patch("Morelia.Stream.source.time.perf_counter", side_effect=lambda: clock.now), \
                 patch("Morelia.Stream.timing.time.monotonic_ns", side_effect=lambda: round(clock.now * 1e9)), \
                 redirect_stdout(output):
                rx.create(_stream_from_pod_device(pod, events[-1][0] + 0.001, mp.Event())).pipe(
                    timestamp_with_counters(pod)).subscribe(accept, errors.append)
        if errors:
            raise errors[0]
        saved = PvfsDataFile()
        if not saved.open(str(path)):
            raise RuntimeError("Cannot reopen PVFS")
        channels = []
        reference = DataPacket8206HR(events[0][1], 10)
        expected_values = (reference.ch0, reference.ch1, reference.ch2)
        try:
            for channel, expected in zip(saved._indexed_data_files.values(), expected_values):
                _, samples = channel.get_data(channel.get_start_time(), channel.get_end_time())
                channels.append((len(samples), [i for i, value in enumerate(samples) if math.isnan(value)],
                                 all(math.isclose(value, expected, rel_tol=1e-6, abs_tol=1e-6)
                                     for value in samples if not math.isnan(value))))
        finally:
            saved.close()
        results.put((len(received), [i for i, value in enumerate(received) if math.isnan(value)], channels, output.getvalue()))
    except Exception:
        import traceback
        results.put(traceback.format_exc())


class SkippedSampleTests(unittest.TestCase):
    @unittest.skipUnless(_PVFS_AVAILABLE, "Native PVFS unavailable")
    def test_one_missing_sample_per_256_survives_com_reader_and_pvfs(self):
        with tempfile.TemporaryDirectory(prefix="morelia-skip-replay-") as directory:
            results = mp.Queue()
            process = mp.Process(target=replay_to_pvfs, args=(str(Path(directory) / "replay.pvfs"), results))
            process.start()
            try:
                result = results.get(timeout=30)
                process.join(timeout=10)
                self.assertEqual(process.exitcode, 0)
                self.assertNotIsInstance(result, str, result)
                count, missing, channels, output = result
                self.assertEqual(count, 1025)
                self.assertEqual(missing, [255, 511, 767, 1023])
                self.assertEqual(len(channels), 3)
                for saved_count, saved_missing, values_match in channels:
                    self.assertEqual(saved_count, 1025)
                    self.assertEqual(saved_missing, missing)
                    self.assertTrue(values_match)
                self.assertNotIn("Waiting for", output)
                self.assertNotIn("Dropped packet", output)
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join()
                process.close()
                results.close()


if __name__ == "__main__":
    unittest.main()
