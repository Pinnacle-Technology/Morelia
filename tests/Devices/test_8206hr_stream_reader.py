"""Regression checks for short startup ACKs and incomplete serial frames."""

import io
import unittest
from contextlib import redirect_stdout
from multiprocessing import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from Morelia.Devices import Pod8206HR
from Morelia.Devices.SerialPorts.SerialComm import PortIO
from Morelia.packet import ControlPacket
from Morelia.Stream.source import _stream_from_pod_device
from tests.mocks.packet.MockControlPacket import MockControlPacket
from tests.mocks.packet.data.MockDataPacket8206HR import MockDataPacket8206HR


class SerialBytes:
    def __init__(self, data=b""):
        self.data = bytearray(data)
        self.timeout = None
        self.is_open = True
        self.requests = []

    def close(self):
        self.is_open = False

    @property
    def in_waiting(self):
        return len(self.data)

    def read(self, count):
        self.requests.append(count)
        result = bytes(self.data[:count])
        del self.data[:count]
        return result


def device(data=b""):
    serial = SerialBytes(data)
    port = PortIO.__new__(PortIO)
    port._serial_inst = serial
    pod = Pod8206HR.__new__(Pod8206HR)
    pod._port = port
    pod._preamp_gain = 10
    pod._control_packet_factory = lambda raw: ControlPacket(lambda command, payload: (), raw)
    return pod, serial


class HRReaderTests(unittest.TestCase):
    def test_short_startup_ack_does_not_wait_for_sixteen_bytes(self):
        raw = MockControlPacket(6).to_bytes()
        pod, serial = device(raw)
        packet = pod.read_pod_packet_streaming()
        self.assertIsInstance(packet, ControlPacket)
        self.assertEqual(packet.raw_packet, raw)
        self.assertLess(max(serial.requests), 16)

    def test_ack_and_binary_packets_do_not_merge(self):
        ack = MockControlPacket(6).to_bytes()
        # Payload includes framing byte values; these must never split a frame.
        first = MockDataPacket8206HR(packet_number=2, ch0=0x0302).to_bytes()
        second = MockDataPacket8206HR(packet_number=3).to_bytes()
        pod, serial = device(ack + first + second)
        self.assertEqual(pod.read_pod_packet_streaming().raw_packet, ack)
        self.assertEqual(pod.read_pod_packet_streaming().raw_packet, first)
        self.assertEqual(pod.read_pod_packet_streaming().raw_packet, second)
        self.assertEqual(len(serial.requests), 1)

    def test_buffered_stream_keeps_every_counter_and_channel_byte(self):
        frames = [MockDataPacket8206HR(packet_number=i, ch0=i, ch1=0x0302, ch2=0x0203).to_bytes()
                  for i in range(500)]
        pod, serial = device(b"".join(frames))
        for raw in frames:
            self.assertEqual(pod.read_pod_packet_streaming().raw_packet, raw)
        self.assertEqual(len(serial.requests), 2)

    def test_com_wait_timeout_is_stable_for_small_deadline_changes(self):
        pod, serial = device()
        for timeout in (0.1999994, 0.199995, 0.19998):
            self.assertEqual(pod._port.read_available(4096, timeout), b"")
            self.assertEqual(serial.timeout, 0.2)

    def test_serial_fragments_survive_timeout_at_every_position(self):
        raw = MockDataPacket8206HR(packet_number=19).to_bytes()
        for split in range(1, len(raw)):
            with self.subTest(split=split):
                pod, serial = device(raw[:split])
                with self.assertRaises(TimeoutError):
                    pod.read_pod_packet_streaming()
                self.assertEqual(bytes(pod._stream_receive_buffer), raw[:split])
                serial.data.extend(raw[split:])
                self.assertEqual(pod.read_pod_packet_streaming().raw_packet, raw)

    def test_d2xx_style_partial_reads_survive_timeout(self):
        raw = MockDataPacket8206HR(packet_number=20).to_bytes()
        pod, serial = device(raw[:9])
        pod._port = SimpleNamespace(read=lambda count, timeout: serial.read(count))
        with self.assertRaises(TimeoutError):
            pod.read_pod_packet_streaming()
        serial.data.extend(raw[9:])
        self.assertEqual(pod.read_pod_packet_streaming().packet_counter, 20)

    def test_junk_prefix_and_bad_checksum_recover(self):
        good = MockDataPacket8206HR(packet_number=21).to_bytes()
        bad = good[:-3] + b"ZZ\x03"
        pod, _ = device(b"junk" + bad + good)
        with self.assertRaisesRegex(ValueError, "checksum"):
            pod.read_pod_packet_streaming()
        self.assertEqual(pod.read_pod_packet_streaming().packet_counter, 21)

    def test_one_deadline_bounds_partial_reads(self):
        pod, _ = device(b"\x02")
        with patch("Morelia.Devices.PodDevice_8206HR.time.perf_counter", side_effect=[0, 0.1, 0.3]):
            with self.assertRaises(TimeoutError):
                pod.read_pod_packet_streaming(timeout_sec=0.2)
        self.assertEqual(bytes(pod._stream_receive_buffer), b"\x02")

    def test_serial_exact_read_contract_is_unchanged(self):
        pod, serial = device(b"abc")
        with self.assertRaises(TimeoutError):
            pod._port.read(5, 0.2)
        serial.data.extend(b"abc")
        self.assertEqual(pod._port.read_partial(5, 0.2), b"abc")

    def test_startup_wait_is_quiet_but_persistent_timeout_is_reported(self):
        for waits, expected_warning in ((3, False), (12, True)):
            with self.subTest(waits=waits):
                pod = MagicMock(spec=Pod8206HR)
                stop = Event()
                clock = SimpleNamespace(now=0, count=0)
                def read(**kwargs):
                    clock.now += 0.2
                    clock.count += 1
                    if clock.count <= waits:
                        raise TimeoutError("no complete frame")
                    stop.set()
                    return SimpleNamespace(packet_counter=0)
                pod.read_pod_packet_streaming.side_effect = read
                observer = MagicMock()
                output = io.StringIO()
                with patch("Morelia.Stream.source.time.perf_counter", side_effect=lambda: clock.now), redirect_stdout(output):
                    _stream_from_pod_device(pod, 10, stop)(observer, None)
                self.assertEqual("Waiting for first data packet" in output.getvalue(), expected_warning)
                self.assertNotIn("Dropped packet", output.getvalue())
                observer.on_next.assert_called_once()
                observer.on_completed.assert_called_once()

    def test_short_waits_are_quiet_and_sustained_stalls_warn_once(self):
        for events, warnings in (
            (["data", "timeout"] * 15, 0),
            (["data"] + ["timeout"] * 15, 1),
            (["data"] + ["timeout"] * 15 + ["data"] + ["timeout"] * 15, 2),
            (["data"] + ["control", "timeout"] * 15, 1),
        ):
            with self.subTest(events=events):
                pod = MagicMock(spec=Pod8206HR)
                stop = Event()
                clock = SimpleNamespace(now=0)
                queue = iter(events)
                def read(**kwargs):
                    kind = next(queue, "stop")
                    clock.now += 0.2
                    if kind == "stop":
                        stop.set()
                        raise TimeoutError("stopped")
                    if kind == "timeout":
                        raise TimeoutError("no complete frame")
                    return (ControlPacket(lambda *_: (), MockControlPacket(6).to_bytes())
                            if kind == "control" else SimpleNamespace(packet_counter=0))
                pod.read_pod_packet_streaming.side_effect = read
                output = io.StringIO()
                with patch("Morelia.Stream.source.time.perf_counter", side_effect=lambda: clock.now), redirect_stdout(output):
                    _stream_from_pod_device(pod, 100, stop)(MagicMock(), None)
                self.assertEqual(output.getvalue().count("Waiting for streaming data"), warnings)


if __name__ == "__main__":
    unittest.main()
