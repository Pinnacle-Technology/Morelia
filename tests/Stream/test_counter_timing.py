"""Deterministic acquisition counter, loss, clock and batch timing checks."""

import math
import unittest
from multiprocessing import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import reactivex as rx

from Morelia.Devices import Pod8206, Pod8206HR, Pod8401HR, Pod8274D
from Morelia.packet.data import DataPacket8206HR, DataPacket8401HR, DataPacket8274D
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Stream.source import _timestamp_8206_batches
from Morelia.Stream.source import get_data
from Morelia.Stream.timing import timestamp_with_counters, MissingDataPacket


def pod(cls, rate=1000):
    device = MagicMock(spec=cls)
    device.sample_rate = rate
    device.channel_labels = ("A", "B", "C", "D")
    return device


def packet(counter, batched=False):
    return SimpleNamespace(packet_counter=counter, ch0=1, ch1=2, ch2=3, ch3=4,
                           ext0=0, ext1=1, ttl1=0, ttl2=1, ttl3=0, ttl4=1,
                           ch5=[1] * 40, ch6=[2] * 40, ch7=[3] * 40)


def replay(device, counters, arrivals=None, wall_jump=0, operator=None, prime_hr=True):
    batched = isinstance(device, Pod8274D)
    rate = device.sample_rate
    if arrivals is None:
        arrivals = [round(i * (40 if batched else 1) * 1e9 / rate) for i in range(len(counters))]
    # Gap tests exercise an already-confirmed HR counter. Probe behavior has
    # separate tests using prime_hr=False.
    prefix = 0
    if isinstance(device, Pod8206HR) and prime_hr:
        prefix = 201
        first = counters[0]
        counters = [(first - prefix + i) % 256 for i in range(prefix)] + list(counters)
        arrivals = [arrivals[0] - round((prefix - i) * 1e9 / rate) for i in range(prefix)] + list(arrivals)
    clock = SimpleNamespace(mono=0, wall=1_800_000_000_000_000_000)
    result, errors = [], []
    def emit(observer, scheduler=None):
        for i, (counter, arrival) in enumerate(zip(counters, arrivals)):
            clock.mono = arrival
            clock.wall = 1_800_000_000_000_000_000 + arrival + (wall_jump if i else 0)
            observer.on_next(packet(counter, batched))
        observer.on_completed()
    with patch("Morelia.Stream.timing.time.monotonic_ns", side_effect=lambda: clock.mono), \
         patch("Morelia.Stream.timing.time.time_ns", side_effect=lambda: clock.wall):
        rx.create(emit).pipe(operator or timestamp_with_counters(device)).subscribe(result.append, errors.append)
    if errors:
        raise errors[0]
    return result[prefix:]


class CounterTimingTests(unittest.TestCase):
    def test_wire_counter_offsets_and_endianness(self):
        for cls, size, offset, encoded, expected in (
            (DataPacket8206HR, 16, 5, b"\xfe", 254),
            (DataPacket8401HR, 31, 5, b"\x81", 129),
            (DataPacket8274D, 259, 12, b"\x34\x12", 0x1234),
        ):
            with self.subTest(device=cls.__name__):
                raw = bytearray(size)
                raw[offset:offset + len(encoded)] = encoded
                value = cls.__new__(cls)
                value._raw_packet, value._min_length = bytes(raw), size
                self.assertEqual(value.packet_counter, expected)
                value._raw_packet = b""
                with self.assertRaises(ValueError):
                    _ = value.packet_counter

    def test_hr_gap_wrap_and_unknown_digital_samples(self):
        device = pod(Pod8206HR)
        out = replay(device, [254, 1], [0, 3_000_000])
        self.assertEqual([p.packet_counter for _, p in out], [254, 255, 0, 1])
        layout = resolve_layout(device, profile="with_digital")
        for _, missing in out[1:3]:
            self.assertIsInstance(missing, MissingDataPacket)
            self.assertTrue(all(math.isnan(v) for _, row in layout.expand(0, missing) for v in row))
        self.assertTrue(all(b[0] - a[0] == 1_000_000 for a, b in zip(out, out[1:])))

    def test_8274_gap_contains_40_samples_per_channel_and_no_overlap(self):
        device = pod(Pod8274D, 1024)
        out = replay(device, [65535, 1], [0, 78_125_000])
        self.assertEqual([p.packet_counter for _, p in out], [65535, 0, 1])
        missing = out[1][1]
        self.assertEqual(missing.sample_count, 40)
        self.assertTrue(all(math.isnan(v) for v in missing.ch5 + missing.ch6 + missing.ch7))
        expanded = [row for ts, value in out for row in resolve_layout(device).expand(ts, value)]
        self.assertEqual(len(expanded), 120)
        self.assertTrue(all(b[0] - a[0] == round(1e9 / 1024) for a, b in zip(expanded, expanded[1:])) )

    def test_8274_adaptation_preserves_batch_boundaries(self):
        device = pod(Pod8274D, 1024)
        out = replay(device, list(range(200)), [round(i * 40e9 / 1010) for i in range(200)])
        self.assertGreater(out[-1][1].sample_period_ns, out[0][1].sample_period_ns)
        layout = resolve_layout(device)
        for (ts, value), (next_ts, _) in zip(out, out[1:]):
            last_ts = layout.expand(ts, value)[-1][0]
            self.assertEqual(next_ts - last_ts, value.sample_period_ns)

    def test_real_wire_packets_flow_through_get_data_with_inserted_gaps(self):
        from tests.mocks.packet.data.MockDataPacket8206HR import MockDataPacket8206HR
        from tests.mocks.packet.data.MockDataPacket8274D import MockDataPacket8274D
        from tests.mocks.packet.data.MockDataPacket8401HR import MockDataPacket8401HR
        from Morelia.packet import PrimaryChannelMode, SecondaryChannelMode
        from Morelia.Stream.sink.buffer_sink import BufferSink
        for cls, packet_cls, mock_cls, kwargs in (
            (Pod8206HR, DataPacket8206HR, MockDataPacket8206HR, {"preamp_gain": 10}),
            (Pod8274D, DataPacket8274D, MockDataPacket8274D, {}),
            (Pod8401HR, DataPacket8401HR, MockDataPacket8401HR,
             {"preamp_gain": (10,) * 4, "ss_gain": (5,) * 4,
              "primary_channel_modes": (PrimaryChannelMode.EEG_EMG,) * 4,
              "secondary_channel_modes": (SecondaryChannelMode.DIGITAL,) * 6}),
        ):
            with self.subTest(device=cls.__name__):
                device = pod(cls)
                device._port = object()
                counters = [0, 2] if cls is Pod8274D else list(range(201)) + [203]
                packets = [packet_cls(raw_packet=mock_cls(packet_number=n).to_bytes(), **kwargs)
                           for n in counters]
                def emit(observer, scheduler=None):
                    for value in packets:
                        observer.on_next(value)
                    observer.on_completed()
                buffer = []
                with patch("Morelia.Stream.source._stream_from_pod_device", return_value=emit), \
                     patch("Morelia.Stream.source.threading.Thread"):
                    get_data(1, Event(), device, [BufferSink(buffer, device, batch_size=1)])
                self.assertEqual(len(buffer), 4 if cls is Pod8274D else 205)
                missing_values = buffer[-2][1]
                if cls is Pod8274D:
                    missing_values = [v for channel in missing_values for v in channel]
                    self.assertEqual(len(missing_values), 120)
                self.assertTrue(all(math.isnan(v) for v in missing_values))

    def test_duplicate_is_not_an_extra_sample(self):
        out = replay(pod(Pod8206HR), [8, 8, 9])
        self.assertEqual([p.packet_counter for _, p in out], [8, 9])

    def test_counter_reset_and_large_jump_resynchronize_without_filling(self):
        with self.assertLogs("Morelia.Stream.timing", level="WARNING"):
            out = replay(pod(Pod8206HR), [90, 2, 3, 100, 101])
        self.assertEqual(len(out), 5)
        self.assertFalse(any(isinstance(p, MissingDataPacket) for _, p in out))
        self.assertTrue(all(b[0] > a[0] for a, b in zip(out, out[1:])))

    def test_8274_large_gap_is_not_silently_clamped(self):
        with self.assertLogs("Morelia.Stream.timing", level="WARNING"):
            out = replay(pod(Pod8274D, 1024), [1, 23])
        self.assertEqual(len(out), 2)

    def test_full_counter_cycle_silence_is_ambiguous(self):
        with self.assertLogs("Morelia.Stream.timing", level="WARNING"):
            out = replay(pod(Pod8206HR), [0, 2], [0, 258_000_000])
        self.assertEqual(len(out), 2)
        self.assertEqual(out[1][0] - out[0][0], 258_000_000)

    def test_8401_requires_successful_probe(self):
        with self.assertLogs("Morelia.Stream.timing", level="INFO"):
            out = replay(pod(Pod8401HR), list(range(201)) + [203],
                         [i * 1_000_000 for i in range(201)] + [203_000_000])
        self.assertEqual([p.packet_counter for _, p in out[-3:]], [201, 202, 203])
        self.assertTrue(all(isinstance(p, MissingDataPacket) for _, p in out[-3:-1]))
        with self.assertLogs("Morelia.Stream.timing", level="WARNING"):
            out = replay(pod(Pod8401HR), [0] * 201 + [3, 3])
        self.assertEqual(len(out), 203)
        self.assertFalse(any(isinstance(p, MissingDataPacket) for _, p in out))

    def test_reported_8206hr_jumps_do_not_create_synthetic_data(self):
        jumps = [240, 112, 240, 129, 96, 113, 239, 113, 240, 112, 240, 113]
        counters = [0]
        for i in range(500):
            counters.append((counters[-1] + jumps[i % len(jumps)] + 1) % 256)
        with self.assertLogs("Morelia.Stream.timing", level="WARNING") as logs:
            out = replay(pod(Pod8206HR), counters, prime_hr=False)
        self.assertEqual(len(out), len(counters))
        self.assertFalse(any(isinstance(p, MissingDataPacket) for _, p in out))
        self.assertEqual(len(logs.output), 1)
        self.assertIn("counter disabled", logs.output[0])
        self.assertTrue(all(p.sample_period_ns == 1_000_000 for _, p in out))

    def test_repeated_bad_jumps_disable_previously_confirmed_counter(self):
        counters = [0]
        for i in range(100):
            counters.append((counters[-1] + (241 if i % 2 else 114)) % 256)
        with self.assertLogs("Morelia.Stream.timing", level="WARNING") as logs:
            out = replay(pod(Pod8206HR), counters)
        self.assertEqual(len(out), len(counters))
        self.assertFalse(any(isinstance(p, MissingDataPacket) for _, p in out))
        self.assertEqual(len(logs.output), 3)
        self.assertIn("counter disabled", logs.output[-1])

    def test_losses_count_toward_measured_rate_and_wall_clock_changes_do_not(self):
        indexes = [i for i in range(7000) if i not in (100, 101, 3050, 6000)]
        out = replay(pod(Pod8206HR), [i % 256 for i in indexes],
                     [i * 1_000_000 for i in indexes], wall_jump=-50_000_000_000)
        self.assertEqual(len(out), 7000)
        self.assertEqual(sum(isinstance(p, MissingDataPacket) for _, p in out), 4)
        self.assertTrue(all(p.sample_period_ns == 1_000_000 for _, p in out))
        self.assertTrue(all(b[0] - a[0] == 1_000_000 for a, b in zip(out, out[1:])))

    def test_measured_rate_can_depart_from_nominal(self):
        out = replay(pod(Pod8206HR), [i % 256 for i in range(7000)],
                     [round(i * 1e9 / 990) for i in range(7000)])
        self.assertGreater(out[-1][1].sample_period_ns, 1_000_000)
        self.assertLess(out[-1][1].sample_period_ns, round(1e9 / 990))

    def test_subscriptions_start_fresh(self):
        device = pod(Pod8206HR)
        operator = timestamp_with_counters(device)
        self.assertEqual(len(replay(device, [200, 201], operator=operator)), 2)
        self.assertEqual(len(replay(device, [0, 1], operator=operator)), 2)

    def test_legacy_8206_remains_fixed_rate_without_counters(self):
        out = []
        rx.from_iterable([SimpleNamespace(sample_count=4), SimpleNamespace(sample_count=8)]).pipe(
            _timestamp_8206_batches(400)).subscribe(out.append)
        self.assertEqual(out[1][0] - out[0][0], 10_000_000)
        with self.assertRaises(ValueError):
            timestamp_with_counters(pod(Pod8206, 400))


if __name__ == "__main__":
    unittest.main()
