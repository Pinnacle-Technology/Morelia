"""EDF missing-value behavior verified through real file round trips."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pyedflib

from Morelia.Devices import Pod8206, Pod8206HR, Pod8274D, Pod8401HR
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Stream.sink.edf_sink import EDFSink


def make_pod(cls, rate=4):
    pod = MagicMock(spec=cls)
    pod.sample_rate = rate
    pod.channel_labels = ("A", "B", "C", "D")
    return pod


def packet_for(cls, value):
    if cls is Pod8274D:
        return SimpleNamespace(ch5=[value], ch6=[7.0], ch7=[-3.0])
    if cls is Pod8206:
        return SimpleNamespace(ch0=[value], ch1=[7.0], ch2=[-3.0])
    return SimpleNamespace(
        ch0=value, ch1=7.0, ch2=-3.0, ch3=2.0,
        ext0=1.0, ext1=0.0, ttl1=0, ttl2=1, ttl3=0, ttl4=1,
    )


class EDFMissingValuesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="morelia-edf-test-")
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "recording.edf")

    def test_missing_samples_become_zero_without_losing_other_samples(self):
        values = [1.0, np.nan, 2.0, np.inf, -np.inf, 3.0, 0.0, 4.0]
        for cls in (Pod8206, Pod8206HR, Pod8274D, Pod8401HR):
            with self.subTest(device=cls.__name__):
                with EDFSink(self.path, make_pod(cls)) as sink:
                    for value in values:
                        sink.flush(0, packet_for(cls, value))
                with pyedflib.EdfReader(self.path) as reader:
                    np.testing.assert_array_equal(reader.getNSamples(), 8)
                    # EDF's 16-bit scaling introduces up to one quantization step.
                    np.testing.assert_allclose(
                        reader.readSignal(0), [1, 0, 2, 0, 0, 3, 0, 4], atol=0.063,
                    )
                    np.testing.assert_allclose(reader.readSignal(1), 7, atol=0.063)
                    onsets, durations, texts = reader.readAnnotations()
                    np.testing.assert_allclose(onsets, [0.25])
                    self.assertIn("Missing values detected", texts[0])

    def test_annotations_are_throttled_across_records_and_channels(self):
        with EDFSink(self.path, make_pod(Pod8206HR)) as sink:
            for index in range(96):
                packet = packet_for(Pod8206HR, np.nan if index in (1, 2, 40, 41, 81) else 5)
                if index in (1, 3, 42):
                    packet.ch1 = np.nan
                # Arrival timestamps are intentionally useless: annotations must
                # follow written sample positions, including backward host jumps.
                sink.flush(-index * 1_000_000_000, packet)
        with pyedflib.EdfReader(self.path) as reader:
            np.testing.assert_array_equal(reader.getNSamples(), 96)
            onsets, _, texts = reader.readAnnotations()
            np.testing.assert_allclose(onsets, [0.25, 10.25, 20.25])
            self.assertEqual(len(texts), 3)

    def test_large_batch_throttles_annotations_and_keeps_clean_overflow(self):
        values = np.full(96, np.nan)
        values[84:] = 6.0
        packet = SimpleNamespace(ch0=values, ch1=[7.0] * 96, ch2=[-3.0] * 96)
        with EDFSink(self.path, make_pod(Pod8206)) as sink:
            sink.flush(0, packet)
        with pyedflib.EdfReader(self.path) as reader:
            np.testing.assert_array_equal(reader.getNSamples(), 96)
            np.testing.assert_allclose(reader.readSignal(0)[:84], 0, atol=0.063)
            np.testing.assert_allclose(reader.readSignal(0)[84:], 6, atol=0.063)
            np.testing.assert_allclose(reader.readAnnotations()[0], [0, 10, 20])

    def test_clean_zeros_do_not_generate_annotations(self):
        with EDFSink(self.path, make_pod(Pod8206HR)) as sink:
            for _ in range(8):
                sink.flush(0, packet_for(Pod8206HR, 0.0))
        with pyedflib.EdfReader(self.path) as reader:
            self.assertEqual(len(reader.readAnnotations()[0]), 0)
            np.testing.assert_array_equal(reader.getNSamples(), 8)

    def test_shared_layout_preserves_8206hr_nan(self):
        pod = make_pod(Pod8206HR)
        packet = packet_for(Pod8206HR, np.nan)
        for profile in ("analog", "with_digital"):
            layout = resolve_layout(pod, profile=profile)
            self.assertTrue(np.isnan(layout.expand(0, packet)[0][1][0]))


if __name__ == "__main__":
    unittest.main()
