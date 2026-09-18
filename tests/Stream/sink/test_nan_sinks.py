"""Missing-data serialization and plot range regression tests (no servers/GUI)."""

import math
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import MagicMock

from Morelia.Devices import Pod8206HR, Pod8274D
from Morelia.Stream.sink.influx_sink import InfluxSink
from Morelia.Stream.sink.quest_sink import QuestSink
from Morelia.Stream.sink.plot_sink import PlotDisplay


class MissingDataSinkTests(unittest.TestCase):
    def test_database_lines_preserve_missing_timestamps_and_good_channels(self):
        for sink_class in (InfluxSink, QuestSink):
            for device in (Pod8206HR, Pod8274D):
                with self.subTest(sink=sink_class.__name__, device=device.__name__):
                    pod = MagicMock(spec=device)
                    pod.sample_rate = 2
                    pod.SAMPLES_PER_PACKET = 1
                    pod.device_name = "test"
                    pod.port_inst = object()
                    sink = sink_class(pod=pod, measurement="samples")
                    output = []
                    sink._data.subscribe(output.append)
                    if device is Pod8206HR:
                        packet = SimpleNamespace(ch0=math.nan, ch1=math.inf, ch2=-math.inf,
                                                 ttl1=0, ttl2=1, ttl3=0, ttl4=1)
                        sink._subject.on_next((100, packet))
                        packet.ch0, packet.ch1, packet.ch2 = 0, 2, 3
                        sink._subject.on_next((200, packet))
                        first_tag = "EEG1"
                    else:
                        packet = SimpleNamespace(ch5=[math.nan, 0], ch6=[math.inf, 2], ch7=[-math.inf, 3])
                        sink._subject.on_next((100, packet))
                        first_tag = "Ch5"
                    lines = b"\n".join(output).decode().splitlines()
                    self.assertEqual(sum("missing=true" in line for line in lines), 3)
                    self.assertIn(f"samples,channel={first_tag},name=test missing=true 100", lines)
                    self.assertTrue(any("value=0.0,missing=false" in line for line in lines))
                    self.assertFalse(any("value=nan" in line or "value=inf" in line or "value=-inf" in line for line in lines))
                    self.assertTrue(all("value=" not in line for line in lines if "missing=true" in line))

    def make_display(self, values, current_range=None):
        display = PlotDisplay.__new__(PlotDisplay)
        display._glw = object()
        display._order = ["test"]
        display._channels = {"test": SimpleNamespace(buffers=[deque(enumerate(values))])}
        display._curves = [MagicMock()]
        display._plots = [MagicMock()]
        display._y_ranges = [current_range]
        return display

    def test_plot_leading_nan_and_infinities_do_not_poison_range(self):
        display = self.make_display([math.nan, -2, math.inf, 3, -math.inf])
        display._redraw()
        low, high = display._plots[0].setYRange.call_args.args
        self.assertTrue(math.isfinite(low) and math.isfinite(high))
        self.assertLess(low, -2)
        self.assertGreater(high, 3)
        self.assertEqual(display._curves[0].setData.call_args.kwargs["connect"], "finite")
        self.assertTrue(math.isnan(display._curves[0].setData.call_args.args[1][0]))

    def test_all_missing_plot_clears_curve_and_keeps_range_then_recovers(self):
        for previous in (None, (-5, 5)):
            with self.subTest(previous=previous):
                display = self.make_display([math.nan, math.inf], previous)
                display._redraw()
                display._curves[0].setData.assert_called_once()
                display._plots[0].setYRange.assert_not_called()
                self.assertEqual(display._y_ranges[0], previous)
                display._channels["test"].buffers[0] = deque([(0, -20), (1, 20)])
                display._redraw()
                low, high = display._plots[0].setYRange.call_args.args
                self.assertTrue(math.isfinite(low) and math.isfinite(high))


if __name__ == "__main__":
    unittest.main()
