"""Live data display sink: stream time series to a real-time EEG-style plot.

Accepts any number of input streams (one sink per device in the DataFlow).
Data is sent via a shared multiprocessing.Queue to a PlotDisplay running in the
main process. Plotting is rate-limited and uses bounded buffers so that up to
~10,000 samples per second per channel can be handled without overwhelming the UI.

Multiple sample rates are supported; each channel is displayed with its own
time base in traditional EEG layout (stacked traces, time on X).
"""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

import math
import multiprocessing as mp
from collections import deque
from typing import Any

from Morelia.Stream.sink import SinkInterface
from Morelia.packet.data import DataPacket
from Morelia.Devices import Pod8206, Pod8206HR, Pod8401HR, Pod8274D, AcquisitionDevice


# Control and data message types for the plot queue
_PLOT_REGISTER = "register"
_PLOT_DATA = "data"
_PLOT_UNREGISTER = "unregister"

# Chunk size for sending samples (reduces queue traffic)
_DEFAULT_CHUNK_SAMPLES = 50
# Max samples per channel to retain (e.g. 10 s at 10k Hz)
_MAX_SAMPLES_PER_CHANNEL = 100_000
# Display downsampling: max points per trace for redraw
_DISPLAY_MAX_POINTS = 3000
# Y-axis hysteresis: padding fraction and minimum (for flat signals)
_Y_PAD_FRAC = 0.1
_Y_PAD_MIN = 1.0
# Shrink when data span is less than this fraction of current display range
_Y_SHRINK_RATIO = 0.5
# Initial samples to discard per stream (avoids bad first readings skewing scale)
_SKIP_INITIAL_SAMPLES = 10
# Max effective sample rate sent to the plot (decimate above this to avoid lag)
_DEFAULT_MAX_DISPLAY_RATE = 2000


def _channel_values_8206(packet: DataPacket) -> list[tuple[float, ...]]:
    ch0 = float(packet.ch0) if not (math.isnan(packet.ch0) or math.isinf(packet.ch0)) else 0.0
    ch1 = float(packet.ch1) if not (math.isnan(packet.ch1) or math.isinf(packet.ch1)) else 0.0
    ch2 = float(packet.ch2) if not (math.isnan(packet.ch2) or math.isinf(packet.ch2)) else 0.0
    return [ (ch0, ch1, ch2) ]


def _channel_values_8401(packet: DataPacket) -> list[tuple[float, ...]]:
    ch0 = float(packet.ch0)
    ch1 = float(packet.ch1)
    ch2 = float(packet.ch2)
    ch3 = float(packet.ch3)
    return [ (ch0, ch1, ch2, ch3) ]

def _channel_values_8274(packet: DataPacket) -> list[tuple[float, ...]]:
    return list(zip(packet.ch5, packet.ch6, packet.ch7))

class PlotSink(SinkInterface):
    """Stream data to a live EEG-style plot via a shared queue.

    Each sink instance is bound to one device (pod). Multiple devices in a
    DataFlow each use their own PlotSink sharing the same queue; the PlotDisplay
    in the main process aggregates streams and shows them in one EEG-style view.

    :param queue: Shared multiprocessing.Queue used to send data to PlotDisplay.
    :param pod: POD device data is being streamed from.
    :param chunk_samples: Number of samples to buffer before sending (default 50).
    """

    def __init__(
        self,
        queue: mp.Queue,
        pod: AcquisitionDevice,
        chunk_samples: int = _DEFAULT_CHUNK_SAMPLES,
        source_id: str | None = None,
        max_display_rate: int = _DEFAULT_MAX_DISPLAY_RATE,
        channel_names: tuple[str, ...] | None = None,
    ) -> None:
        self._queue = queue
        self._pod = pod
        self._source_id = source_id if source_id is not None else getattr(pod, "device_name", str(id(pod)))

        cached_rate = getattr(pod, "_sample_rate", None)
        if cached_rate is not None:
            effective_rate = int(cached_rate[0]) if isinstance(cached_rate, tuple) else int(cached_rate)
        else:
            effective_rate = 1000

        if chunk_samples == _DEFAULT_CHUNK_SAMPLES:
            self._chunk_samples = max(_DEFAULT_CHUNK_SAMPLES, effective_rate // 50)
        else:
            self._chunk_samples = max(1, chunk_samples)

        self._decimate_step = max(1, effective_rate // max_display_rate)
        self._decimate_counter = 0

        if isinstance(self._pod, Pod8206):
            self._channel_names = channel_names if channel_names is not None else ("EEG1", "EEG2", "EMG")
            self._get_values = lambda packet: list(zip(packet.ch0, packet.ch1, packet.ch2))
        elif isinstance(self._pod, Pod8206HR):
            self._channel_names = channel_names if channel_names is not None else ("EEG1", "EEG2", "EEG3/EMG")
            self._get_values = _channel_values_8206
        elif isinstance(self._pod, Pod8401HR):
            if channel_names is not None:
                self._channel_names = channel_names
            else:
                labels = getattr(self._pod, "channel_labels", None)
                if labels is not None:
                    self._channel_names = labels
                else:
                    preamp_map = Pod8401HR.get_channel_map_for_preamp_device(self._pod.preamp)
                    if preamp_map is not None:
                        self._channel_names = tuple(preamp_map.values())
                    else:
                        self._channel_names = ("A", "B", "C", "D")
            self._get_values = _channel_values_8401
        elif isinstance(self._pod, Pod8274D):
            self._channel_names = channel_names if channel_names is not None else ("Ch5", "Ch6", "Ch7")
            self._get_values = _channel_values_8274
        else:
            raise ValueError(f'Device "{getattr(self._pod, "device_name", self._pod)}" is not supported by PlotSink.')

        self._buffer: list[tuple[int, tuple[float, ...]]] = []
        self._skip_remaining = _SKIP_INITIAL_SAMPLES

    @property
    def pod(self) -> AcquisitionDevice:
        return self._pod

    def __enter__(self) -> "PlotSink":
        sample_rate = float(self._pod.sample_rate) if self._pod.sample_rate else 1000.0
        display_rate = sample_rate / self._decimate_step
        try:
            self._queue.put_nowait((_PLOT_REGISTER, self._source_id, display_rate, self._channel_names))
        except Exception:
            pass
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> bool:
        self._flush_buffer()
        try:
            self._queue.put_nowait((_PLOT_UNREGISTER, self._source_id))
        except Exception:
            pass
        return False

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        sample_rate = float(self._pod.sample_rate) if self._pod.sample_rate else 1000.0
        display_rate = sample_rate / self._decimate_step
        msg = (_PLOT_DATA, self._source_id, display_rate, self._channel_names, self._buffer[:])
        self._buffer.clear()
        try:
            self._queue.put_nowait(msg)
        except Exception:
            pass

    def flush(self, timestamp: int, packet: DataPacket) -> None:
        if isinstance(self._pod, Pod8206):
            # Skip/decimate individual samples, not whole USB batches.
            for i, values in enumerate(zip(packet.ch0, packet.ch1, packet.ch2)):
                if self._skip_remaining:
                    self._skip_remaining -= 1
                    continue
                self._decimate_counter += 1
                if self._decimate_counter < self._decimate_step:
                    continue
                self._decimate_counter = 0
                ts = timestamp + round(i * 1e9 / self._pod.sample_rate)
                self._buffer.append((ts, values))
                if len(self._buffer) >= self._chunk_samples:
                    self._flush_buffer()
            return

        # Skip initial unstable samples
        if self._skip_remaining > 0:
            self._skip_remaining -= 1
            return

        # Decimation
        self._decimate_counter += 1
        if self._decimate_counter < self._decimate_step:
            return
        self._decimate_counter = 0

        # Extract values (DEVICE-SPECIFIC)
        if isinstance(self._pod, Pod8206HR):
            values = _channel_values_8206(packet)
            self._buffer.append((timestamp, values[0]))

        elif isinstance(self._pod, Pod8401HR):
            values = _channel_values_8401(packet)
            self._buffer.append((timestamp, values[0]))

        elif isinstance(self._pod, Pod8274D):
            values = _channel_values_8274(packet)

            sample_period_ns = int(1e9 / self._pod.sample_rate)

            for i, value in enumerate(values):
                ts = timestamp + i * sample_period_ns
                self._buffer.append((ts, value))

        else:
            return

        # Chunk flush
        if len(self._buffer) >= self._chunk_samples:
            self._flush_buffer()

    def get_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "queue": self._queue,
            "source_id": self._source_id,
            "chunk_samples": self._chunk_samples,
            "max_display_rate": _DEFAULT_MAX_DISPLAY_RATE,
        }
        if self._channel_names is not None:
            d["channel_names"] = self._channel_names
        return d


# ---------------------------------------------------------------------------
# PlotDisplay: runs in main process, consumes queue and draws EEG-style plot
# ---------------------------------------------------------------------------

try:
    import numpy as np
    import pyqtgraph as pg
    from pyqtgraph.Qt import QtCore, QtWidgets
    _PLOT_AVAILABLE = True
except ImportError:
    _PLOT_AVAILABLE = False
    np = None
    pg = None
    QtCore = None
    QtWidgets = None


class _ChannelState:
    """Per-channel ring buffer and metadata for one stream's channels."""

    __slots__ = ("source_id", "sample_rate", "names", "buffers", "maxlen")

    def __init__(
        self,
        source_id: str,
        sample_rate: float,
        channel_names: tuple[str, ...],
        span_sec: float,
    ) -> None:
        self.source_id = source_id
        self.sample_rate = sample_rate
        self.names = channel_names
        self.maxlen = min(
            _MAX_SAMPLES_PER_CHANNEL,
            max(1, int(span_sec * sample_rate)),
        )
        self.buffers: list[deque[tuple[float, float]]] = [
            deque(maxlen=self.maxlen) for _ in channel_names
        ]

    def append(self, timestamp_ns: int, values: tuple[float, ...]) -> None:
        t_sec = timestamp_ns / 1e9
        for buf, val in zip(self.buffers, values):
            buf.append((t_sec, float(val)))

    def append_rel(self, t_rel_sec: float, values: tuple[float, ...]) -> None:
        """Append one sample per channel with time in seconds since start."""
        for buf, val in zip(self.buffers, values):
            buf.append((t_rel_sec, float(val)))


def _downsample(t: list[float], y: list[float], max_pts: int) -> tuple[list[float], list[float]]:
    if len(t) <= max_pts:
        return t, y
    step = len(t) / max_pts
    idx = [int(i * step) for i in range(max_pts)]
    idx[-1] = min(idx[-1], len(t) - 1)
    return [t[i] for i in idx], [y[i] for i in idx]


class PlotDisplay:
    """EEG-style live plot that consumes from a shared queue (main process).

    Create a multiprocessing.Queue, pass it to each PlotSink, then run this
    display in the main process after starting DataFlow.collect(). Call run()
    to show the window and process the queue until the window is closed.

    Example::

        queue = mp.Queue(maxsize=2048)
        flow = DataFlow([(pod1, [PlotSink(queue, pod1)]), (pod2, [PlotSink(queue, pod2)])])
        flow.collect()
        app = QtWidgets.QApplication([])
        display = PlotDisplay(queue)
        display.run()
        flow.stop_collection()
    """

    def __init__(
        self,
        queue: mp.Queue,
        window_sec: float = 60.0,
        refresh_ms: int = 40,
    ) -> None:
        if not _PLOT_AVAILABLE:
            raise RuntimeError(
                "PlotDisplay requires pyqtgraph and PyQt6.\n"
                "Install with:  pip install ptech-morelia[plot]\n"
                "On Ubuntu/Debian you may also need system libraries — see install_ubuntu.sh.\n"
                "On ARM Linux or where pip cannot build Qt, use conda:\n"
                "  conda install -c conda-forge pyqtgraph pyqt"
            )
        self._queue = queue
        self._window_sec = float(window_sec)
        self._refresh_ms = refresh_ms
        self._channels: dict[str, _ChannelState] = {}
        self._order: list[str] = []
        self._t0_sec: float | None = None  # first sample time (seconds since epoch) for x-axis "seconds since start"
        self._app: QtWidgets.QApplication | None = None
        self._win: QtWidgets.QMainWindow | None = None
        self._glw: "pg.GraphicsLayoutWidget | None" = None
        self._curves: list[pg.PlotDataItem] = []
        self._plots: list = []  # PlotItem per curve, for setYRange
        self._y_ranges: list[tuple[float, float] | None] = []  # (y_min, y_max) per curve, for hysteresis
        self._timer: QtCore.QTimer | None = None

    def _process_queue(self) -> None:
        while True:
            try:
                msg = self._queue.get_nowait()
            except Exception:
                break
            if not isinstance(msg, tuple) or len(msg) < 2:
                continue
            kind = msg[0]
            if kind == _PLOT_REGISTER and len(msg) >= 4:
                _reg, source_id, sample_rate, channel_names = msg[0], msg[1], msg[2], msg[3]
                if source_id not in self._channels:
                    self._channels[source_id] = _ChannelState(
                        source_id, float(sample_rate), tuple(channel_names), self._window_sec
                    )
                    self._order.append(source_id)
                    self._rebuild_plot()
            elif kind == _PLOT_DATA and len(msg) >= 5:
                _data, source_id, sample_rate, channel_names, samples = msg[0], msg[1], msg[2], msg[3], msg[4]
                state = self._channels.get(source_id)
                if state is None:
                    state = _ChannelState(
                        source_id, float(sample_rate), tuple(channel_names), self._window_sec
                    )
                    self._channels[source_id] = state
                    if source_id not in self._order:
                        self._order.append(source_id)
                    self._rebuild_plot()
                for ts, vals in samples:
                    t_sec = ts / 1e9
                    if self._t0_sec is None:
                        self._t0_sec = t_sec
                    t_rel = t_sec - self._t0_sec
                    state.append_rel(t_rel, vals)
            elif kind == _PLOT_UNREGISTER:
                pass  # optional: remove stream from layout
        self._redraw()

    def _rebuild_plot(self) -> None:
        if self._glw is None:
            return
        self._glw.ci.clear()
        self._curves.clear()
        self._plots.clear()
        self._y_ranges.clear()
        for source_id in self._order:
            state = self._channels.get(source_id)
            if state is None:
                continue
            for ch_name in state.names:
                plot = self._glw.addPlot(title=f"{source_id}: {ch_name}")
                plot.setLabel("bottom", "Seconds since start")
                plot.setMouseEnabled(x=True, y=False)
                plot.showGrid(x=True, y=True, alpha=0.3)
                plot.enableAutoRange(axis="y", enable=False)
                curve = plot.plot(pen="y")
                self._curves.append(curve)
                self._plots.append(plot)
                self._y_ranges.append(None)
                self._glw.nextRow()
        self._redraw()

    def _redraw(self) -> None:
        if not self._curves or self._glw is None:
            return
        idx = 0
        for source_id in self._order:
            state = self._channels.get(source_id)
            if state is None:
                continue
            for buf in state.buffers:
                if idx >= len(self._curves):
                    break
                if not buf:
                    idx += 1
                    continue
                t = [x[0] for x in buf]
                y = [x[1] for x in buf]
                if not t:
                    idx += 1
                    continue
                t_ds, y_ds = _downsample(t, y, _DISPLAY_MAX_POINTS)
                self._curves[idx].setData(t_ds, y_ds)
                # Y-axis hysteresis: only update range when data exceeds current bounds
                data_min = min(y_ds)
                data_max = max(y_ds)
                pad = max(_Y_PAD_MIN, (data_max - data_min) * _Y_PAD_FRAC) if data_max > data_min else _Y_PAD_MIN
                cur_range = self._y_ranges[idx] if idx < len(self._y_ranges) else None
                if cur_range is None:
                    y_lo = data_min - pad
                    y_hi = data_max + pad
                    self._plots[idx].setYRange(y_lo, y_hi)
                    self._y_ranges[idx] = (y_lo, y_hi)
                else:
                    y_lo_cur, y_hi_cur = cur_range
                    cur_span = y_hi_cur - y_lo_cur
                    data_span = data_max - data_min if data_max > data_min else 0.0
                    # Expand when data exceeds current bounds
                    if data_min < y_lo_cur or data_max > y_hi_cur:
                        y_lo = min(y_lo_cur, data_min) - pad
                        y_hi = max(y_hi_cur, data_max) + pad
                        self._plots[idx].setYRange(y_lo, y_hi)
                        self._y_ranges[idx] = (y_lo, y_hi)
                    # Shrink when data is much smaller than current range
                    elif cur_span > 0 and data_span < _Y_SHRINK_RATIO * cur_span:
                        y_lo = data_min - pad
                        y_hi = data_max + pad
                        self._plots[idx].setYRange(y_lo, y_hi)
                        self._y_ranges[idx] = (y_lo, y_hi)
                idx += 1

    def run(self) -> None:
        """Create window, start timer, and run Qt event loop until closed."""
        if QtWidgets.QApplication.instance() is None:
            self._app = QtWidgets.QApplication([])

        app = QtWidgets.QApplication.instance()

        def on_window_closed() -> None:
            # destroyed can be emitted from a non-GUI thread; run cleanup on main thread
            def do_cleanup() -> None:
                if self._timer:
                    self._timer.stop()
                    self._timer = None
                app.quit()
            QtCore.QTimer.singleShot(0, do_cleanup)

        self._win = QtWidgets.QMainWindow()
        self._win.setWindowTitle("Morelia Live EEG")
        _WA_DeleteOnClose = getattr(
            QtCore.Qt.WidgetAttribute, "WA_DeleteOnClose",  # PyQt6
            getattr(QtCore.Qt, "WA_DeleteOnClose", None),    # PyQt5
        )
        self._win.setAttribute(_WA_DeleteOnClose)
        self._win.destroyed.connect(on_window_closed)
        self._glw = pg.GraphicsLayoutWidget()
        self._glw.setBackground("k")
        self._win.setCentralWidget(self._glw)
        self._timer = QtCore.QTimer()
        self._timer.timeout.connect(self._process_queue)
        self._timer.start(self._refresh_ms)
        self._win.show()
        app.exec()
