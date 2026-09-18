"""Stream data to a PVFS (Pinnacle Virtual File System) file."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

import logging
import multiprocessing as mp
import os
import sys
import time

_log = logging.getLogger(__name__)


def _pvfs_debug(msg: str, *args) -> None:
    """Debug logging for PVFS sink (visible without logging config)."""
    text = msg % args if args else msg
    print(f"[PvfsSink] {text}", flush=True)
    _log.info("[PvfsSink] %s", text)
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.packet.data import DataPacket
from Morelia.Devices import AcquisitionDevice

_PVFS_IMPORT_ERROR = None
try:
    from pvfs_tools.Core.pvfs_data_file import PvfsDataFile
    from pvfs_tools.Core.pvfs_binding import HighTime
    _PVFS_AVAILABLE = True
except (ImportError, RuntimeError) as e:
    _PVFS_AVAILABLE = False
    _PVFS_IMPORT_ERROR = e
    PvfsDataFile = None  # type: ignore
    HighTime = None  # type: ignore


def _pvfs_writer_target(
    queue: mp.Queue,
    stop_event: mp.Event,
    file_path: str,
    channels: tuple[str, ...],
    units: tuple[str, ...],
    sample_rate: float,
    device_preferences: list[dict] | None = None,
) -> None:
    """Target for the dedicated PVFS writer process.

    Drains *queue* in batches, buffering channel values and writing to the
    PVFS file once per second of data.  Exits when *stop_event* is set and the
    queue is empty.
    """
    from pvfs_tools.Core.pvfs_data_file import PvfsDataFile
    from pvfs_tools.Core.pvfs_binding import HighTime

    pvfs_data = PvfsDataFile()
    if not pvfs_data.create(file_path):
        print(f"[PvfsWriter] ERROR: PvfsDataFile.create failed for {file_path}", file=sys.stderr, flush=True)
        return

    start_time = HighTime.from_seconds(time.time())
    pvfs_data.set_experiment_info(
        name="Morelia PVFS recording",
        description="Streamed data from Morelia data collection",
        start_time=start_time,
    )
    for ch_name, unit in zip(channels, units):
        idf = pvfs_data.create_channel(ch_name, data_rate=sample_rate, unit=unit or "uV")
        if idf is None:
            print(f"[PvfsWriter] ERROR: Failed to create channel {ch_name}", file=sys.stderr, flush=True)
            return
        idf._delta_time = HighTime(0, 1.0 / sample_rate)

    if device_preferences:
        pvfs_data.set_device_preferences(device_preferences)

    n_channels = len(channels)
    buf: list[list[float]] = [[] for _ in channels]
    samples_written = 0
    flush_threshold = int(sample_rate)

    def write_buf() -> None:
        nonlocal samples_written
        if not buf[0]:
            return
        n = len(buf[0])
        block_start = HighTime.from_seconds(start_time.to_seconds() + samples_written / sample_rate)
        for ch_name, ch_buf in zip(channels, buf):
            idf = pvfs_data._indexed_data_files.get(ch_name)
            if idf is not None:
                idf.append_block(block_start, ch_buf)
        samples_written += n
        for b in buf:
            b.clear()

    def drain_queue() -> int:
        """Get all available items without blocking. Returns count drained."""
        count = 0
        while True:
            try:
                item = queue.get_nowait()
            except Exception:
                break
            for ch_idx in range(n_channels):
                buf[ch_idx].append(item[ch_idx])
            count += 1
            if len(buf[0]) >= flush_threshold:
                write_buf()
        return count

    while not stop_event.is_set():
        try:
            item = queue.get(timeout=0.1)
        except Exception:
            continue
        for ch_idx in range(n_channels):
            buf[ch_idx].append(item[ch_idx])
        drain_queue()
        if len(buf[0]) >= flush_threshold:
            write_buf()

    drain_queue()
    write_buf()
    for idf in pvfs_data._indexed_data_files.values():
        if idf is not None:
            idf.flush(synchronous=True)
    pvfs_data.flush(synchronous=True)
    pvfs_data.close()
    _pvfs_debug("Writer process finished: %s (%d samples)", file_path, samples_written)


class PvfsSink(SinkInterface):
    """Stream data to a PVFS file compatible with Sirenia data format.

    Creates a PVFS file with experiment.db3 and indexed channel files (.index / .idat).
    Index timestamps are written in absolute time (seconds since epoch).
    Supports Pod8206, Pod8206HR, Pod8401HR, and Pod8274D via the shared stream layout.

    :param file_path: Path to the .pvfs file to create.
    :param pod: POD device data is being streamed from.
    :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler so the stream is not blocked by PVFS I/O. Use with multi-sink flows to avoid slowing other sinks. Queue is unbounded; slow sinks can increase memory use.
    :param use_writer_process: If True, all PVFS I/O runs in a dedicated child process, fully eliminating GIL contention with the emission thread. Recommended at sample rates >= 5000 sps when used alongside PlotSink.
    """

    def __init__(
        self,
        file_path: str,
        pod: AcquisitionDevice,
        observe_on_scheduler: str | None = None,
        use_writer_process: bool = False,
        device_preferences: list[dict] | None = None,
    ) -> None:
        if not _PVFS_AVAILABLE:
            msg = (
                "PVFS support is not available, or the PVFS native library failed to load for this platform. "
                "Install the `pypvfs` package (`pip install pypvfs`) and ensure you are on Windows or Linux with the correct binaries."
            )
            if _PVFS_IMPORT_ERROR is not None:
                msg += f" Reason: {_PVFS_IMPORT_ERROR}"
            raise RuntimeError(msg) from _PVFS_IMPORT_ERROR
        self._file_path = file_path
        self._pod = pod
        self._layout = resolve_layout(pod, profile="analog")
        self._channels = self._layout.channel_names
        self._units = self._layout.units

        self._buffer = [ [] for _ in self._channels ]
        self._pvfs_data: PvfsDataFile | None = None
        self._start_time: HighTime | None = None
        self._samples_written = 0
        self._use_writer_process = use_writer_process
        if use_writer_process:
            self.observe_on_scheduler = None
        else:
            self.observe_on_scheduler = observe_on_scheduler
        self._device_preferences = device_preferences
        self._writer_queue: mp.Queue | None = None
        self._writer_proc: mp.Process | None = None
        self._writer_stop: mp.Event | None = None

    @property
    def pod(self) -> AcquisitionDevice:
        return self._pod

    @pod.setter
    def pod(self, device: AcquisitionDevice) -> None:
        self._pod = device

    @property
    def file_path(self) -> str:
        return self._file_path

    def __enter__(self) -> Self:
        path_abs = os.path.abspath(self._file_path) if self._file_path else self._file_path
        sample_rate = float(self._pod.sample_rate) if self._pod.sample_rate else 400.0

        if self._use_writer_process:
            _pvfs_debug(
                "__enter__ (writer process mode) pid=%s file_path=%s",
                os.getpid(), path_abs,
            )
            self._writer_queue = mp.Queue(maxsize=0)
            self._writer_stop = mp.Event()
            self._writer_proc = mp.Process(
                target=_pvfs_writer_target,
                args=(self._writer_queue, self._writer_stop, self._file_path,
                      self._channels, self._units, sample_rate,
                      self._device_preferences),
            )
            self._writer_proc.start()
            return self

        _pvfs_debug(
            "__enter__ pid=%s cwd=%s file_path=%s -> absolute=%s",
            os.getpid(), os.getcwd(), self._file_path, path_abs,
        )
        self._pvfs_data = PvfsDataFile()
        ok = self._pvfs_data.create(self._file_path)
        if not ok:
            raise RuntimeError(f"PvfsDataFile.create failed for {self._file_path}")
        _pvfs_debug("PVFS created successfully: %s", path_abs)

        self._start_time = HighTime.from_seconds(time.time())
        self._pvfs_data.set_experiment_info(
            name="Morelia PVFS recording",
            description="Streamed data from Morelia data collection",
            start_time=self._start_time,
        )

        for ch_name, unit in zip(self._channels, self._units):
            idf = self._pvfs_data.create_channel(
                ch_name, data_rate=sample_rate, unit=unit or "uV"
            )
            if idf is None:
                raise RuntimeError(f"Failed to create PVFS channel {ch_name}")
            idf._delta_time = HighTime(0, 1.0 / sample_rate)

        if self._device_preferences:
            self._pvfs_data.set_device_preferences(self._device_preferences)

        self._samples_written = 0
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        print(f"[PvfsSink] __exit__ entered pid={os.getpid()}", file=sys.stderr, flush=True)
        path_abs = os.path.abspath(self._file_path) if self._file_path else self._file_path

        if self._use_writer_process:
            if self._writer_stop is not None:
                self._writer_stop.set()
            if self._writer_proc is not None:
                self._writer_proc.join(timeout=15.0)
                if self._writer_proc.is_alive():
                    _pvfs_debug("Writer process did not exit in time, terminating: %s", path_abs)
                    self._writer_proc.terminate()
                    self._writer_proc.join(timeout=2.0)
            self._writer_queue = None
            self._writer_proc = None
            self._writer_stop = None
            _pvfs_debug("Writer process exited cleanly: %s", path_abs)
            return False

        _pvfs_debug(
            "__exit__ pid=%s saving and closing path=%s samples_written=%s",
            os.getpid(), path_abs, self._samples_written,
        )
        self._write_buffer_to_pvfs()

        if self._pvfs_data is not None:
            try:
                for idf in self._pvfs_data._indexed_data_files.values():
                    if idf is not None:
                        idf.flush(synchronous=True)
                self._pvfs_data.flush(synchronous=True)
                self._pvfs_data.close()
                _pvfs_debug("PVFS closed successfully: %s", path_abs)
            except Exception as e:
                _log.warning("Error closing PVFS file: %s: %s", path_abs, e, exc_info=True)
                print(f"Warning: Error closing PVFS file: {type(e).__name__}: {e}", file=sys.stderr)
            finally:
                self._pvfs_data = None
        return False

    def flush(self, timestamp: int, packet: DataPacket) -> None:
        """Append one sample per channel to the buffer; write blocks when buffer reaches 1 second."""
        if self._use_writer_process:
            if self._writer_queue is None:
                return
            try:
                for _ts, values in self._layout.expand(timestamp, packet):
                    self._writer_queue.put_nowait(values)
            except Exception:
                pass
            return

        if self._pvfs_data is None:
            return

        sample_rate = float(self._pod.sample_rate) if self._pod.sample_rate else 400.0

        for _ts, values in self._layout.expand(timestamp, packet):
            for i, value in enumerate(values):
                self._buffer[i].append(value)

        if len(self._buffer[0]) >= int(sample_rate):
            self._write_buffer_to_pvfs()

    def _write_buffer_to_pvfs(self) -> None:
        if self._pvfs_data is None or self._start_time is None:
            return
        if not self._buffer or not self._buffer[0]:
            return

        lengths = [len(b) for b in self._buffer]
        if len(set(lengths)) != 1:
            print(
                f"Warning: PvfsSink skipping write due to mismatched buffer lengths: {lengths}",
                file=sys.stderr,
            )
            self._buffer = [ [] for _ in self._channels ]
            return

        sample_rate = float(self._pod.sample_rate) if self._pod.sample_rate else 400.0
        n = lengths[0]
        block_start = HighTime.from_seconds(
            self._start_time.to_seconds() + self._samples_written / sample_rate
        )

        for ch_name, buf in zip(self._channels, self._buffer):
            idf = self._pvfs_data._indexed_data_files.get(ch_name)
            if idf is None:
                continue
            result = idf.append_block(block_start, buf)
            if result != 0:
                print(
                    f"Warning: PvfsSink append_block({ch_name}) returned {result}",
                    file=sys.stderr,
                )

        self._samples_written += n
        self._buffer = [ [] for _ in self._channels ]

    def get_dict(self) -> dict:
        # Use absolute path so the worker process (DataFlow runs sinks in a subprocess)
        # writes to the same file regardless of its current working directory.
        file_path = self._file_path
        if file_path and not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)
        return {
            "file_path": file_path,
            "observe_on_scheduler": self.observe_on_scheduler,
            "use_writer_process": self._use_writer_process,
            "device_preferences": self._device_preferences,
        }
