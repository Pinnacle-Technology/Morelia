"""Send data to an in-memory buffer."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket

class BufferSink(SinkInterface):
    """Stream data to a buffer.

    When using a multiprocessing Manager list, set batch_size > 1 to append samples in chunks
    and reduce IPC (one extend per batch instead of one append per sample).

    For batched devices (Pod8206, Pod8274D), each flush appends one row whose channel
    values are the full per-packet sample lists (one buffer entry per USB/BT packet).

    :param buffer: Target list to append (timestamp, data) rows to; supports list and manager.list().
    :param pod: POD device data is being streamed from.
    :param batch_size: Flush to buffer every this many samples (default 100). Use 1 for no batching.
    :type pod: class:`AcquisitionDevice`
    """

    def __init__(self, buffer, pod: AcquisitionDevice, batch_size: int = 100) -> None:
        """Class constructor."""
        self._pod = pod
        self._buffer = buffer
        self._batch_size = max(1, int(batch_size))
        self._batch: list = []
        self._layout = resolve_layout(pod, profile="with_digital")

    @property
    def buffer(self):
        return self._buffer

    def __enter__(self) -> Self:
        self._batch = []
        if self._layout.batched:
            names = tuple(f"{n} Batch" for n in self._layout.channel_names)
        else:
            names = self._layout.channel_names
        self._buffer.append(('Time',) + names)
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        if self._batch:
            self._buffer.extend(self._batch)
            self._batch = []
        return False

    def _flush_batch_if_full(self) -> None:
        """Push batch to shared buffer when it reaches batch_size (reduces IPC for manager.list())."""
        if len(self._batch) >= self._batch_size:
            self._buffer.extend(self._batch)
            self._batch.clear()

    #TODO: check that sink is open
    def flush(self, timestamp: int, packet: DataPacket) -> None:
        if self._layout.batched:
            # One buffer entry per packet; channel values are sample lists.
            self._batch.append((timestamp, self._layout.analog_packet_values(packet)))
            self._flush_batch_if_full()
            return

        for ts, values in self._layout.expand(timestamp, packet):
            self._batch.append((ts, values))
            self._flush_batch_if_full()
    
    def get_dict(self):
        return {
            'buffer': self.buffer,
            'batch_size': self._batch_size,
        }
