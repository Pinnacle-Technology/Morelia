"""Send data to CSV file."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

import csv
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket

class CSVSink(SinkInterface):
    """Stream data to a CSV file, truncates the destination file each time.
    
    :param file_path: Path to CSV file to write to.
    :param pod: POD device data is being streamed from.
    :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler so the stream is not blocked by CSV I/O. Optional; queue is unbounded.
    """

    def __init__(self, file_path: str, pod: AcquisitionDevice, observe_on_scheduler: str | None = None) -> None:
        """Class constructor."""
        self._file_path = file_path
        self._pod = pod
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="with_digital")

    @property
    def file_path(self):
        return self._file_path
   
    def __enter__(self) -> Self:
        self._file_handle = open(self._file_path, 'w', newline='') 
        self._csv_writer = csv.writer(self._file_handle)
        self._csv_writer.writerow(('time',) + self._layout.channel_names)
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        self._file_handle.close()
        del self._csv_writer
        del self._file_handle
        return False

    #TODO: check that sink is open
    def flush(self, timestamp: int, packet: DataPacket) -> None:
        """
        :meta private:
        """
        for ts, values in self._layout.expand(timestamp, packet):
            self._csv_writer.writerow((ts,) + values)

    def get_dict(self):
        return {
            'file_path': self._file_path,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
