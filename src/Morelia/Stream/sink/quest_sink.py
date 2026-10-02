"""Send data to QuestDB."""

__author__      = 'Josselyn Bui'
__maintainer__  = 'Josselyn Bui'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Andrew Huang', 'Josselyn Bui']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2025, Josselyn Bui'
__email__       = 'sales@pinnaclet.com'

import socket
import math
import reactivex as rx
import reactivex.operators as ops
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import ilp_channel_tag, resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket


class QuestSink(SinkInterface):
    """Stream data to QuestDB for real-time monitoring.

    Non-finite samples use ``missing=true`` and omit the numeric value field;
    QuestDB supplies NULL for the omitted value. Valid samples set missing=false.

        :param host: Specifies the source of data. For local hosting use "localhost".
        :param port: Default QuestDB port is 9009 for ILP TCP service (InfluxDB Line Protocol).
        :param measurement: Measurement within QuestDB to write data to.
        :param pod: Acquisition device you are streaming data from.
        :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler so the stream is not blocked by QuestDB I/O. Optional; queue is unbounded.
    """
    def __init__(self, pod: AcquisitionDevice, host: str = "localhost", port: int = 9009, measurement: str = "default_measurement", observe_on_scheduler: str | None = None) -> None:
        """Set instance variables"""
        self._host = host
        self._port = port
        self._measurement = measurement
        self._pod = pod
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="with_digital")

        channel_tags = tuple(ilp_channel_tag(n) for n in self._layout.channel_names)
        device_name = pod.device_name
        spp = int(getattr(pod, "SAMPLES_PER_PACKET", 1) or 1)
        buffer_count = max(1, pod.sample_rate // (spp * 2)) if pod.sample_rate else 1

        def _line_protocol_factory(timestamp, packet) -> bytes:
            lines = []
            for ts, values in self._layout.expand(timestamp, packet):
                for tag, value in zip(channel_tags, values):
                    fields = f"value={value},missing=false" if math.isfinite(value) else "missing=true"
                    lines.append(
                        f"{self._measurement},channel={tag},name={device_name} {fields} {ts}"
                    )
            return "\n".join(lines).encode("utf-8")

        if self._pod.port_inst is None:
            pass
        else:
            self._subject = rx.Subject()
            self._data = self._subject.pipe(
                ops.starmap(_line_protocol_factory),
                ops.buffer_with_count(buffer_count),
                ops.map(lambda x: b'\n'.join(x))
            )

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def measurement(self):
        return self._measurement

    def __enter__(self) -> Self:
        self._sock = socket.create_connection((self._host, self._port))
        self._data.subscribe(lambda data: self._sock.sendall(data + b'\n'))
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        self._sock.close()
        del self._sock
        return False

    def open(self) -> None:
        self.__enter__()

    def close(self) -> None:
        self.__exit__()

    def flush(self, timestamp: int, packet: DataPacket) -> None:
        if not hasattr(self, '_sock'):
            raise RuntimeError("Sink must be opened before flushing.")
        self._subject.on_next((timestamp, packet))

    def get_dict(self):
        return {
            'host': self.host,
            'port': self.port,
            'measurement': self.measurement,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
