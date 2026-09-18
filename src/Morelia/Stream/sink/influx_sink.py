"""Send data to InfluxDB."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert' 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'
 
import math
from influxdb_client import InfluxDBClient, WriteApi, WriteOptions
import reactivex as rx
import reactivex.operators as ops
from typing import List
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import ilp_channel_tag, resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket


class InfluxSink(SinkInterface):
    """Stream data to InfluxDB for real-time monitoring.

    Non-finite samples retain their timestamp and tags with ``missing=true``
    and no numeric value field. Valid samples include ``missing=false``.

            :param url: URL that points to an InfluxDB server.
            :param api_token: API token to authenticate to InfluxDB. Needs write permissions.
            :param org: Organization within InfluxDB to write data to.
            :param bucket: Bucket within InfluxDB to write data to.
            :param measurement: Measurement within InfluxDB to write data to.
            :param pod: Acquisition device you are streaming data from.
            :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler so the stream is not blocked by InfluxDB I/O. Optional; queue is unbounded.
    """

    def __init__(self, pod: AcquisitionDevice, url: str = "http://localhost:8086", api_token: str = 'admin-token', org: str = 'default-org', bucket: str = 'influx_dump', measurement: str = 'default-measurement', observe_on_scheduler: str | None = None)  -> None:
        """Set instance variables."""
        self.__api_token: str = api_token
        self._url: str = url
        self._pod: AcquisitionDevice = pod
        self._org: str = org
        self._bucket: str = bucket
        self._measurement: str = measurement
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="with_digital")

        spp = int(getattr(pod, "SAMPLES_PER_PACKET", 1) or 1)
        buffer_size = max(1, pod.sample_rate // (spp * 2)) if pod.sample_rate else 1000
        channel_tags = tuple(ilp_channel_tag(n) for n in self._layout.channel_names)
        device_name = pod.device_name

        def _line_protocol_factory(timestamp, packet) -> List[bytes]:
            lines: List[bytes] = []
            for ts, values in self._layout.expand(timestamp, packet):
                for tag, value in zip(channel_tags, values):
                    fields = f"value={value},missing=false" if math.isfinite(value) else "missing=true"
                    lines.append(
                        f"{self._measurement},channel={tag},name={device_name} {fields} {ts}".encode("utf-8")
                    )
            return lines
        
        if self._pod.port_inst is None:
            pass
        else:
            self._subject = rx.Subject()
            self._data = self._subject.pipe(
                ops.starmap(_line_protocol_factory),
                ops.buffer_with_count(buffer_size),
                ops.map(lambda batches: b'\n'.join(line for batch in batches for line in batch))
            )

    @property
    def url(self):
        return self._url

    @property 
    def api_token(self):
        return self.__api_token

    @property
    def org(self):
        return self._org
    
    @property
    def bucket(self):
        return self._bucket
    
    @property
    def measurement(self):
        return self._measurement

    def __enter__(self) -> Self:
        self._client: InfluxDBClient = InfluxDBClient(url=self._url, token=self.__api_token, org=self._org)
        self._writer: WriteApi = self._client.write_api(write_options=WriteOptions(batch_size=1))
        self._writer.write(bucket=self._bucket, org=self._org, record=self._data)

        return self

    
    def __exit__(self, *args, **kwargs) -> bool:

        self._writer.close()
        self._client.close()
        
        del self._writer
        del self._client
       
        return False

    def open(self) -> None:
        """Wrapper around ``self.__enter__`` for use outside of a context manager."""
        self.__enter__()
    
    def close(self) -> None:
        """Wrapper around ``self.__exit__`` for use outside of a context manager."""
        self.__exit__()
    
    def flush(self, timestamp: int, packet: DataPacket) -> None:
        """Write data to InfluxDB.
        :meta private:
        """
        if not hasattr(self, '_client') or not hasattr(self, '_writer'):
            raise RuntimeError('Must open sink before using.')

        self._subject.on_next((timestamp, packet))

    def get_dict(self):
        return {
            'url': self.url,
            'api_token': self.api_token,
            'org': self.org,
            'bucket': self.bucket,
            'measurement': self.measurement,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
