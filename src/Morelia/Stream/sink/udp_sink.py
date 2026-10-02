"""Stream data over UDP to a configurable host/port."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

import socket
import struct
import sys
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket


class UDPSink(SinkInterface):    
    """Stream data over UDP to a destination host/port.

    Send-only UDP sink: one datagram per sample for single-sample devices, or
    one datagram per packet for batched devices (8206 / 8274D). Payload is
    little-endian binary derived from the shared stream layout:

    - 3 analog channels: ``<Qfff>`` per sample (or batch header ``<QH>`` + N×``fff``)
    - 4 analog channels: ``<Qffff>`` per sample

    :param port: Destination port (required).
    :param pod: POD device data is being streamed from.
    :param host: Destination host (default 127.0.0.1 for local use).
    :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler. Optional; queue is unbounded.
    """

    def __init__(
        self,
        port: int,
        pod: AcquisitionDevice,
        host: str = "127.0.0.1",
        observe_on_scheduler: str | None = None,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._pod = pod
        self._socket: socket.socket | None = None
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="analog")
        n = len(self._layout.analog_attrs)
        if n not in (3, 4):
            raise ValueError(f"UDPSink supports 3 or 4 analog channels, got {n}")
        self._sample_fmt = "<Q" + ("f" * n)
        self._body_fmt = "<" + ("f" * n)

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    def __enter__(self) -> Self:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind(('', 0))
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        if self._socket is not None:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        return False

    def flush(self, timestamp: int, packet: DataPacket) -> None:
        if self._socket is None:
            return
        try:
            payload = self._pack_payload(timestamp, packet)
            if payload is not None:
                self._socket.sendto(payload, (self._host, self._port))
        except OSError as e:
            print(f"UDPSink sendto failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"UDPSink flush error: {e}", file=sys.stderr)

    def _pack_payload(self, timestamp: int, packet: DataPacket) -> bytes | None:
        """Pack (timestamp, packet) into little-endian bytes."""
        samples = self._layout.expand(timestamp, packet)
        if not samples:
            return None

        if self._layout.batched:
            header = struct.pack("<QH", timestamp, len(samples))
            body = b"".join(
                struct.pack(self._body_fmt, *values) for _ts, values in samples
            )
            return header + body

        ts, values = samples[0]
        return struct.pack(self._sample_fmt, ts, *values)

    def get_dict(self) -> dict:
        return {
            'host': self._host,
            'port': self._port,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
