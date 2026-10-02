"""Stream OSC packets over UDP to a configurable host/port."""

__author__      = 'Sean Gupta'
__maintainer__  = ''
__credits__     = []
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2026'
__email__       = 'sales@pinnaclet.com'

import sys
from pythonosc.udp_client import SimpleUDPClient
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.Devices import AcquisitionDevice
from Morelia.packet.data import DataPacket

class OSCSink(SinkInterface):    
    """
    Streams acquisition data as Open Sound Control (OSC) messages over UDP.

    Channel values come from the shared stream layout (analog profile).
    Single-sample devices send ``[timestamp, *channel_values]``.
    Batched devices send one message per packet:
    ``[timestamp, *ch0_samples, *ch1_samples, *ch2_samples]`` (flattened).

    :param port: UDP port on the destination host to send OSC messages to.
    :param pod: POD device whose streamed packets will be converted to OSC messages.
    :param address: OSC address pattern used for transmitted messages. Defaults to "/".
    :param host: Destination IP address or hostname. Defaults to "127.0.0.1" (localhost).
    :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler. Optional; queue is unbounded.
    """
    
    def __init__(
        self,
        port: int,
        pod: AcquisitionDevice,
        address: str = "/",
        host: str = "127.0.0.1",
        observe_on_scheduler: str | None = None,
    ) -> None:
        
        # Address validation
        if not isinstance(address, str):
            raise TypeError("address must be a string.")
        if not address.startswith("/"):
            raise ValueError("address must start with '/'.")
        if " " in address:
            raise ValueError("address cannot contain spaces.")

        self._host = host
        self._port = int(port)
        self._pod = pod
        self._address = address
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="analog")

        self._client: SimpleUDPClient | None = None

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port
    
    @property
    def address(self) -> str:
        return self._address

    def __enter__(self) -> Self:
        self._client = SimpleUDPClient(self._host, self._port)
        return self

    def __exit__(self, *args, **kwargs) -> bool:
        self._client = None
        return False

    def flush(self, timestamp: int, packet: DataPacket) -> None:
        if self._client is None:
            return

        try:
            if self._layout.batched:
                # Preserve prior 8274-style payload: timestamp + flattened channel lists.
                flat: list[float] = [int(timestamp)]
                for series in self._layout.analog_packet_values(packet):
                    flat.extend(float(x) for x in series)
                self._client.send_message(self._address, flat)
                return

            for ts, values in self._layout.expand(timestamp, packet):
                self._client.send_message(
                    self._address,
                    [int(ts), *[float(v) for v in values]],
                )

        except Exception as e:
            print(f"OSCSink flush error: {e}", file=sys.stderr)

    def get_dict(self) -> dict:
        return {
            'host': self._host,
            'port': self._port,
            'address': self._address,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
