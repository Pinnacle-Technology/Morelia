"""POD device that streams data."""

__author__      = 'James Hurd'
__maintainer__  = 'James Hurd'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2023, James Hurd'
__email__       = 'sales@pinnaclet.com'

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self
from datetime import datetime
from typing import Union

from Morelia.Devices import Pod

class AcquisitionDevice(Pod):
    """
    This class is the parent of any device that can stream (i.e. data acquisiton devices).

    :param port: Serial port to be opened. For COM ports: "COM9" or 9. For D2XX: serial number string or device index.
    :param max_sample_rate: Maximum sample rate supported by the device (in Hz).
    :param baudrate: Baud rate of the opened serial port. Default value is 9600.
    :param device_name: Virtual Name used to indentify device.
    :param get_sample_rate_cmd_no: Command number for the ``GET SAMPLE RATE`` command on the device.
    :param set_sample_rate_cmd_no: Command number for the ``SET SAMPLE RATE`` command on the device.
    :param use_d2xx: If True, use FTDI D2XX direct USB communication instead of COM port. Requires ftd2xx (Windows) or pylibftdi (Linux/Mac). Defaults to False.

    """
    def __init__(self, port: str|int, max_sample_rate: int, baudrate:int=9600, device_name: str | None =  None, 
                 get_sample_rate_cmd_no: int = 100, set_sample_rate_cmd_no: int = 101, use_d2xx: bool = False) -> None:

        super().__init__(port, baudrate=baudrate, device_name=device_name, use_d2xx=use_d2xx) 

        UINT16: int = Pod.get_u(16)
                    
        #initialize as none so that when we ask for the sample rate later, it uses the overidden WriteRead.
        self._sample_rate: int = None

        self._max_sample_rate: int = max_sample_rate

    @property
    def max_sample_rate(self) -> int:
        """Maximum allowable sample rate."""
        return self._max_sample_rate

    @property
    def sample_rate(self) -> int:
        """Currently set sample rate."""
        if self._sample_rate is None:
            self._sample_rate = self.write_read('GET SAMPLE RATE').payload
        return self._sample_rate[0]

    @sample_rate.setter
    def sample_rate(self, rate: int) -> None:
        if rate > self.max_sample_rate:
            raise ValueError(f'The maximum allowable sample rate is {self.max_sample_rate} Hz.')
        self.write_read('SET SAMPLE RATE', (rate,))
        self._sample_rate: int = (rate,)

    @property
    def id(self) -> int:
        """Returns a device's hardware ID."""
        device_id = self.write_read("ID")
        return device_id.payload[0]

    @property
    def type(self) -> int:
        """Returns a device's TYPE number."""
        device_type = self.write_read("TYPE")
        return device_type.payload[0]

    @property
    def pod_type(self) -> str:
        """Maps the device TYPE number to a pod device class name string."""
        type_code = str(self.type)
        type_map = {
            "1": "Pod8206",
            "48": "Pod8206HR",
            "52": "Pod8229",
            "46": "Pod8274D",
            "49": "Pod8401HR",
            "50": "Pod8480SC",
        }
        return type_map.get(type_code, "Unknown Pod Device")

    @property
    def pod_info(self) -> dict:
        """Returns pod identification info as a dictionary."""
        info = {
            "device_name": self.device_name,
            "device_id": self.id,
            "type_number": self.type,
            "device_type": self.pod_type,
        }
        return info

    def validate_config(self, expected_config: dict, skip_keys: set | None = None) -> dict:
        """Compare *expected_config* against the device's current settings.

        :param expected_config: Expected configuration dictionary.
        :param skip_keys: Keys to ignore during comparison.
        :return: Dict mapping config keys (dot-notation) to ``(expected, actual)``
                 tuples for any differences.  Empty when everything matches.
        """
        device_type = type(self).__name__
        skip_keys_map = {
            "Pod8206HR": {"title", "filename", "filter_config", "ttl_pin0", "ttl_pin1", "ttl_pin2", "ttl_pin3", "ttl_port"},
            "Pod8401HR": {"title", "filename", "Gain", "High-pass"},
        }
        if skip_keys is None:
            skip_keys = skip_keys_map.get(device_type, {"title", "filename"})

        actual_config = self._collect_config()
        diffs: dict = {}

        def _normalize(val):
            if isinstance(val, str):
                val = val.strip()
                if val.isdigit():
                    return int(val)
                try:
                    return float(val)
                except ValueError:
                    return val
            if isinstance(val, float):
                return round(val, 6)
            return val

        def _recursive_diff(expected, actual, cur_skip_keys, path=""):
            for key in expected:
                if key in cur_skip_keys:
                    continue
                expected_val = expected[key]
                actual_val = actual.get(key) if isinstance(actual, dict) else None
                current_path = f"{path}.{key}" if path else key
                if isinstance(expected_val, dict) and isinstance(actual_val, dict):
                    _recursive_diff(expected_val, actual_val, cur_skip_keys, current_path)
                else:
                    norm_expected = _normalize(expected_val)
                    norm_actual = _normalize(actual_val)
                    if norm_expected != norm_actual:
                        diffs[current_path] = (
                            (norm_expected, type(norm_expected)),
                            (norm_actual, type(norm_actual)),
                        )

        _recursive_diff(expected_config, actual_config, skip_keys)
        return diffs

    property_map: dict = {
        "sample_rate": {
            "sample_rate": "sample_rate",
        },
    }

    @classmethod
    def get_combined_property_map(cls) -> dict:
        """Merge the base :attr:`property_map` with any device-specific
        ``_property_map`` defined on a subclass."""
        combined = {}
        combined.update(AcquisitionDevice.property_map)
        device_map = getattr(cls, "_property_map", None)
        if device_map is not None:
            combined.update(device_map)
        return combined

    def __enter__(self) -> Self:
        # Open port on first use when using D2XX (port is deferred in __init__ to avoid
        # main process holding the device and causing the worker to block on open).
        if self._port is None:
            self.open_port()

        #no WriteRead, because the confirmation packet may arrive
        #after streaming data due to a race condition in the device's
        #firmware. Therefore, we leave dealing with the response packet
        #to the user.
        self.write_packet('STREAM', 1)

        return self

    def __exit__(self, *args, **kwargs) -> bool:
        self.write_packet('STREAM', 0)

        #get any packets that may have arrived between the user ending stream
        #and the command being received from the device + plus the response
        #packet from earlier.

        while True:
            try:
                self.read_pod_packet(timeout_sec=1)
            except TimeoutError:
                break

        # with open("end_times.log", "a") as f:
        #     f.write(f"Stream ended at {datetime.now().isoformat()}\n")

        #explicitly tell the context manager to propagate execptions.
        return False
