# local imports
import time
from Morelia.Devices import AcquisitionDevice, Pod
from Morelia.packet import ControlPacket, PodPacket
from Morelia.packet.data import DataPacket8206HR
from Morelia.Commands import CommandSet
import Morelia.packet.conversion as conv

from Morelia.ParamSchema.ParamSchema import ParamSchema

from functools import partial

# authorship
__author__      = "Thresa Kelly"
__maintainer__  = "James Hurd"
__credits__     = ["Thresa Kelly", "Seth Gabbert", "James Hurd"]
__license__     = "New BSD License"
__copyright__   = "Copyright (c) 2023, Thresa Kelly"
__email__       = "sales@pinnaclet.com"

class Pod8206HR(AcquisitionDevice) : 
    """
    Pod8206HR is used to interact with a 8206HR data acquisition device.

    :param port: Serial port to be opened. For COM ports: "COM9" or 9. For D2XX: serial number string or device index.
    :param preamp_gain: A unitless number used to add gain to vlues recived from the preamplifier. Used in converting streaming data from the device into something human-readable. Must be 10 or 100.
    :param baudrate: Baud rate of the opened serial port. Default value is 9600.
    :param device_name: Virtual name used to indentify device.
    :param use_d2xx: If True, use FTDI D2XX direct USB communication instead of COM port. Requires ftd2xx (Windows) or pylibftdi (Linux/Mac). Defaults to False.
   """ 
    def __init__(self, port: str|int, preamp_gain: int, baudrate:int=9600, device_name: str | None =  None, use_d2xx: bool = False, sample_rate: int | None = None) -> None :
        
        #self._port_value = port

        # initialize POD_Basics
        super().__init__(port, 2000, baudrate, device_name, use_d2xx=use_d2xx)
        
        self._init_device(
            preamp_gain=preamp_gain,
            sample_rate=sample_rate,
        )

    def _init_device(
            self,
            preamp_gain: int,
            sample_rate: int,
            ):
        if sample_rate is not None:
            self._sample_rate = (sample_rate,) 

        # get constants for adding commands 
        UINT8  = Pod.get_u(8)
        UINT16 = Pod.get_u(16)
        BINARY_4  = 8

        # remove unimplemented commands 
        self._commands.remove_command(5)  # STATUS
        self._commands.remove_command(10) # SAMPLE RATE
        self._commands.remove_command(11) # BINARY

        # add device specific commands
        self._commands.add_command(100, 'GET SAMPLE RATE',      (0,),       (UINT16,),    False,   'Gets the current sample rate of the system, in Hz.')
        self._commands.add_command(101, 'SET SAMPLE RATE',      (UINT16,),     (0,),      False,   'Sets the sample rate of the system, in Hz. Valid values are 100 - 2000 currently.')
        self._commands.add_command(102, 'GET LOWPASS',          (UINT8,),      (UINT16,),    False,   'Gets the lowpass filter for the desired channel (0 = EEG1, 1 = EEG2, 2 = EEG3/EMG). Returns the value in Hz.')
        self._commands.add_command(103, 'SET LOWPASS',          (UINT8,UINT16),   (0,),      False,   'Sets the lowpass filter for the desired channel (0 = EEG1, 1 = EEG2, 2 = EEG3/EMG) to the desired value (11 - 500) in Hz.')
        self._commands.add_command(104, 'SET TTL OUT',          (UINT8,UINT8),    (0,),      False,   'Sets the selected TTL pin (0,1,2,3) to an output and sets the value (0-1).')
        self._commands.add_command(105, 'GET TTL IN',           (UINT8,),      (UINT8,),     False,   'Sets the selected TTL pin (0,1,2,3) to an input and returns the value (0-1).')
        self._commands.add_command(106, 'GET TTL PORT',         (0,),       (UINT8,),     False,   'Gets the value of the entire TTL port as a byte. Does not modify pin direction.')
        self._commands.add_command(107, 'GET FILTER CONFIG',    (0,),       (UINT8,),     False,   'Gets the hardware filter configuration. 0=SL, 1=SE (Both 40/40/100Hz lowpass), 2 = SE3 (40/40/40Hz lowpas).')
        self._commands.add_command(108, 'GET TTL DIRECTION',    (0,),       (UINT8,UINT8,),  False,   'Returns the direction and state for each TTL pin.  1=input, 0=output')
        self._commands.add_command(109, 'SET FILTER CONFIG',    (UINT8,),      (0,),      False,   'Sets the hardware filter configuration. 0=SL, 1=SE (Both 40/40/100Hz lowpass), 2 = SE3 (40/40/40Hz lowpas).')
        self._commands.add_command(110, 'GET PREAMP TYPE',      (0,),       (UINT8,),     False,   'Gets the hardware filter configuration. 0=SL, 1=SE (Both 40/40/100Hz lowpass), 2 = SE3 (40/40/40Hz lowpas).')
        self._commands.add_command(111, 'SET PREAMP TYPE',      (UINT8,),      (0,),      False,   'Gets the hardware filter configuration. 0=SL, 1=SE (Both 40/40/100Hz lowpass), 2 = SE3 (40/40/40Hz lowpas).')
        self._commands.add_command(180, 'BINARY4 DATA ',        (0,),       (BINARY_4,),     True,    'Binary4 data packets, enabled by using the STREAM command with a \'1\' argument.') # see _read_binary()

        # preamplifier gain (should be 10x or 100x) - NOTE: We should do some error checkign here, but this is the wrong way to do it because there can be non 10/100 gains
        if(preamp_gain != 10 and preamp_gain != 100):
            raise Exception('[!] Preamplifier gain must be 10 or 100.')
        self._preamp_gain : int = preamp_gain 

         # define function used to decode packet from binary data.
        def decode_packet(command_number: int, payload: bytes) -> tuple:
            if command_number == 106:
                return Pod8206HR._translate_ttlbyte_ascii(payload)

            return ControlPacket.decode_payload_from_cmd_set(self._commands, command_number, payload)

        # the constructor used to create control packets as they are recieved.
        self._control_packet_factory = partial(ControlPacket, decode_packet)

    @property
    def preamp_gain(self):
        return self._preamp_gain

    # ------------ DEVICE PROPERTIES (config-capable) ------------ 

    @property
    def lowpass_ch0(self) -> int:
        """Lowpass filter for channel 0 (EEG1) in Hz."""
        return self.write_read("GET LOWPASS", (0,)).payload[0]

    @lowpass_ch0.setter
    def lowpass_ch0(self, value: int) -> None:
        try:
            self.write_read("SET LOWPASS", (0, value), timeout_sec=2.0)
        except Exception as e:
            print(f"[LOWPASS] Failed to set lowpass_ch0 to {value}: {e}")

    @property
    def lowpass_ch1(self) -> int:
        """Lowpass filter for channel 1 (EEG2) in Hz."""
        return self.write_read("GET LOWPASS", (1,)).payload[0]

    @lowpass_ch1.setter
    def lowpass_ch1(self, value: int) -> None:
        try:
            self.write_read("SET LOWPASS", (1, value), timeout_sec=2.0)
        except Exception as e:
            print(f"[LOWPASS] Failed to set lowpass_ch1 to {value}: {e}")

    @property
    def lowpass_ch2(self) -> int:
        """Lowpass filter for channel 2 (EEG3/EMG) in Hz."""
        return self.write_read("GET LOWPASS", (2,)).payload[0]

    @lowpass_ch2.setter
    def lowpass_ch2(self, value: int) -> None:
        try:
            self.write_read("SET LOWPASS", (2, value), timeout_sec=2.0)
        except Exception as e:
            print(f"[LOWPASS] Failed to set lowpass_ch2 to {value}: {e}")

    @property
    def ttl_pin0(self) -> int:
        """TTL pin 0 input value (0 or 1)."""
        return self.write_read("GET TTL IN", (0,)).payload[0]

    @ttl_pin0.setter
    def ttl_pin0(self, value: int) -> None:
        self.write_packet("SET TTL OUT", (0, value))

    @property
    def ttl_pin1(self) -> int:
        """TTL pin 1 input value (0 or 1)."""
        return self.write_read("GET TTL IN", (1,)).payload[0]

    @ttl_pin1.setter
    def ttl_pin1(self, value: int) -> None:
        self.write_packet("SET TTL OUT", (1, value))

    @property
    def ttl_pin2(self) -> int:
        """TTL pin 2 input value (0 or 1)."""
        return self.write_read("GET TTL IN", (2,)).payload[0]

    @ttl_pin2.setter
    def ttl_pin2(self, value: int) -> None:
        self.write_packet("SET TTL OUT", (2, value))

    @property
    def ttl_pin3(self) -> int:
        """TTL pin 3 input value (0 or 1)."""
        return self.write_read("GET TTL IN", (3,)).payload[0]

    @ttl_pin3.setter
    def ttl_pin3(self, value: int) -> None:
        self.write_packet("SET TTL OUT", (3, value))

    @property
    def ttl_port(self) -> int:
        """Entire TTL port value as a byte (read-only, does not modify pin direction)."""
        return self.write_read("GET TTL PORT").payload[0]

    @property
    def filter_config(self) -> int:
        """Hardware filter configuration.  0=SL, 1=SE (40/40/100Hz), 2=SE3 (40/40/40Hz)."""
        return self.write_read("GET FILTER CONFIG").payload[0]

    # ------------ CONFIGURATION MAP ------------ 

    _property_map: dict = {
        "lowpass": {
            "lowpass_ch0": "lowpass_ch0",
            "lowpass_ch1": "lowpass_ch1",
            "lowpass_ch2": "lowpass_ch2",
        },
        "ttl_pins": {
            "ttl_port": "ttl_port",
        },
        "filter_config": {
            "filter_config": "filter_config",
        },
    }

    def _apply_config_recursive(self, config: dict, skip_keys: set):
        super()._apply_config_recursive(config, skip_keys)

    @staticmethod
    def _translate_ttlbyte_ascii(ttl_byte: bytes) -> dict[str,int] : 
        """Separates the bits of each TTL (0-3) from a ASCII encoded byte.

        :param ttl_byte: One byte string for the TTL (ASCII encoded).

        :return: Dictionary of the TTLs. Values are 1 when input, 0 when output.
        """
        # TTL : b 0123 XXXX <-- 8 bits, lowest 4 are always 0 (dont care=X), msb is TTL0
        return ( {
            'TTL1' : conv.ascii_bytes_to_int_split(ttl_byte, 8, 7), # TTL 0 
            'TTL2' : conv.ascii_bytes_to_int_split(ttl_byte, 7, 6), # TTL 1 
            'TTL3' : conv.ascii_bytes_to_int_split(ttl_byte, 6, 5), # TTL 2 
            'TTL4' : conv.ascii_bytes_to_int_split(ttl_byte, 5, 4)  # TTL 3 
        }, )   

    def _read_binary(self, pre_packet: bytes, validate_checksum:bool=True) -> DataPacket8206HR :
        """After receiving the pre_packet, it reads the 8 bytes(TTL+channels) and then reads to ETX (checksum+ETX). 
        See the documentation of ``DataPacket8206HR`` for my details on what this packet looks like at a protocol level.

        :param pre_packet: Bytes string containing the beginning of a POD packet: STX (1 byte) + command number (4 bytes).
        :param validate_checksum: Set to True to validate the checksum. Set to False to skip validation. Defaults to True.

        :return: Binary4 (8206HR data) POD packet.
        """

        # get prepacket + packet number, TTL, and binary ch0-2 (these are all binary, do not search for STX/ETX) + read csm and ETX (3 bytes) (these are ASCII, so check for STX/ETX)
        packet = pre_packet + self._port.read(8) + self._read_to_etx(validate_checksum=validate_checksum)
        # check if checksum is correct 
        if(validate_checksum):
            if(not self._validate_checksum(packet) ) :
                raise Exception('Bad checksum for binary POD packet read.')
        
        # return complete variable length binary packet
        return DataPacket8206HR(packet, self._preamp_gain)

    # Fixed size of 8206HR binary data packet: STX(1) + cmd(4) + payload(8) + checksum(2) + ETX(1) = 16 bytes
    _STREAMING_PACKET_LEN = 16

    # def _read_exactly_n_streaming(self, n: int, timeout_sec: float) -> bytes | None:
    #     """Read exactly n bytes, accumulating partial reads until we have n or hit timeout."""
    #     data = b''
    #     deadline = time.perf_counter() + timeout_sec
    #     while len(data) < n:
    #         remaining = max(0.01, deadline - time.perf_counter())
    #         if remaining <= 0:
    #             return None
    #         chunk = self._port.read(n - len(data), remaining)
    #         if chunk:
    #             data = data + chunk
    #         elif len(data) == 0:
    #             return None
    #     return data if len(data) == n else None

    def read_pod_packet_streaming(self, timeout_sec: float = 0.1, validate_checksum: bool = True):
        """Read one packet (data or control) using a single read(16) when aligned. Use in streaming mode for higher throughput.

        When a fixed-size block is a complete data packet (16 bytes, ends with ETX), returns DataPacket8206HR.
        When the block starts with STX but does not end with ETX (e.g. control packet), reads to ETX, parses as
        ControlPacket, and returns it so the pipeline can deliver it to read_queue for mixed traffic.
        Accumulates partial reads so driver returning fewer than 16 bytes does not cause timeout.
        Raises TimeoutError if no data in timeout_sec.
        """
        if self._port is None:
            raise TypeError("PortIO object does not exist!")
        n = Pod8206HR._STREAMING_PACKET_LEN
        while True:
            data = self._port.read(n, timeout_sec)
            if data is None or len(data) < n:
                raise TimeoutError("No data received from device within timeout (streaming read)")
            if data[0:1] != PodPacket.STX:
                while True:
                    b = self._port.read(1, timeout_sec)
                    if b is None or len(b) == 0:
                        raise TimeoutError("No data received from device within timeout (streaming sync)")
                    if b == PodPacket.STX:
                        break
                rest = self._port.read(n - 1, timeout_sec)
                if rest is None or len(rest) < n - 1:
                    raise TimeoutError("No data received from device within timeout (streaming read after sync)")
                data = b + rest
            if data[-1:] != PodPacket.ETX:
                # Variable-length packet (e.g. control) - read to ETX and try to deliver as ControlPacket
                while data[-1:] != PodPacket.ETX:
                    b = self._port.read(1, timeout_sec)
                    if b is None or len(b) == 0:
                        raise TimeoutError("No data received from device within timeout (streaming read to ETX)")
                    data = data + b
                if validate_checksum and not self._validate_checksum(data):
                    continue  # discard bad packet and retry
                try:
                    return self._control_packet_factory(data)
                except Exception:
                    continue  # not a valid control packet, discard and retry
            if validate_checksum and not self._validate_checksum(data):
                raise Exception("Bad checksum for binary POD packet read (streaming).")
            return DataPacket8206HR(data, self._preamp_gain)

    def get_dict(self):
        d = {
            'port': self.port,
            'preamp_gain': self.preamp_gain,
            'baudrate': self.baudrate,
            'device_name': self.device_name,
            'use_d2xx': self._use_d2xx,
        }
        if self._sample_rate is not None:
            d['sample_rate'] = self._sample_rate[0]
        return d

    def _check_preamp_gain(self, value: object) -> None:
        if value not in (10, 100):
            raise ValueError("preamp_gain must be 10 or 100")

    def _check_sample_rate(self, value: object) -> None:
        if value < 100 or value > 2_000:
            raise ValueError("sample_rate must be >= 100 and <= 2,000")

    @property
    def param_schema(self):
        return ParamSchema(
            required=frozenset(
                {
                    "preamp_gain",
                    }
                ),
            optional=frozenset(
                {
                    "sample_rate",
                    "lowpass_ch0",
                    "lowpass_ch1",
                    "lowpass_ch2",
                    "ttl_pin0",
                    "ttl_pin1",
                    "ttl_pin2",
                    "ttl_pin3",
                }
            ),
            validators={
                "preamp_gain": self._check_preamp_gain,
                'sample_rate': self._check_sample_rate,
                },
        )
