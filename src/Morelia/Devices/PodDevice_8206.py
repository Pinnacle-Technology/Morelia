"""Support for the original 8206 (4100), device type 1."""

import math
import time

from Morelia.Devices import AcquisitionDevice, Pod
from Morelia.packet.data import DataPacket8206


class Pod8206(AcquisitionDevice):
    """Three-channel legacy EEG/EMG acquisition device.

    Configure the hardware while stopped. Gain indices select multipliers
    (1, 2, 4, 5, 8, 10, 16, 20); lowpass values in SET CONFIG are hundredths
    of Hz. Optional sample_rate is a cached value for worker reconstruction;
    use the sample_rate property to change the hardware rate.
    """

    SAMPLE_RATES = (200, 400, 600, 800, 1000, 2000)

    def __init__(self, port: str | int, preamp_gain: float, baudrate: int = 9600,
                 device_name: str | None = None, use_d2xx: bool = False,
                 sample_rate: int | None = None, eeg_gain_index: int = 1,
                 emg_gain_index: int = 1):
        if not math.isfinite(preamp_gain) or preamp_gain <= 0:
            raise ValueError("preamp_gain must be positive and finite")
        self._validate_settings(eeg_gain_index, emg_gain_index, sample_rate)
        super().__init__(port, 2000, baudrate, device_name, use_d2xx=use_d2xx)
        self._preamp_gain = preamp_gain
        self._eeg_gain_index = eeg_gain_index
        self._emg_gain_index = emg_gain_index
        self._sample_rate = None if sample_rate is None else (sample_rate,)
        self._receive_buffer = bytearray()
        u8, u16 = Pod.get_u(8), Pod.get_u(16)
        self._commands.remove_command(8)
        self._commands.add_command(8, 'TYPE', (0,), (u16,), False,
                                   'Legacy device type (16-bit response)')
        self._commands.remove_command(5)
        self._commands.remove_command(10)
        self._commands.remove_command(11)
        self._commands.add_command(11, 'BINARY', (0,), (u16,), True,
                                   'Length-prefixed binary sample batch')
        commands = (
            (5, 'STATUS', (0,), (u8,) * 6 + (u16, u8, u16, u8, u16, u8, u8, u8, u16)),
            (100, 'SET TIME', (u8,) * 6, (0,)),
            (101, 'SET CONFIG', (u16, u8, u16, u8, u16), (0,)),
            # 102-111 are reserved filter/gain/rate IDs in the C++ enum;
            # p8206 registers configuration through STATUS/SET CONFIG only.
            (112, 'SET TTL OUT', (u8,), (0,)),
            (113, 'GET TTL OUT', (0,), (u8,)),
            (114, 'GET TTL IN', (0,), (u8,)),
            (115, 'ENABLE TTL EVENTS', (u8,), (0,)),
            (116, 'GET ENABLE TTL EVENTS', (0,), (u8,)),
            (117, 'EVENT TTL IN 1', (0,), (u8, u8)),
            (118, 'ENABLE DEBOUNCE', (u8,), (0,)),
            (119, 'GET ENABLE DEBOUNCE', (0,), (u8,)),
            (32769, 'EVENT TTL IN', (0,), (u8,)),
        )
        for number, name, args, result in commands:
            self._commands.add_command(number, name, args, result, False, name)

    @staticmethod
    def _validate_settings(eeg_gain_index, emg_gain_index, sample_rate):
        for index in (eeg_gain_index, emg_gain_index):
            if not isinstance(index, int) or not 0 <= index < 8:
                raise ValueError("Gain indices must be integers from 0 through 7")
        if sample_rate is not None and sample_rate not in Pod8206.SAMPLE_RATES:
            raise ValueError(f"Sample rate must be one of {Pod8206.SAMPLE_RATES}")

    @property
    def preamp_gain(self):
        return self._preamp_gain

    def read_configuration(self) -> tuple:
        """Read STATUS; return EEG lowpass/gain, EMG lowpass/gain, rate.

        Lowpass values are returned in hundredths of Hz, as on the wire.
        Also refresh the gains used to convert incoming samples.
        """
        status = self.write_read('STATUS').payload
        config = status[6:11]
        self._validate_settings(config[1], config[3], config[4])
        self._eeg_gain_index, self._emg_gain_index = config[1], config[3]
        self._sample_rate = (config[4],)
        return config

    def configure(self, eeg_lowpass: int, eeg_gain_index: int,
                  emg_lowpass: int, emg_gain_index: int, sample_rate: int):
        """Set hardware configuration; lowpass arguments are hundredths of Hz."""
        self._validate_settings(eeg_gain_index, emg_gain_index, sample_rate)
        self.write_read('SET CONFIG', (eeg_lowpass, eeg_gain_index,
                                      emg_lowpass, emg_gain_index, sample_rate))
        self._eeg_gain_index, self._emg_gain_index = eeg_gain_index, emg_gain_index
        self._sample_rate = (sample_rate,)

    @property
    def sample_rate(self):
        if self._sample_rate is None:
            self.read_configuration()
        return self._sample_rate[0]

    @sample_rate.setter
    def sample_rate(self, rate):
        self._validate_settings(self._eeg_gain_index, self._emg_gain_index, rate)
        config = self.read_configuration()
        self.configure(*config[:4], rate)

    def __enter__(self):
        if self._port is None:
            self.open_port()
        self.read_configuration()
        return super().__enter__()

    def write_read(self, cmd, payload=None, validate_checksum=True, timeout_sec=5):
        """Wait for the requested response, ignoring interleaved TTL events."""
        if self._port is None:
            return super().write_read(cmd, payload, validate_checksum, timeout_sec)
        number = self._commands.command_number_from_name(cmd) if isinstance(cmd, str) else cmd
        self.flush_port()
        self._receive_buffer.clear()
        self.write_packet(cmd, payload)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            packet = self.read_pod_packet(validate_checksum, deadline - time.monotonic())
            if packet.command_number == number:
                return packet
            if packet.command_number in (1, 4):
                raise RuntimeError(f'8206 rejected command {number}: {packet.raw_packet!r}')
        raise TimeoutError(f'No response to 8206 command {number}')

    def read_pod_packet(self, validate_checksum=True, timeout_sec=5):
        """Read mixed control/binary traffic, retaining partial reads on timeout."""
        if self._port is None:
            raise TypeError("PortIO object does not exist!")
        deadline = time.monotonic() + timeout_sec
        buf = self._receive_buffer

        def fill(size):
            while len(buf) < size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Incomplete 8206 packet")
                # Serial PortIO raises on short reads and discards their bytes.
                # Single-byte reads preserve fragments there; D2XX returns
                # partial blocks and can use the more efficient batch read.
                count = size - len(buf) if self._use_d2xx else 1
                chunk = self._port.read(count, remaining)
                if not chunk:
                    raise TimeoutError("No data received from 8206")
                buf.extend(chunk)

        while True:
            fill(1)
            if buf[0] == 2:
                break
            del buf[0]
        fill(5)
        try:
            command = int(bytes(buf[1:5]), 16)
        except ValueError:
            del buf[0]
            raise ValueError("Invalid 8206 command header")
        if command == 11:
            fill(12)
            header = bytes(buf[:12])
            if header[11] != 3 or not self._validate_checksum(header):
                del buf[0]
                raise ValueError("Invalid 8206 binary header/checksum")
            size = int(header[5:9], 16)
            if not size or size % 6:
                del buf[:12]
                raise ValueError("Invalid 8206 binary payload length")
            fill(size + 15)
            raw = bytes(buf[:size + 15])
            del buf[:size + 15]
            if raw[-1] != 3:
                raise ValueError("Invalid 8206 binary terminator")
            if validate_checksum and Pod.checksum(raw[12:-3]) != raw[-3:-1]:
                raise ValueError("Bad checksum for 8206 binary payload")
            return DataPacket8206(raw, self.preamp_gain,
                                  self._eeg_gain_index, self._emg_gain_index)
        while 3 not in buf[5:]:
            fill(len(buf) + 1)
        end = buf.index(3, 5) + 1
        raw = bytes(buf[:end])
        del buf[:end]
        if validate_checksum and not self._validate_checksum(raw):
            raise ValueError("Bad checksum for 8206 control packet")
        return self._control_packet_factory(raw)

    def read_pod_packet_streaming(self, timeout_sec=0.2, validate_checksum=True):
        return self.read_pod_packet(validate_checksum, timeout_sec)

    def get_dict(self):
        return dict(port=self._port_value, preamp_gain=self.preamp_gain,
                    baudrate=self.baudrate, device_name=self.device_name,
                    use_d2xx=self._use_d2xx,
                    sample_rate=None if self._sample_rate is None else self._sample_rate[0],
                    eeg_gain_index=self._eeg_gain_index, emg_gain_index=self._emg_gain_index)
