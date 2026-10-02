"""Legacy 8206 variable-length, signed ADC sample batches."""

import math
import struct

from Morelia.packet.data.data_packet import DataPacket


class DataPacket8206(DataPacket):
    """A BINARY (11) header followed by interleaved EEG1, EEG2, EMG samples.

    Layout: STX + ``000B`` + four hex length digits + header checksum + ETX,
    then N binary bytes + binary checksum + ETX. Each sample is three
    little-endian signed 16-bit words containing 14-bit ADC readings.
    ``ch0``, ``ch1`` and ``ch2`` are tuples of microvolt values, one per sample.
    TTL changes arrive separately as control events, not in this payload.
    """

    GAIN_SETTINGS = (1, 2, 4, 5, 8, 10, 16, 20)

    def __init__(self, raw_packet: bytes, preamp_gain: float,
                 eeg_gain_index: int = 1, emg_gain_index: int = 1):
        if not math.isfinite(preamp_gain) or preamp_gain <= 0:
            raise ValueError("preamp_gain must be positive and finite")
        for index in (eeg_gain_index, emg_gain_index):
            if not isinstance(index, int) or not 0 <= index < len(self.GAIN_SETTINGS):
                raise ValueError("Gain indices must be integers from 0 through 7")
        if (len(raw_packet) < 21 or raw_packet[:5] != b'\x02000B'
                or raw_packet[11:12] != self.ETX or raw_packet[-1:] != self.ETX):
            raise ValueError("Malformed 8206 binary packet")
        size = int(raw_packet[5:9], 16)
        if size == 0 or size % 6 or len(raw_packet) != size + 15:
            raise ValueError("8206 payload must contain complete three-channel samples")
        super().__init__(raw_packet, 21)
        self.sample_count = size // 6
        # Matches Sirenia AnalogProperties::ConvertDigitalToAnalog(sint32):
        # (code + 0.5) * ((2.048 - -2.048) / 2**14) / gain.
        scales = tuple(4.096 / 16384 / (preamp_gain * self.GAIN_SETTINGS[i] * 50.78e-6)
                       for i in (eeg_gain_index, eeg_gain_index, emg_gain_index))
        rows = tuple(struct.iter_unpack('<hhh', raw_packet[12:-3]))
        self.ch0, self.ch1, self.ch2 = (
            tuple((row[ch] + 0.5) * scales[ch] for row in rows) for ch in range(3)
        )
