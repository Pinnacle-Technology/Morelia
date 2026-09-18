from Morelia.packet.data.data_packet import DataPacket
from Morelia.signal import DigitalSignal

import Morelia.packet.conversion as conv

class DataPacket8206HR(DataPacket):
    """
    This class handles decoding 8206HR data packets (previously known as Binary4 packets). It is optimized to be as effectient as possible
    for quick streaming by implementing lazy decoding via properties,
    slots, and memoization. On a binary level, data packets for the 8206HR look as
    follows:

    .. image:: _static/data_packet_8206hr.png

    
    The raw packet and several values from the device (with the same names as the parameters)
    needed for calculations are passed to the constructor. Outside
    of testing, users should never be instantiating this class directly,
    that should be limited to instances of the ``Pod8206HR`` class.

    :param raw_packet: Raw bytes of packet read from device.
    :preamp_gain: Desired preamplifer gain. Used for decoding read values from device. Must be 10 or 100.
    """

    __slots__ = ('_ch0', '_ch1', '_ch2', '_ttl1', '_ttl2', '_ttl3', '_ttl4')
    def __init__(self, raw_packet: bytes, preamp_gain: int) -> None:
        self._preamp_gain = preamp_gain
        super().__init__(raw_packet, 16)

        self._ch0 = None
        self._ch1 = None
        self._ch2 = None

        self._ttl1 = None
        self._ttl2 = None
        self._ttl3 = None
        self._ttl4 = None
    
    @property
    def packet_counter(self) -> int:
        """8-bit acquisition packet sequence (payload byte zero)."""
        if len(self._raw_packet) < self._min_length:
            raise ValueError("Truncated 8206HR data packet")
        return self._raw_packet[5]

    @property
    def ch0(self) -> int:
        """:return: Value read from channel 0."""
        if self._ch0 is None:
            self._ch0 = DataPacket8206HR.get_primary_channel_value(self._raw_packet[7:9], self._preamp_gain)
        return self._ch0

    @property
    def ch1(self) -> int:
        """:return: Value read from channel 1."""
        if self._ch1 is None:
            self._ch1 = DataPacket8206HR.get_primary_channel_value(self._raw_packet[9:11], self._preamp_gain)
        return self._ch1

    @property
    def ch2(self) -> int:
        """:return: Value read from channel 2."""
        if self._ch2 is None:
            self._ch2 = DataPacket8206HR.get_primary_channel_value(self._raw_packet[11:13], self._preamp_gain)
        return self._ch2
 
    @property
    def ttl1(self) -> DigitalSignal:
        """:return: The first bit of the TTL byte."""
        if self._ttl1 is None:
            self._ttl1 = DigitalSignal.LOW if self._raw_packet[6] & 0x80 == 0 else DigitalSignal.HIGH
        return self._ttl1

    @property
    def ttl2(self) -> DigitalSignal:
        """:return: The second bit of the TTL byte."""
        if self._ttl2 is None:
            self._ttl2 = DigitalSignal.LOW if self._raw_packet[6] & 0x40 == 0 else DigitalSignal.HIGH
        return self._ttl2

    @property
    def ttl3(self) -> DigitalSignal:
        """:return: The third bit of the TTL byte."""
        if self._ttl3 is None:
            self._ttl3 = DigitalSignal.LOW if self._raw_packet[6] & 0x20 == 0 else DigitalSignal.HIGH
        return self._ttl3

    @property
    def ttl4(self) -> DigitalSignal:
        """:return: The fourth bit of the TTL byte."""
        if self._ttl4 is None:
            self._ttl4 = DigitalSignal.LOW if self._raw_packet[6] & 0x10 == 0 else DigitalSignal.HIGH
        return self._ttl4

    @staticmethod
    def get_primary_channel_value(raw_value: bytes, preamp_gain: int) -> float:
        """Channel values from the data packet cannot be used directly, we must preform some math on them to get them to be real,
        usable values. This function is used **by the properties** to calcuate this when a channel value is asked for. 
        **This method is used by the properties internally, therefore it does not need to be called when acessing their value outside of this class.**

        :meta private:
        """
        # calculate voltage 
        value = conv.binary_bytes_to_int(raw_value, conv.Endianness.LITTLE)
        voltage_adc = ( value / 65535.0 ) * 4.096 # V
        total_gain = preamp_gain * 50.2918
        # Guard against division by zero if preamp_gain is invalid
        if total_gain == 0:
            total_gain = 10 * 50.2918  # Default to preamp_gain=10 if invalid
        real_voltage = ( voltage_adc - 2.048 ) / total_gain
        return round(real_voltage * 1E6, 12)

