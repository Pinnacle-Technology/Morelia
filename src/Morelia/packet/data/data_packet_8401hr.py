from Morelia.packet.data.data_packet import DataPacket
from Morelia.packet import PrimaryChannelMode, SecondaryChannelMode
from Morelia.signal import DigitalSignal

import Morelia.packet.conversion as conv

class DataPacket8401HR(DataPacket):
    """
    This class handles decoding 8401HR data packets (previously known as Binary5 packets). It is optimized to be as effectient as possible
    for quick streaming by implementing lazy decoding via properties, slots, and memoization. On a binary level, data packets for the 8401HR look as
    follows (this packet is big, so you may need to zoom in... sorry about that 😭):

    .. image:: _static/data_packet_8401hr.png
    
    There are a couple things to note. Firstly, you may see that some
    of the byte indices are repeated between the segements containing
    the channels. That is because each 18-bit channel value is packed over 3 bytes. Meaning, for example, that bytes 7 and 8 contain entirely
    data related to CH3's value but byte 9 is *mixed*. The first two bits
    contain the 2 least significant bits of CH3's value, and the
    remaining six bits contain the six most significant bits of CH2's value.

    Secondly, you may notice we capture both analog and digital information for each secondary channel. Based on the ``secondary channel modes`` passed to the packet constructor, we will present the proper value read (analog or digital) to the user.

    In terms of reading the digital status values, the byte is decoded as follows:

    .. image:: _static/digital_byte_8401hr.png

    The raw packet and several values from the device (with the same names as the parameters)
    needed for calculations are passed to the constructor. Outside
    of testing, users should never be instantiating this class directly,
    that should be limited to instances of the ``Pod8401HR`` class.

    :param preamp_gain: Tuple storing the pramplifier gain for all four channels. 
    :param ss_gain: Tuple storing the second-stage gain for all four channels.
    :param primary_channel_mode: A tuple containing the mode of operation for each primary channel (EEG/EMG or Biosensor).
    :param secondary_channel_modes: A tuple containing the mode of operation for each secondary (TTL/AEXT) channel (analog or digital).
    :param raw_packet: Raw bytes of packet read from device.
    """

    
    __slots__  = ('_ss_gain', '_preamp_gain', '_primary_channel_modes', '_secondary_channel_modes', '_channel_invert', '_ch0', '_ch1', '_ch2', '_ch3', '_ext0', '_ext1', '_ttl1', '_ttl2', '_ttl3', '_ttl4')
    def __init__(self, preamp_gain: tuple[int], ss_gain: tuple[int], 
                 primary_channel_modes: tuple[PrimaryChannelMode], secondary_channel_modes: tuple[SecondaryChannelMode],
                 raw_packet: bytes,
                 channel_invert: tuple[bool, ...] = (False, False, False, False)) -> None:

        super().__init__(raw_packet, 31)
            
        self._preamp_gain = preamp_gain
        self._ss_gain = ss_gain
        self._primary_channel_modes = primary_channel_modes
        self._secondary_channel_modes = secondary_channel_modes
        self._channel_invert = channel_invert

        self._raw_packet = raw_packet

        self._ch0 = None
        self._ch1 = None
        self._ch2 = None
        self._ch3 = None
        self._ch0 = None

        self._ext0 = None
        self._ext1 = None

        self._ttl1 = None
        self._ttl2 = None
        self._ttl3 = None
        self._ttl4 = None

    @property
    def packet_counter(self) -> int:
        """Candidate 8-bit sequence byte; streaming verifies it before use."""
        if len(self._raw_packet) < self._min_length:
            raise ValueError("Truncated 8401HR data packet")
        return self._raw_packet[5]

    @property
    def ch0(self) -> int:
        """:return: Value read from channel 0."""
        if self._ch0 is None:
            val = DataPacket8401HR.get_primary_channel_value(self._primary_channel_modes[0], self._preamp_gain[0], self._ss_gain[0], conv.binary_bytes_to_int_split(self._raw_packet[7:16][6:9], 18, 0))
            self._ch0 = -val if self._channel_invert[0] else val

        return self._ch0

    @property
    def ch1(self) -> int:
        """:return: Value read from channel 1."""
        if self._ch1 is None:
            val = DataPacket8401HR.get_primary_channel_value(self._primary_channel_modes[1], self._preamp_gain[1], self._ss_gain[1], conv.binary_bytes_to_int_split(self._raw_packet[7:16][4:7], 20, 2))
            self._ch1 = -val if self._channel_invert[1] else val

        return self._ch1

    @property
    def ch2(self) -> int:
        """:return: Value read from channel 2."""
        if self._ch2 is None:
            val = DataPacket8401HR.get_primary_channel_value(self._primary_channel_modes[2], self._preamp_gain[2], self._ss_gain[2], conv.binary_bytes_to_int_split(self._raw_packet[7:16][2:5], 22, 4))
            self._ch2 = -val if self._channel_invert[2] else val

        return self._ch2

    @property
    def ch3(self) -> int:
        """:return: Value read from channel 3."""
        if self._ch3 is None:
            val = DataPacket8401HR.get_primary_channel_value(self._primary_channel_modes[3], self._preamp_gain[3], self._ss_gain[3], conv.binary_bytes_to_int_split(self._raw_packet[7:16][0:3], 24, 6))
            self._ch3 = -val if self._channel_invert[3] else val

        return self._ch3

    @property
    def ext0(self) -> int | DigitalSignal:
        """:return: Value read from EXT0."""
        if self._ext0 is None:
            raw_value: int = self._raw_packet[6] & 0x80 if self._secondary_channel_modes[0] is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[16:18])
            self._ext0 = self.get_secondary_channel_value(self._secondary_channel_modes[0], raw_value)

        return self._ext0

    @property
    def ext1(self) -> int | DigitalSignal:
        """:return: Value read from EXT1."""
        if self._ext1 is None:
            raw_value: int = self._raw_packet[6] & 0x40 if self._secondary_channel_modes[1] is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[18:20])
            self._ext1 = self.get_secondary_channel_value(self._secondary_channel_modes[1], raw_value)

        return self._ext1

    @property
    def ttl1(self) -> int | DigitalSignal:
        """:return: Value read from TTL1."""

        if self._ttl1 is None:
            raw_value: int = self._raw_packet[6] & 0x01 if self._secondary_channel_modes[2]  is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[20:22])
            self._ttl1 = self.get_secondary_channel_value(self._secondary_channel_modes[2], raw_value)

        return self._ttl1

    @property
    def ttl2(self) -> int | DigitalSignal:
        """:return: Value read from TTL2."""

        if self._ttl2 is None:
            raw_value: int = self._raw_packet[6] & 0x02 if self._secondary_channel_modes[3] is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[22:24])
            self._ttl2 = self.get_secondary_channel_value(self._secondary_channel_modes[3], raw_value)

        return self._ttl2

    @property
    def ttl3(self) -> int | DigitalSignal:
        """:return: Value read from TTL3."""

        if self._ttl3 is None:
            raw_value: int = self._raw_packet[6] & 0x04 if self._secondary_channel_modes[4] is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[24:26])
            self._ttl3 = self.get_secondary_channel_value(self._secondary_channel_modes[4], raw_value)

        return self._ttl3

    @property
    def ttl4(self) -> int | DigitalSignal:
        """:return: Value read from TTL4."""

        if self._ttl4 is None:
            raw_value: int = self._raw_packet[6] & 0x08 if self._secondary_channel_modes[5] is SecondaryChannelMode.DIGITAL else conv.binary_bytes_to_int(self._raw_packet[26:28])
            self._ttl4 = self.get_secondary_channel_value(self._secondary_channel_modes[5], raw_value)

        return self._ttl4

    @staticmethod
    def get_primary_channel_value(channel_mode: PrimaryChannelMode, preamp_gain: int | None, ss_gain: int | None, raw_value: int) -> int:
        """Channel values from the data packet cannot be used directly, we must preform some math on them to get them to be real,
        usable values. This function is used **by the properties** to calcuate this when a channel value is asked for. 
        **This method is used by the properties internally, therefore it does not need to be called when acessing their value outside of this class.**

        When ss_gain or preamp_gain is None (e.g. no-connect channel), a default is used so that the formula does not raise.
        :meta private:
        """
        # Use defaults when gain is None (allowed for no-connect channels) to avoid TypeError in gain math
        if preamp_gain is None:
            preamp_gain = 10
        if ss_gain is None:
            ss_gain = 5
        match channel_mode:
            case PrimaryChannelMode.EEG_EMG:
                voltage_at_ADC = (raw_value / 262144.0) * 4.096
                total_gain    = 10.0 * ss_gain * preamp_gain # SSGain = 1 or 5, PreampGain = 10 or 100
                real_voltage  = (voltage_at_ADC - 2.048) / total_gain # V
                return round( real_voltage * 1E6, 12)

            case PrimaryChannelMode.BIOSENSOR:
                voltage_at_ADC = (raw_value / 262144.0) * 4.096 # V
                total_gain    = 1.557 * ss_gain * 1E7 # SSGain = 1 or 5
                real_voltage  = (voltage_at_ADC - 2.048) / total_gain # V
                return round( real_voltage * 1E6, 12)

        raise ValueError('Inavlid channel mode!')
    
    @staticmethod
    def get_secondary_channel_value(channel_mode: SecondaryChannelMode, raw_value: int) -> int | DigitalSignal:
        """Channel values from the data packet cannot be used directly, we must preform some math on them to get them to be real,
        usable values. This function is used **by the properties** to calcuate this when a channel value is asked for. 
        **This method is used by the properties internally, therefore it does not need to be called when acessing their value outside of this class.**

        :meta private:
        """

        match channel_mode:
            case SecondaryChannelMode.DIGITAL:
                return DigitalSignal.LOW if raw_value == 0 else DigitalSignal.HIGH

            case SecondaryChannelMode.ANALOG:
                return round(  ( (raw_value / 4096.0) * 3.3 ) * 1E6, 12)  # V

        raise ValueError('Inavlid channel mode!')


