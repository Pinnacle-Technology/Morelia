from Morelia.packet.data.data_packet import DataPacket
from typing import List

import Morelia.packet.conversion as conv

class DataPacket8274D(DataPacket):
    '''
    This class handles decoding 8274D data packets. On a binary level, data 
    packets for the 8206HR look as follows:

    .. image:: _static/data_packet_8274D.png


    The raw packet and several values from the device (with the same names as the parameters)
    needed for calculations are passed to the constructor. Outside
    of testing, users should never be instantiating this class directly,
    that should be limited to instances of the ``Pod8274D`` class.

    :param raw_packet: Raw bytes of packet read from device.
    :primary_gain: Desired primary gain. Use 100 for 8274-SL, 8274-SE, 8274-SE3, and 8274-E
    :secondary_gain: Desired secondary gain. Use 26 for 8274-SL, 8274-SE, and 8274-SE3. Use 13 for 8274-E.
    '''

    __slots__ = ('_ch5', '_ch6', '_ch7')
    def __init__(self, raw_packet: bytes, primary_gain: int = 100, secondary_gain: int = 26) -> None:
        self._primary_gain = primary_gain
        self._secondary_gain = secondary_gain
        super().__init__(raw_packet=raw_packet, min_length=259)

        # Lists to hold sample voltage values for each channel
        self._ch5 = []
        self._ch6 = []
        self._ch7 = []
        
        # Loop through each sample in the payload
        for i in range(16, 256, 2):
            # Get the 2 bytes for this sample
            two_bytes = raw_packet[i:i+2]
            
            # Channel is in the first 4 bits of the 16-bit sample value
            # For big-endian bytes, those are bits 12-15 of the full 16-bit integer
            channel_number = conv.binary_bytes_to_int_split(
                msg=two_bytes,
                msb_index=16,
                lsb_index=12,
                byteorder=conv.Endianness.LITTLE
            )
            
            # The remaining 12 bits are the ADC value: bits 0-11 of the 16-bit int
            sample_12bit_ADC = conv.binary_bytes_to_int_split(
                msg=two_bytes,
                msb_index=12,
                lsb_index=0,
                byteorder=conv.Endianness.LITTLE
            )

            # Convert ADC value to voltage
            sample_volts = DataPacket8274D.get_primary_channel_value(value=sample_12bit_ADC)

            # Add the sample voltage reading to the respective list
            if channel_number == 5:
                self._ch5.append(sample_volts)
            elif channel_number == 6:
                self._ch6.append(sample_volts)
            elif channel_number == 7:
                self._ch7.append(sample_volts)

    @property
    def packet_counter(self) -> int:
        """16-bit little-endian EEG batch sequence following the binary header."""
        if len(self._raw_packet) < self._min_length:
            raise ValueError("Truncated 8274D data packet")
        return int.from_bytes(self._raw_packet[12:14], "little")

    @property
    def ch5(self) -> List[int]:
        """:return: Values list from channel 5."""
        return self._ch5

    @property
    def ch6(self) -> List[int]:
        """:return: Values list from channel 6."""
        return self._ch6

    @property
    def ch7(self) -> List[int]:
        """:return: Values list from channel 7."""
        return self._ch7

    @staticmethod
    def get_primary_channel_value(value: bytes, primary_gain: int = 100, secondary_gain: int = 26) -> float:
        """
        Channel values from the data packet cannot be used directly, we must preform some math on them to get them to be real,
        usable values. This function is used **by the properties** to calcuate this when a channel value is asked for. 
        **This method is used by the properties internally, therefore it does not need to be called when acessing their value outside of this class.**

        :meta private:
        """

        adc_voltage = (2.5 / 4096) * value
        input_voltage = adc_voltage - 1.25

        real_voltage = input_voltage / (primary_gain * secondary_gain)
        
        return round(real_voltage * 1E6, 12)
