import random

import Morelia.packet.conversion as conv

from typing import List

from tests.mocks.packet.MockPacket import MockPacket

class MockDataPacket8274D(MockPacket):
    """
    Mock 8274D Data Packet Generator

    This class simulates an 8274D hardware data packet for testing purposes.
    It generates synthetic channel data (ch5, ch6, ch7) and encodes it into
    a raw binary packet format compatible with `DataPacket8274D`.

    The mock is designed to replace hardware input during unit tests,
    integration tests, and simulation environments.

    ------------------------------------------------------------
    Key Features
    ------------------------------------------------------------
    - Generates realistic ADC channel data for ch5, ch6, and ch7
    - Supports deterministic output via fixed input or random generation
    - Encodes data using the same protocol utilities as production code
    - Produces fully valid raw packets compatible with `DataPacket8274D`

    ------------------------------------------------------------
    Channel Data
    ------------------------------------------------------------
    - Each channel contains 40 samples by default
    - Default ADC range: 1900–2200
    - Channels represent:
        * ch5 → Channel 5 ADC values
        * ch6 → Channel 6 ADC values
        * ch7 → Channel 7 ADC values

    ------------------------------------------------------------
    Packet Format
    ------------------------------------------------------------
    The generated binary packet follows the 8274D protocol structure:

        STX
        Command Number (11)
        Payload Length (244)
        Header Checksum
        ETX
        Counter (2 bytes)
        Timestamp (2 bytes)
        Interleaved Channel Samples (40 per channel)
        Data Checksum
        ETX

    Each sample encodes:
        - 4-bit channel identifier (5, 6, or 7)
        - 12-bit ADC value

    ------------------------------------------------------------
    Notes
    ------------------------------------------------------------
    - Intended for testing and simulation only
    - Not used in production firmware
    - Output is fully compatible with `DataPacket8274D`
    """

    NUM_SAMPLES = 40 # Number of samples per packet of data from device

    def __init__(self, ch5: List=None, ch6: List=None, ch7: List=None, seed: int=None, packet_number: int=1234):
        rng = random.Random(seed)
        self.packet_number = packet_number & 0xFFFF

        self.ch5 = ch5 if ch5 is not None else [
            rng.randint(1900, 2200) for _ in range(self.NUM_SAMPLES)
        ]
        self.ch6 = ch6 if ch6 is not None else [
            rng.randint(1900, 2200) for _ in range(self.NUM_SAMPLES)
        ]
        self.ch7 = ch7 if ch7 is not None else [
            rng.randint(1900, 2200) for _ in range(self.NUM_SAMPLES)
        ]

    def to_bytes(self):
        stx = b'\x02'
        etx = b'\x03'

        N = self.NUM_SAMPLES

        command_number_bytes = conv.int_to_ascii_bytes(11, 4) # Always 11 for binary
        payload_length_bytes = conv.int_to_ascii_bytes(244, 4)
        
        header = command_number_bytes + payload_length_bytes
        header_checksum = self.calculate_checksum(header)

        counter = self.packet_number.to_bytes(2, "little")
        timestamp = conv.int_to_binary_bytes(17823, 2)

        payload = counter + timestamp

        for i in range(N):
            payload += conv.ints_to_binary_bytes_split(
                fields=[(5, 16, 12), (self.ch5[i], 12, 0)],
                msg_len_bytes=2,
                byteorder=conv.Endianness.LITTLE,
            )

            payload += conv.ints_to_binary_bytes_split(
                fields=[(6, 16, 12), (self.ch6[i], 12, 0)],
                msg_len_bytes=2,
                byteorder=conv.Endianness.LITTLE,
            )

            payload += conv.ints_to_binary_bytes_split(
                fields=[(7, 16, 12), (self.ch7[i], 12, 0)],
                msg_len_bytes=2,
                byteorder=conv.Endianness.LITTLE,
            )

        data_checksum = self.calculate_checksum(payload)
        
        test_packet_bytes: bytes = stx + command_number_bytes + payload_length_bytes + header_checksum + etx + payload + data_checksum + etx

        return test_packet_bytes
