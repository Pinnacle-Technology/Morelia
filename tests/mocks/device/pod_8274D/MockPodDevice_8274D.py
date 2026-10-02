import threading
import time

from collections import deque

import Morelia.packet.conversion as conv

from Morelia.Commands import CommandSet
from Morelia.packet.pod_packet import PodPacket
from Morelia.packet.control_packet import ControlPacket
from Morelia.packet.data.data_packet_8274D import DataPacket8274D
from Morelia.Devices.PodDevice_8274D import Pod8274D

from tests.mocks.packet.data.MockDataPacket8274D import MockDataPacket8274D
from tests.mocks.packet.MockControlPacket import MockControlPacket
from tests.mocks.port.MockPort import MockPort
from tests.mocks.port.MockQueueManager import MockQueueManager

_BINARY_CMD_NUM = 11

class MockPod8274D(Pod8274D):
    """
    Mock implementation of the Pod8274D device for testing.
    
    This class simulates the behavior of a real 8274-SL pod device by intercepting
    serial communication and generating synthetic responses. Instead of communicating
    with actual hardware, it maintains internal state and responds to commands via
    in-memory queues.
    
    Attributes:
        _port_read_queue (deque): Queue containing bytes to be read by the device
        _connected (bool): Whether a remote Bluetooth device is currently connected
        _scan_enabled (bool): Whether Bluetooth scanning is active
        _stream_counter (int): Counter for tracking data packets
        _devices (list): List of simulated Bluetooth devices available for connection
        _stream_thread (Thread): Thread running the data generation worker
        device_plugged_into_usb (bool): Simulates device USB connection status
        
    Args:
        port (optional): Ignored for mock device
        baudrate (int): Baud rate for serial communication. Default: 921600
        device_name (str, optional): User-defined device name
        device_serial_number (str, optional): Serial number of the device
        scan_timeout_sec (int/float): Timeout for Bluetooth scanning. Default: 15
        sample_rate (int, optional): Sampling rate in Hz (256, 512, or 1024)
    """
    
    # port must be in here for when the obj is recreated from get_dict during sink use
    def __init__(self, port=None, baudrate:int=921600, device_name: str | None = None, device_serial_number: str | None = None, scan_timeout_sec: int|float = 15, sample_rate: int | None = None):
        self._baudrate = baudrate
        self._device_serial_number = device_serial_number
        self._scan_timeout_sec = scan_timeout_sec

        self.model_number = '8274-SL'

        # Default sample rate for device on boot if not set
        self._device_default_sample_rate = 256

        # init _sample_rate
        if sample_rate is not None:
            self._sample_rate = sample_rate
        else:
            self._sample_rate = None

        self._device_type = None

        self._port_read_queue = deque()
        
        self._port = MockPort(read_queue=self._port_read_queue)
        self._port_value = "MOCK_PORT"

        self.device_plugged_into_usb = True # Simulate device plugged into USB or not
        
        self._device_name: str = device_name if device_name else str(self._port) # User selected virtual name (defaults to port name)

        self._manager = MockQueueManager()

        # Simulated BT devices that this 8274D will see
        # These devices can be discovered during LOCAL SCAN and connected via CONNECT command
        self._devices = [
            {
                "slot": 0,
                "bt_address": "A1B2C3D4E5F6",
                "rssi": -42,
                "name": "8274-MOCK1",
            },
            {
                "slot": 1,
                "bt_address": "123456789ABC",
                "rssi": -58,
                "name": "8274-MOCK2",
            },
            {
                "slot": 2,
                "bt_address": "FEDCBA987654",
                "rssi": -71,
                "name": "8274-MOCK3",
            },
        ]

        self._connected = False
        self._scan_enabled = False

        self._connection_slot = None
        self._stream_counter = 0

        self._device_name_from_device = None # Name stored on device, not changeable, (8274-SerialNumber), Set in mock on connecting to device
        
        self._stream_thread = None
        self._stream_stop = threading.Event()

        self._commands : CommandSet = CommandSet()
        self._init_device()

    # CORE MOCK TRANSPORT
    def write_packet(self, cmd, payload=None) -> None:
        """
        Send a command to the mock device and generate a response.
        
        Args:
            cmd (str): Command name (e.g., "RESET", "CONNECT", "STREAM")
            payload (optional): Command payload, depends on command type
        """
        self.device_response(cmd, payload)

    def flush_port(self) -> None:
        """
        Clear all pending packets from the read queue.
        
        This simulates flushing a serial port buffer.
        """
        self._port_read_queue.clear()

    def read_pod_packet(self, timeout_sec=1.0, validate_checksum=True) -> PodPacket:
        """
        Read the next packet from the mock device's response queue.
        
        Blocks until a packet is available or timeout expires. Automatically
        parses the raw bytes into appropriate packet types (DataPacket8274D
        for streaming data, ControlPacket for command responses).
        
        Args:
            timeout_sec (float): Maximum time to wait for a packet in seconds. Default: 1.0
            validate_checksum (bool): Currently unused for mock. Default: True
            
        Returns:
            PodPacket: Either a DataPacket8274D (for streaming) or ControlPacket
            
        Raises:
            TimeoutError: If no packet received within the timeout period
        """
        deadline = time.monotonic() + timeout_sec

        while time.monotonic() < deadline:
            if self._port_read_queue:
                raw_packet = self._port_read_queue.popleft()

                packet = PodPacket(raw_packet)
                cmd_num = packet.command_number

                if cmd_num == _BINARY_CMD_NUM:  # Command 11 for binary meaning data packet
                    return DataPacket8274D(
                        raw_packet=raw_packet,
                        primary_gain=self._primary_gain,
                        secondary_gain=self._secondary_gain
                    )
                else:
                    return ControlPacket(
                        decode_from=self._commands,
                        raw_packet=raw_packet
                    )

            time.sleep(0.01)

        raise TimeoutError("No packet received from mock device within timeout")

    def device_response(self, cmd, payload) -> None:
        """
        Process a command and enqueue appropriate response packet(s).
        
        This is the core command handler. It interprets the command and payload,
        updates internal state as needed, and generates the response packet(s)
        that would come from a real device. Supports commands including:
        - RESET: Device reset
        - LOCAL SCAN: Start/stop Bluetooth scanning
        - DEVICE LIST INFO: Get information about discovered devices
        - CONNECT: Connect to a Bluetooth device
        - DISCONNECT: Disconnect from device
        - GET MODEL NUMBER: Request device model
        - GET/SET SAMPLE RATE: Configure sampling rate
        - GET NAME: Request device name
        - STREAM: Start/stop data streaming
        
        Args:
            cmd (str): Command name
            payload: Command-specific payload
            
        Note:
            If device_plugged_into_usb is False, this method returns without
            processing to simulate a disconnected device.
        """
        if not self.device_plugged_into_usb: # Simulate device is not plugged into USB or not responding
            return
        
        # PING
        if cmd == "PING":
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=2,
                ).to_bytes()
            )
            return

        # RESET
        if cmd == "RESET":
            self.flush_port()
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=3,
                ).to_bytes()
            )
            return

        # LOCAL SCAN
        if cmd == "LOCAL SCAN":
            self._scan_enabled = bool(payload)
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=100,
                    payload=conv.int_to_ascii_bytes(0, 4),
                ).to_bytes()
            )

        # DEVICE LIST INFO
        if cmd == "DEVICE LIST INFO":
            if 0 <= payload < len(self._devices):
                device = self._devices[payload]

                # Build the raw byte representation first (slot + 6-byte address + 1-byte rssi + 16-byte name)
                bt_address_bytes = bytes.fromhex(device["bt_address"])
                rssi_byte = (device["rssi"] + 256) % 256  # consistent 1-byte signed representation
                raw_payload = (
                    bytes([device["slot"]]) +
                    bt_address_bytes +
                    bytes([rssi_byte]) +
                    device["name"].encode("ascii").ljust(16, b"\x00")
                )

                # The command set expects ASCII-encoded hex (two chars per byte). Encode raw bytes
                # into uppercase ASCII hex so existing decoding logic (ascii_bytes_to_int) works.
                payload_bytes = b''.join(f"{b:02X}".encode('ascii') for b in raw_payload)

            else:
                raw_payload = (
                    bytes([payload]) +
                    b"\x00" * 6 +
                    b"\x00" +
                    b"\x00" * 16
                )
                payload_bytes = b''.join(f"{b:02X}".encode('ascii') for b in raw_payload)

            self._port_read_queue.append(
                MockControlPacket(
                    command_number=101,
                    payload=payload_bytes,
                ).to_bytes()
            )
            return

        # CONNECT
        if cmd == "CONNECT":
            if self._connected: # Device already connected
                payload_bytes = conv.int_to_ascii_bytes(0xCFCA, 4) # Connected already
                self._port_read_queue.append(
                    MockControlPacket(
                        command_number=200,
                        payload=payload_bytes,
                    ).to_bytes()
                )
                return

            if self._devices[payload]: # Device exists
                self._connection_slot = payload

                payload_bytes = conv.int_to_ascii_bytes(0xC000, 4) # Connection success
                self._port_read_queue.append(
                    MockControlPacket(
                        command_number=200,
                        payload=payload_bytes,
                    ).to_bytes()
                )

                self._connected = True
                self._device_name_from_device = self._devices[self._connection_slot]['name']

                self._port_read_queue.append( # Return connection success
                    MockControlPacket(
                        command_number=201,
                    ).to_bytes()
                )
                
            else: # Device does not exist at provided index
                payload_bytes = conv.int_to_ascii_bytes(0xCF00, 4) # Connection failed
                self._port_read_queue.append(
                    MockControlPacket(
                        command_number=200,
                        payload=payload_bytes,
                    ).to_bytes()
                )
                return
            return

        # DISCONNECT
        if cmd == "DISCONNECT":
            pass
        # TODO

        # MODEL NUMBER GET
        if cmd == "GET MODEL NUMBER":
            payload_bytes = conv.int_to_ascii_bytes(0xC000, 4) # Get model number command return
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=206,
                    payload=payload_bytes,
                ).to_bytes()
            )

            # 12-byte model number
            raw = self.model_number.encode("ascii").ljust(12, b"\x00")
            payload_bytes = b"".join(
                f"{b:02X}".encode("ascii")
                for b in raw
            )

            self._port_read_queue.append(
                MockControlPacket( # Return model number
                    command_number=207,
                    payload=payload_bytes,
                ).to_bytes()
            )
            return

        # SAMPLE RATE GET
        if cmd == "GET SAMPLE RATE":
            payload_bytes = conv.int_to_ascii_bytes(0xC000, 4) # Get sample rate command return
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=208,
                    payload=payload_bytes,
                ).to_bytes()
            )

            sample_rate = self._device_default_sample_rate if self._sample_rate is None else self._sample_rate
            key = next(
                (k for k, v in self._SAMPLE_RATE_INDEX.items() if v == sample_rate),
                None,
            )

            self._port_read_queue.append(
                MockControlPacket( # Return sample rate
                    command_number=209,
                    payload=conv.int_to_ascii_bytes(key, 2),
                ).to_bytes()
            )
            return

        # SAMPLE RATE SET
        if cmd == "SET SAMPLE RATE":
            payload_bytes = conv.int_to_ascii_bytes(0xC000, 4) # Set sample rate
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=200,
                    payload=payload_bytes,
                ).to_bytes()
            )
            
            self._sample_rate = self._SAMPLE_RATE_INDEX[payload] # Set mock device's sample rate

            self._port_read_queue.append(
                MockControlPacket( # Return procedure complete
                    command_number=211
                ).to_bytes()
            )
            return
        
        # NAME GET
        if cmd == "GET NAME":
            payload_bytes = conv.int_to_ascii_bytes(0xC000, 4) # Get name command return
            self._port_read_queue.append(
                MockControlPacket(
                    command_number=220,
                    payload=payload_bytes,
                ).to_bytes()
            )

            # 12-byte model number
            raw = self._device_name_from_device.encode("ascii").ljust(13, b"\x00")
            payload_bytes = b"".join(
                f"{b:02X}".encode("ascii")
                for b in raw
            )

            self._port_read_queue.append(
                MockControlPacket( # Return device name
                    command_number=221,
                    payload=payload_bytes,
                ).to_bytes()
            )
            return
        
        if cmd == "STREAM":
            if payload == 0 or payload == 1:
                payload_bytes = conv.int_to_ascii_bytes(payload, 4)
                self._port_read_queue.append(
                    MockControlPacket(
                        command_number=6,
                        payload=payload_bytes,
                    ).to_bytes()
                )

                if payload == 1: # Start streaming
                    if self._stream_thread is None or not self._stream_thread.is_alive():
                        self._stream_stop.clear()
                        self._stream_thread = threading.Thread(
                            target=self._stream_worker,
                            daemon=True,
                        )
                        self._stream_thread.start()

                elif payload == 0: # Stop streaming
                    self._stream_stop.set()
                    if self._stream_thread is not None:
                        self._stream_thread.join(timeout=0.5)
                        if not self._stream_thread.is_alive():
                            self._stream_thread = None
        return

    # Sine wave
    def _stream_worker(self):
        """
        Background thread worker that generates synthetic streaming data.
        
        Generates continuous packets of three-phase sine wave data on channels 5, 6, and 7.
        The sine waves are phase-offset by 120 degrees, simulating a typical 3-phase
        measurement. This method runs in a background thread and is
        controlled by the _stream_stop event.
        
        The data is generated at the configured sample rate with proper timing to
        simulate real-time streaming. Each packet contains SAMPLES_PER_PACKET samples.
        """
        import math
        packet_period = self.SAMPLES_PER_PACKET / self.sample_rate
        next_emit_time = time.perf_counter() + packet_period

        sample_index = 0

        frequency = 10      # Hz
        amplitude = 150     # ADC counts
        offset = 2048       # ADC midpoint

        while not self._stream_stop.is_set():
            sleep_time = next_emit_time - time.perf_counter()
            if sleep_time > 0:
                if self._stream_stop.wait(sleep_time):
                    break

            ch5 = []
            ch6 = []
            ch7 = []

            for i in range(self.SAMPLES_PER_PACKET):
                t = (sample_index + i) / self.sample_rate

                ch5.append(int(offset + amplitude * math.sin(2 * math.pi * frequency * t)))
                ch6.append(int(offset + amplitude * math.sin(2 * math.pi * frequency * t + 2 * math.pi / 3)))
                ch7.append(int(offset + amplitude * math.sin(2 * math.pi * frequency * t + 4 * math.pi / 3)))

            raw_packet = MockDataPacket8274D(
                ch5=ch5,
                ch6=ch6,
                ch7=ch7,
                packet_number=self._stream_counter,
            ).to_bytes()

            self._port_read_queue.append(raw_packet)
            self._stream_counter = (self._stream_counter + 1) & 0xFFFF

            sample_index += self.SAMPLES_PER_PACKET

            next_emit_time += packet_period
            if next_emit_time < time.perf_counter():
                next_emit_time = time.perf_counter() + packet_period

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write_packet("STREAM", 0)

        for _ in range(3):
            try:
                self.read_pod_packet(timeout_sec=0.05)
            except TimeoutError:
                break

        return False

    def check_write_queue(self):
        # Mock doesn't use an inter-process write queue.
        return
