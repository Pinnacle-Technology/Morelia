"""Legacy protocol tests use actual wire frames without connected hardware."""

from functools import partial
from queue import Queue
import struct
from unittest.mock import Mock

import pytest
import reactivex as rx

from Morelia.Commands import CommandSet
from Morelia.Devices import AcquisitionDevice, Pod, Pod8206
from Morelia.packet import ControlPacket
from Morelia.packet.data import DataPacket8206
from Morelia.Stream.sink import PlotSink
from Morelia.Stream.source import _timestamp_8206_batches


def binary(rows):
    payload = b''.join(struct.pack('<hhh', *row) for row in rows)
    return (Pod.build_pod_packet_standard(11, f'{len(payload):04X}'.encode())
            + payload + Pod.checksum(payload) + b'\x03')


class Port:
    def __init__(self, data=b'', chunk=2):
        self.data = bytearray(data)
        self.chunk = chunk

    def read(self, count, timeout_sec=5):
        count = min(count, self.chunk)
        result = bytes(self.data[:count])
        del self.data[:count]
        return result


@pytest.fixture
def pod(monkeypatch):
    def init(self, port, max_rate, baudrate, device_name, **kwargs):
        self._commands = CommandSet()
        self._control_packet_factory = partial(ControlPacket, self._commands)
        self._port = Port()
        self._port_value = port
        self._baudrate = baudrate
        self._device_name = device_name or port
        self._use_d2xx = kwargs['use_d2xx']
    monkeypatch.setattr(AcquisitionDevice, '__init__', init)
    return Pod8206('COM_TEST', 100, sample_rate=400)


def test_signed_conversion_and_gain():
    packet = DataPacket8206(binary([(-8192, 0, 8191), (2, 3, -1)]), 100, 0, 5)
    assert packet.sample_count == 2
    assert packet.ch0 == pytest.approx(tuple((x + .5) * .00025 / .005078 for x in (-8192, 2)))
    assert packet.ch1[0] == pytest.approx(.5 * .00025 / .005078)
    assert packet.ch2[0] == pytest.approx(8191.5 * .00025 / .05078)
    assert packet.command_number == 11
    assert not hasattr(packet, 'ttl1')


def test_mixed_packets_and_embedded_delimiters(pod):
    control = Pod.build_pod_packet_standard(114, b'01')
    raw = binary([(2, 3, -8192), (8191, -1, 0)])
    pod._port = Port(control + raw + control, chunk=1)
    assert pod.read_pod_packet().payload == (1,)
    assert pod.read_pod_packet_streaming().raw_packet == raw
    assert pod.read_pod_packet().payload == (1,)


def test_partial_packet_survives_timeout(pod):
    raw = binary([(2, 3, -1)])
    pod._port = Port(raw[:14])
    with pytest.raises(TimeoutError):
        pod.read_pod_packet(timeout_sec=.01)
    pod._port.data.extend(raw[14:])
    assert pod.read_pod_packet().raw_packet == raw


@pytest.mark.parametrize('position', [9, -3])
def test_checksums(pod, position):
    raw = bytearray(binary([(0, 1, 2)]))
    raw[position] = ord('0') if raw[position] != ord('0') else ord('1')
    pod._port = Port(raw)
    with pytest.raises(ValueError, match='checksum'):
        pod.read_pod_packet()


def test_optional_payload_checksum(pod):
    raw = bytearray(binary([(1, 2, 3)]))
    raw[-3:-1] = b'XX'
    pod._port = Port(raw)
    assert pod.read_pod_packet(validate_checksum=False).sample_count == 1


def test_write_read_ignores_ttl_event(pod):
    pod.flush_port = Mock()
    pod.write_packet = Mock()
    pod._port = Port(Pod.build_pod_packet_standard(117, b'0102')
                     + Pod.build_pod_packet_standard(114, b'01'))
    assert pod.write_read('GET TTL IN').payload == (1,)
    pod.write_packet.assert_called_once_with('GET TTL IN', None)


def test_d2xx_fragmented_reads(pod):
    pod._use_d2xx = True
    raw = binary([(2, 3, -1)] * 4)
    pod._port = Port(raw, chunk=3)
    assert pod.read_pod_packet().raw_packet == raw


def test_status_and_command_layout(pod):
    # Captured response from an original 8206 on COM6: TYPE is uint16.
    assert pod._control_packet_factory(b'\x020008000176\x03').payload == (1,)
    status = (0, 1, 2, 3, 4, 5, 4000, 2, 10000, 4, 600, 14, 1, 1, 1)
    widths = (2,) * 6 + (4, 2, 4, 2, 4, 2, 2, 2, 4)
    payload = ''.join(f'{v:0{w}X}' for v, w in zip(status, widths)).encode()
    packet = pod._control_packet_factory(Pod.build_pod_packet_standard(5, payload))
    assert packet.payload == status
    assert pod._commands.argument_hex_char(101) == (4, 2, 4, 2, 4)
    assert pod._commands.return_hex_char(117) == (2, 2)
    assert pod._commands.is_command_binary(11)
    assert not pod._commands.does_command_exist(180)
    pod.write_read = Mock(return_value=packet)
    pod.sample_rate = 800
    assert pod.write_read.call_args.args == ('SET CONFIG', (4000, 2, 10000, 4, 800))
    assert pod.sample_rate == 800
    assert pod.get_dict()['eeg_gain_index'] == 2


@pytest.mark.parametrize('rate', [0, 100, 500, 2001])
def test_invalid_sample_rate_does_not_write(pod, rate):
    pod.write_read = Mock()
    with pytest.raises(ValueError):
        pod.sample_rate = rate
    pod.write_read.assert_not_called()


def test_plot_batch_and_timestamp_spacing(pod):
    packet = DataPacket8206(binary([(1, 2, 3)] * 12), 100)
    queue = Queue()
    sink = PlotSink(queue, pod, chunk_samples=2)
    sink.flush(1_000_000_000, packet)
    msg = queue.get_nowait()
    assert msg[3] == ('EEG1', 'EEG2', 'EMG')
    assert len(msg[4]) == 2  # first ten samples, not ten batches, skipped
    assert [row[0] for row in msg[4]] == [1_025_000_000, 1_027_500_000]
    result = []
    rx.from_iterable([packet, packet]).pipe(_timestamp_8206_batches(400)).subscribe(result.append)
    assert result[1][0] - result[0][0] == 30_000_000


@pytest.mark.parametrize('raw', [b'', binary([(0, 0, 0)])[:-1],
                                     Pod.build_pod_packet_standard(11, b'0005') + b'12345AA\x03'])
def test_malformed_packets(raw):
    with pytest.raises(ValueError):
        DataPacket8206(raw, 100)
