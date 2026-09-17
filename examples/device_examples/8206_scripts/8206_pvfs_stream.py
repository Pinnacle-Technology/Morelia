"""Stream original 8206 (device type 1) data to a PVFS file.

Install: pip install -e ".[pvfs,d2xx]"
Run: python examples/device_examples/8206_scripts/8206_pvfs_stream.py --com-port COM9
USB: python examples/device_examples/8206_scripts/8206_pvfs_stream.py --device SERIAL

Output is Sirenia-compatible (experiment.db3 + indexed channels). Close with
Space/Enter, or use --duration. Sample rates: 200, 400, 600, 800, 1000, 2000 Hz.
Preamp gain must match the attached hardware; default is 100.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))

from Morelia.Devices import Pod8206
from Morelia.Stream.data_flow import DataFlow
from Morelia.Stream.sink import PvfsSink


def wait_for_stop_key():
    """Block until the user presses Space or Enter."""
    if sys.platform == "win32":
        try:
            import msvcrt
            while True:
                ch = msvcrt.getch()
                if ch in (b' ', b'\r', b'\n'):
                    return
        except Exception:
            input("Press Enter to stop.")
    else:
        try:
            import termios
            import tty
            if not sys.stdin.isatty():
                input("Press Enter to stop.")
                return
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                try:
                    while True:
                        ch = sys.stdin.read(1)
                        if ch in (' ', '\r', '\n'):
                            return
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)
            except Exception:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
                input("Press Enter to stop.")
        except ImportError:
            input("Press Enter to stop.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    connection = parser.add_mutually_exclusive_group(required=True)
    connection.add_argument('--com-port', help='Serial port, e.g. COM9 or /dev/ttyUSB0')
    connection.add_argument('--device', help='D2XX serial number or device index')
    parser.add_argument('--baudrate', type=int, default=9600)
    parser.add_argument('--preamp-gain', type=float, default=100)
    parser.add_argument('--sample-rate', type=int, choices=Pod8206.SAMPLE_RATES, default=400)
    parser.add_argument(
        '--output', '-o',
        default='output_8206.pvfs',
        help='Output PVFS file path (default: output_8206.pvfs)',
    )
    parser.add_argument(
        '--duration', '-d',
        type=float,
        default=None,
        metavar='SECONDS',
        help='Record for SECONDS then stop (default: Space/Enter to stop)',
    )
    args = parser.parse_args()

    port = args.com_port if args.com_port else args.device
    use_d2xx = args.com_port is None
    if use_d2xx and port.isdigit() and len(port) <= 2:
        port = int(port)

    pod = Pod8206(port, args.preamp_gain, baudrate=args.baudrate, use_d2xx=use_d2xx)
    flow = None
    try:
        if pod._port is None:
            pod.open_port()
        pod.write_packet('STREAM', 0)
        while True:
            try:
                pod.read_pod_packet(timeout_sec=0.2)
            except TimeoutError:
                break
            except ValueError:
                # Opening a device that was already streaming may start in
                # the middle of a packet. Drain to idle before querying it.
                continue
        if pod.type != 1:
            raise RuntimeError('Expected an original 8206 (TYPE 1). Use the 8206HR example for TYPE 48.')
        pod.sample_rate = args.sample_rate
        config = pod.read_configuration()
        if config[4] != args.sample_rate:
            raise RuntimeError(f'Device reports {config[4]} Hz after setting {args.sample_rate} Hz')
        pod.close_port()  # worker owns the hardware during acquisition

        pvfs_sink = PvfsSink(args.output, pod)
        flow = DataFlow([(pod, [pvfs_sink])])
        print(
            f'Streaming 8206 at {config[4]} Hz to {args.output} '
            f'(preamp_gain={args.preamp_gain})'
        )
        if args.duration is not None:
            print(f'Duration: {args.duration} s')
            flow.collect_for_seconds(args.duration)
        else:
            print('Press Space or Enter to stop.')
            with flow:
                wait_for_stop_key()
    except KeyboardInterrupt:
        pass
    finally:
        if flow is not None:
            try:
                flow.stop_collection()
            except Exception:
                pass
        try:
            pod.cleanup()
        except Exception:
            pass
    print('Done.')


if __name__ == '__main__':
    main()
