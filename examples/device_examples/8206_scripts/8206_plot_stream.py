"""Live Qt plot for the original 8206 (device type 1).

Install: pip install -e ".[plot,d2xx]"
Run: python examples/device_examples/8206_scripts/8206_plot_stream.py --com-port COM9
USB: python examples/device_examples/8206_scripts/8206_plot_stream.py --device SERIAL

Uses the same PlotSink/PlotDisplay as the 8206HR example. Close the window or
press Ctrl+C to stop. Sample rates: 200, 400, 600, 800, 1000, 2000 Hz.
Preamp gain must match the attached hardware; default is 100.
"""

import argparse
import multiprocessing as mp
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))

from Morelia.Devices import Pod8206
from Morelia.Stream.data_flow import DataFlow
from Morelia.Stream.sink import PlotSink, PlotDisplay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    connection = parser.add_mutually_exclusive_group(required=True)
    connection.add_argument('--com-port', help='Serial port, e.g. COM9 or /dev/ttyUSB0')
    connection.add_argument('--device', help='D2XX serial number or device index')
    parser.add_argument('--baudrate', type=int, default=9600)
    parser.add_argument('--preamp-gain', type=float, default=100)
    parser.add_argument('--sample-rate', type=int, choices=Pod8206.SAMPLE_RATES, default=400)
    parser.add_argument('--span', type=float, default=10, help='Visible seconds (default 10)')
    args = parser.parse_args()
    if args.span <= 0:
        parser.error('--span must be positive')
    port = args.com_port if args.com_port else args.device
    use_d2xx = args.com_port is None
    if use_d2xx and port.isdigit() and len(port) <= 2:
        port = int(port)
    pod = Pod8206(port, args.preamp_gain, baudrate=args.baudrate, use_d2xx=use_d2xx)
    flow = None
    queue = mp.Queue(maxsize=2048)
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
        # Preserve current lowpass and gain settings, then read them back.
        pod.sample_rate = args.sample_rate
        config = pod.read_configuration()
        if config[4] != args.sample_rate:
            raise RuntimeError(f'Device reports {config[4]} Hz after setting {args.sample_rate} Hz')
        pod.close_port()  # worker owns the hardware during acquisition
        display = PlotDisplay(queue, window_sec=args.span, refresh_ms=40)
        flow = DataFlow([(pod, [PlotSink(queue, pod)])])
        print(f'Streaming 8206 at {config[4]} Hz; close the plot window to stop.')
        flow.collect()
        display.run()
    except KeyboardInterrupt:
        pass
    finally:
        if flow is not None:
            flow.stop_collection()
        pod.cleanup()
        queue.close()


if __name__ == '__main__':
    mp.freeze_support()
    main()
