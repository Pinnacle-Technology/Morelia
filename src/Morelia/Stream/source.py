"""Functions for getting streaming data from a POD device using `ReactiveX (RxPy) <https://rxpy.readthedocs.io/en/latest/index.html>`_."""

__author__      = 'James Hurd'
__maintainer__  = 'James Hurd'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2023, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

#environment imports
import signal
import sys
import traceback
from multiprocessing import Event
import threading
import time
from functools import partial
from contextlib import ExitStack

#local imports
from Morelia.Devices import Pod8206, Pod8206HR, Pod8401HR, Pod8274D, AcquisitionDevice

from Morelia.packet import ControlPacket

import reactivex as rx
from reactivex import operators as ops
from reactivex.operators import do_action

# Optional: schedulers for observe_on (decouple slow sinks from emission thread)
def _scheduler_for(spec):  # noqa: C901
    """Return a scheduler for the given spec, or None. Used by sinks that set observe_on_scheduler."""
    if spec is None:
        return None
    if spec == "thread_pool":
        try:
            from reactivex.scheduler import ThreadPoolScheduler
            return ThreadPoolScheduler()
        except ImportError:
            return None
    if spec == "new_thread":
        try:
            from reactivex.scheduler import NewThreadScheduler
            return NewThreadScheduler()
        except ImportError:
            return None
    return None

#TODO: __all__ to tell us what to export.

#TODO: type hints
#reactivex operator to timestamp packets as we get them based on the average observed sample
#rate (# total packets/time elapsed). this way, our timestamps are more evenly distributed
#and more closely resemble the time at which they were read from the device (as opposed
#to things like transfer and buffering delays by the OS/USB messign with things.
def _timestamp_8206_batches(sample_rate: int):
    """Timestamp the first sample of each legacy batch using its sample count."""
    def operator(source):
        def subscribe(observer, scheduler=None):
            start = None
            samples = 0

            def on_next(packet):
                nonlocal start, samples
                if start is None:
                    start = time.time_ns()
                observer.on_next((start + round(samples * 1e9 / sample_rate), packet))
                samples += packet.sample_count

            return source.subscribe(on_next, observer.on_error, observer.on_completed,
                                    scheduler=scheduler)
        return rx.create(subscribe)
    return operator


def _timestamp_via_adjusted_sample_rate(starting_sample_rate: int):
    def _timestamp_via_adjusted_sample_rate_operator(source):
        def subscribe(observer, scheduler=None):

            # Ensure starting_sample_rate is valid (must be > 0)
            # Store a safe fallback value that won't be modified
            safe_sample_rate = starting_sample_rate if starting_sample_rate and starting_sample_rate > 0 else 1000
            
            observer.sample_rate = safe_sample_rate
            observer.time_at_last_update = time.perf_counter()
            observer.starting_time = time.perf_counter()
            observer.last_timestamp = time.time_ns()
            observer.packet_count = 0
            
            def on_next(value):
                now_real_time_ns = time.time_ns()
                # Guard against division by zero if sample_rate is invalid
                if observer.sample_rate <= 0:
                    observer.sample_rate = safe_sample_rate  # Reset to safe fallback value
                
                # Ensure sample_rate is still valid before division
                if observer.sample_rate > 0:
                    predicted = int(observer.last_timestamp + (10**9 / observer.sample_rate))
                else:
                    predicted = now_real_time_ns  # Fallback if sample_rate is still invalid
                    
                drift = now_real_time_ns - predicted

                correction_factor = 0.005

                #add on a fraction of the sample rate to last timestamp, plus drift correction
                observer.last_timestamp = int(predicted + (drift * correction_factor))

                # timestamps used to be this, without correction factors:
                # observer.last_timestamp = int(observer.last_timestamp + (10**9 / observer.sample_rate))

                # if predicted time is greater than current time, reset time stamps
                if observer.last_timestamp > now_real_time_ns: 
                    observer.last_timestamp = now_real_time_ns

                observer.packet_count += 1

                # if it's been more than a second...
                if time.perf_counter() - observer.time_at_last_update >= 1:
                    
                    # adjust sample rate to be closer to what we are actually getting
                    # Guard against division by zero if time difference is too small
                    time_diff = time.perf_counter() - observer.starting_time
                    if time_diff > 0.001 and observer.packet_count > 0:  # At least 1ms elapsed and packets received
                        new_rate = observer.packet_count / time_diff
                        # Ensure calculated rate is valid (must be > 0)
                        if new_rate > 0:
                            observer.sample_rate = new_rate
                        else:
                            observer.sample_rate = safe_sample_rate  # Fallback if calculation gives invalid value
                    # If time_diff is too small or no packets, keep current sample_rate
                
                    observer.time_at_last_update = time.perf_counter()

                # send packet and timestamp on its way.
                observer.on_next((observer.last_timestamp, value))

            return source.subscribe(on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler)
        return rx.create(subscribe)
    return(_timestamp_via_adjusted_sample_rate_operator)

#TODO: type hints
#function used by reactivex to create an observable from a packet stream from an acquisition device.
def _stream_from_pod_device(pod: AcquisitionDevice, duration: float, manual_stop_event: Event):
    # Use fixed-size streaming read when available (1-2 read() per packet instead of many) for higher throughput
    read_fn = getattr(pod, "read_pod_packet_streaming", None)
    use_streaming = callable(read_fn)
    stream_timeout_sec = 0.2  # allow time for partial reads (e.g. 8206) and USB scheduling; still detects stall

    def _stream_from_pod_device_observable(observer, scheduler) -> None:
        timeout_message_shown = False  # only print first timeout so we don't forget it's there
        with pod:
            stream_start_time : float = time.perf_counter()
            while time.perf_counter()-stream_start_time < duration and not manual_stop_event.is_set():

                try:
                    if use_streaming:
                        observer.on_next(read_fn(timeout_sec=stream_timeout_sec, validate_checksum=False))
                    else:
                        observer.on_next(pod.read_pod_packet())
                except Exception as e:
                    if type(e).__name__ == "TimeoutError" and timeout_message_shown:
                        pass  # suppress after first timeout message
                    else:
                        print(f"Dropped packet due to {type(e).__name__}: {e}")
                        if type(e).__name__ == "TimeoutError":
                            timeout_message_shown = True
                    #traceback.print_exc()
                    continue
        # After exiting "with pod": __exit__ has run (STREAM 0 sent, read buffer drained).
        # Now close the port so the USB/D2XX handle is released cleanly.
        pod.close_port()

        # tell the observer we are finished.
        observer.on_completed()
    return _stream_from_pod_device_observable

#function used by reactivex to place raw packets (binary) into the read queue
def make_packet_putter(read_queue):
    def put_read_packet(item):
        if isinstance(item, ControlPacket):
            try:
                read_queue.put_nowait(item._raw_packet)
            except Exception as e:
                print(f"[!] Failed to queue control packet: {e}")
    return put_read_packet

def get_data(duration: float, manual_stop_event: Event, pod: AcquisitionDevice, sinks) -> None: 
    """Streams data from the POD device. The data drops about every 1 second.
    Streaming will continue until a "stop streaming" packet is recieved. 

    :param duration: How long to stream data for.
    :param manual_stop_event: Used to synchronize multiple ``get_data`` operations in a flowgraph. When a flowgraph is told to stop collecting, \
            this event is set which stops the loop within the reactivex operator that is collecting data.
    :param pod: The device to collect data from.
    """
    
    # Ensure port is open before we need it (e.g. sample_rate via write_read). D2XX defers open to first use.
    if pod._port is None:
        pod.open_port()

    #obtain read_queue from pod device
    read_queue = pod.obtain_read_queue()

    #obtain put read packet from the closure function
    put_read_packet = make_packet_putter(read_queue)

    # create an observable to stream from POD device.
    device = rx.create(_stream_from_pod_device(pod, duration, manual_stop_event))

    # create background queue 
    def background_writer(pod: AcquisitionDevice):
        while True:
            try:
                pod.check_write_queue()
            except Exception as e:
                # Catch any unexpected errors to prevent thread from crashing
                # This ensures the program remains interruptable
                import sys
                print(f"Warning: Error in background_writer thread: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(0.005) # sleep to avoid CPU performance issues

    threading.Thread(target=background_writer, args=(pod,), daemon=True).start()

    # pipe the packets from ``device`` into a filter that throws out control packets (eventually we don't want to do this, but have
    # a seperate place these get put so they can still be read during streaming to enable feedback.),
    # and them timestamp packets.
    
    data = device.pipe(
           do_action(lambda item: put_read_packet(item) if isinstance(item, ControlPacket) else None),
           ops.filter(lambda i: not isinstance(i, ControlPacket)), #todo: more strict filtering
           (_timestamp_8206_batches(pod.sample_rate) if isinstance(pod, Pod8206)
            else _timestamp_via_adjusted_sample_rate(pod.sample_rate))
       )
     
    # create a function that outputs a connectable observable.
    streamer = ops.publish()
    
    # create a connectable observable from the pipeline we constructed earlier.
    stream = streamer(data)
   
    # now, subscribe each sink to the connectable observable. Since sinks implment the context manager protocol, we can use an ExitStack.
    # Sinks that set observe_on_scheduler (e.g. "thread_pool") run their flush() on that scheduler, so the emission thread is not blocked by slow sinks.
    #TODO: handle errors (via on_error, right now we just print them).
    _OBSERVE_ON_BATCH_SIZE = 100

    with ExitStack() as context_manager_stack:

        send_to_sink = lambda sink, args: sink.flush(*args)
        send_batch_to_sink = lambda sink, batch: [sink.flush(*args) for args in batch]
        
        for sink in sinks:
            context_manager_stack.enter_context(sink)
            s = stream
            scheduler_spec = getattr(sink, "observe_on_scheduler", None)
            if scheduler_spec is not None:
                scheduler = _scheduler_for(scheduler_spec)
                if scheduler is not None:
                    s = s.pipe(
                        ops.buffer_with_count(_OBSERVE_ON_BATCH_SIZE),
                        ops.observe_on(scheduler),
                    )
                    s.subscribe(on_next=partial(send_batch_to_sink, sink), on_error=lambda e: print(e))
                    continue
            s.subscribe(on_next=partial(send_to_sink, sink), on_error=lambda e: print(e))
        
        # start streaming data from the observable!
        stream.connect()
        print("[DataFlow worker] stream.connect() returned, exiting sinks...", flush=True)

# wrapper function for get_data which reconstructs pod devices and sources after the process is created
def get_data_wrapper(duration_sec, manual_stop_event, source_class, source_dict, sinks_list):
    # Ignore SIGINT (Ctrl+C) in the worker so only the main process handles it. The main process
    # sets manual_stop_event and joins; the worker then exits the loop and runs sink __exit__.
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, ValueError):
        pass  # Windows or unsupported

    # obtain the source class
    source = source_class(**source_dict)

    # create list of sinks to use based on sink class/sink dictionary pair in the list
    sinks = [sink_class(**{**sink_dict, "pod": source}) for sink_class, sink_dict in sinks_list]

    # run get_data with the pod device and list of sinks
    # on Ctrl+C or shutdown (e.g. stop_collection), worker may get KeyboardInterrupt or I/O errors;
    # exit cleanly so we don't dump a traceback when the main process is stopping us
    try:
        get_data(duration_sec, manual_stop_event, source, sinks)
    except (KeyboardInterrupt, OSError, BrokenPipeError):
        if manual_stop_event.is_set():
            sys.exit(0)
        raise
    except BaseException:
        if manual_stop_event.is_set():
            sys.exit(0)
        raise
