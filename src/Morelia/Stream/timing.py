"""Counter-aware acquisition timing for HR devices and 8274D EEG batches.

Timestamps identify the first sample. Missing positions count toward measured
rate exactly like real samples. Calendar time is anchored once; subsequent
elapsed time comes exclusively from a monotonic clock. Transport latency remains
unknown. Legacy 8206 deliberately uses a separate, assigned-rate path.
"""

import logging
import math
import time

import reactivex as rx

from Morelia.Devices import Pod8206HR, Pod8401HR, Pod8274D
from Morelia.packet.data import DataPacket

_log = logging.getLogger(__name__)


class MissingDataPacket(DataPacket):
    """Synthetic missing positions, including unknown EXT/TTL state.

    This is a stream-only packet, not a fabricated device wire packet.
    """

    is_missing = True

    def __init__(self, sample_count, packet_counter, batched):
        super().__init__(b"")
        self.sample_count = sample_count
        self.packet_counter = packet_counter
        for name in ("ch0", "ch1", "ch2", "ch3", "ext0", "ext1",
                     "ttl1", "ttl2", "ttl3", "ttl4"):
            setattr(self, name, math.nan)
        if batched:
            for name in ("ch5", "ch6", "ch7"):
                setattr(self, name, (math.nan,) * sample_count)


def timestamp_with_counters(pod):
    """Return an Rx operator with fresh sequence and clock state per subscription.

    Sirenia-compatible limits: 64 missing packets for HR, 20 for 8274D.
    Unlike clamping a large 8274 gap to 20 packets, resync it explicitly rather
    than imply that the unfilled remainder was recovered. HR counters are enabled
    after 200 transitions with at least 95% unit steps. No retrospective fills
    are made during that probe. Duplicate packets are suppressed when enabled.
    """
    if not isinstance(pod, (Pod8206HR, Pod8401HR, Pod8274D)):
        raise ValueError("Counter timing requires an HR or 8274D device")
    rate = float(pod.sample_rate)
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("sample_rate must be positive and finite")
    batched = isinstance(pod, Pod8274D)
    samples_per_packet = 40 if batched else 1
    modulus = 65536 if batched else 256
    max_missing = 20 if batched else 64
    probe_required = isinstance(pod, (Pod8206HR, Pod8401HR))
    device_label = "8206HR" if isinstance(pod, Pod8206HR) else "8401HR" if probe_required else "8274D"

    def operator(source):
        def subscribe(observer, scheduler=None):
            enabled = not probe_required
            probe_total = probe_good = 0
            probe_examples = []
            bad_jumps = 0
            last_counter = None
            last_arrival = None
            epoch_anchor = monotonic_anchor = None
            next_timestamp = None
            period = round(1e9 / rate)
            window_start = None
            window_samples = 0

            def on_next(packet):
                nonlocal enabled, probe_total, probe_good, last_counter, last_arrival
                nonlocal epoch_anchor, monotonic_anchor, next_timestamp, period
                nonlocal window_start, window_samples
                nonlocal bad_jumps
                now = time.monotonic_ns()
                counter = packet.packet_counter
                if probe_required and probe_total < 200 and len(probe_examples) < 8:
                    raw = getattr(packet, "raw_packet", b"")
                    probe_examples.append(f"{counter}:{raw.hex()}")
                if not 0 <= counter < modulus:
                    raise ValueError("Packet counter outside its device range")
                if epoch_anchor is None:
                    epoch_anchor = time.time_ns()
                    monotonic_anchor = now
                missing = 0
                restart_window = False
                resync = last_counter is None
                # After a whole counter cycle of silence, the number of wraps
                # is unknowable. Also reset after catastrophic transport stalls.
                silence_limit = min(6.0, modulus * samples_per_packet * period / 1e9)
                if last_arrival is not None and (now - last_arrival) / 1e9 >= silence_limit:
                    resync = True
                    _log.warning("Counter timing resynchronized after an ambiguous interruption")
                if last_counter is not None and not resync:
                    step = (counter - last_counter) % modulus
                    if probe_required and probe_total < 200:
                        probe_total += 1
                        probe_good += step == 1
                        if probe_total == 200:
                            enabled = probe_good >= 190
                            # A new rate window must not include untracked losses.
                            restart_window = True
                            if enabled:
                                _log.info("%s counter enabled (%d/200 unit steps)", device_label, probe_good)
                            else:
                                _log.warning(
                                    "%s counter disabled (%d/200 unit steps). No NaNs will be inserted; "
                                    "using assigned sample rate. Counter:raw-frame examples: %s",
                                    device_label, probe_good, ", ".join(probe_examples))
                    elif enabled:
                        if step == 0:
                            # Do not update last_arrival: a stalled counter must
                            # eventually trigger the interruption/resync path.
                            return
                        missing = step - 1
                        if missing > max_missing:
                            bad_jumps += 1
                            if probe_required and bad_jumps >= 3:
                                enabled = False
                                _log.warning("%s counter disabled after repeated implausible jumps; "
                                             "no further NaN insertion, using assigned sample rate. "
                                             "Last counters: %d -> %d; raw frame: %s",
                                             device_label, last_counter, counter,
                                             getattr(packet, "raw_packet", b"").hex())
                            else:
                                _log.warning("%s counter jump %d exceeds backfill limit %d; resynchronizing",
                                             device_label, missing, max_missing)
                            missing = 0
                            resync = True
                        else:
                            bad_jumps = 0

                if resync:
                    period = round(1e9 / rate)
                    arrival_epoch = epoch_anchor + now - monotonic_anchor
                    first = arrival_epoch - (samples_per_packet - 1) * period
                    next_timestamp = first if next_timestamp is None else max(next_timestamp, first)
                    window_start, window_samples = now, 0
                    if probe_required and probe_total < 200:
                        probe_total = probe_good = 0
                else:
                    window_samples += (missing + 1) * samples_per_packet

                for offset in range(missing):
                    gap = MissingDataPacket(samples_per_packet,
                                            (last_counter + offset + 1) % modulus, batched)
                    gap.sample_period_ns = period
                    observer.on_next((next_timestamp, gap))
                    next_timestamp += samples_per_packet * period

                packet.sample_period_ns = period
                observer.on_next((next_timestamp, packet))
                next_timestamp += samples_per_packet * period
                last_counter, last_arrival = counter, now

                if restart_window:
                    window_start, window_samples = now, 0

                # Three-second windows suppress packet/USB bursts. Count device
                # progression (including recovered positions), not arrivals.
                elapsed = now - window_start
                if enabled and elapsed >= 3_000_000_000 and window_samples:
                    measured_period = elapsed / window_samples
                    target = period + 0.1 * (measured_period - period)
                    period = max(1, round(min(period * 1.02, max(period * 0.98, target))))
                    window_start, window_samples = now, 0

            return source.subscribe(on_next, observer.on_error, observer.on_completed,
                                    scheduler=scheduler)
        return rx.create(subscribe)
    return operator
