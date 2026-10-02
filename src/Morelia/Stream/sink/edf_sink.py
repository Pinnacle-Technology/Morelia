"""Send data to EDF file."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert', 'Sean Gupta']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

from pyedflib import EdfWriter
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self
import numpy as np
import os

from Morelia.Stream.sink import SinkInterface
from Morelia.Stream.device_layout import resolve_layout
from Morelia.packet.data import DataPacket
from Morelia.Devices import AcquisitionDevice

class EDFSink(SinkInterface):
    """Stream data to an EDF file.

    NaN and infinite samples are stored as zero without removing sample positions.
    EDF+ annotations report missing values at most once per 10 seconds of written
    signal time, across all channels. EDF integer quantization also applies to zero.

    :param sample_rate: Sample rate of device being streamed from. Used in setting up EDF file.
    :param file_path: Path to CSV file to write to.
    :param pod: POD device data is being streamed from.
    :param observe_on_scheduler: If set (e.g. "thread_pool"), run flush() on that scheduler so the stream is not blocked by EDF I/O. Optional; queue is unbounded.
    """

    def __init__(self, file_path: str, pod: AcquisitionDevice, observe_on_scheduler: str | None = None) -> None:
        """ Class constructor."""
        self._file_path = file_path
        self._pod = pod
        self.observe_on_scheduler = observe_on_scheduler
        self._layout = resolve_layout(pod, profile="with_digital")
        self._channels = self._layout.channel_names
        self._buffer = [ [] for _ in self._channels ]
        self._samples_written = 0
        self._last_missing_annotation_sample = None

    @property 
    def pod(self):
        return self._pod
    
    @pod.setter
    def pod(self, device: AcquisitionDevice):
        self._pod = device
        self._layout = resolve_layout(device, profile="with_digital")
        self._channels = self._layout.channel_names
        self._buffer = [ [] for _ in self._channels ]
    
    @property
    def file_path(self):
        return self._file_path

    def __enter__(self) -> Self:

        EDF_PHYSICAL_BOUND = 2046
        EDF_DIGITAL_MAX = 32767
        EDF_DIGITAL_MIN = -32768

        # Delete existing file if it exists to allow overwrite
        # EdfWriter may not handle existing files correctly, so we must delete first
        # Retry deletion in case file is locked (e.g., from previous run that didn't close properly)
        if os.path.exists(self._file_path):
            import time
            import sys
            max_retries = 10
            retry_delay = 0.1  # 100ms
            deleted = False
            for attempt in range(max_retries):
                try:
                    os.remove(self._file_path)
                    # Small delay to ensure filesystem has processed the deletion
                    time.sleep(0.05)
                    if not os.path.exists(self._file_path):
                        deleted = True
                        break
                except OSError as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        # Last attempt failed - log warning
                        print(f"Warning: Could not delete existing EDF file '{self._file_path}': {e}. "
                              f"File may be locked. Attempting to create writer anyway.", file=sys.stderr)
            
            # Final check - if file still exists, warn but continue
            if os.path.exists(self._file_path):
                import sys
                print(f"Warning: EDF file '{self._file_path}' still exists after deletion attempts. "
                      f"This may cause write errors. Please close any programs using this file.", file=sys.stderr)

        self._edf_writer = EdfWriter(self._file_path, len(self._channels))
        self._samples_written = 0
        self._last_missing_annotation_sample = None

        for idx, channel in enumerate(self._channels):
            unit = self._layout.channels[idx].unit or 'uV'
            self._edf_writer.setSignalHeader( idx, {
                'label'         :  channel,
                'dimension'     :  unit,
                'sample_frequency'   :  self._pod.sample_rate,
                'physical_max'  :  EDF_PHYSICAL_BOUND,
                'physical_min'  : -EDF_PHYSICAL_BOUND,
                'digital_max'   :  EDF_DIGITAL_MAX,
                'digital_min'   :  EDF_DIGITAL_MIN,
                'transducer'    :  '',
                'prefilter'     :  ''
            } )

        return self

    def __exit__(self, *args, **kwargs) -> bool:

        self._write_buffer_to_edf()

        # Ensure file is properly closed
        if hasattr(self, '_edf_writer') and self._edf_writer is not None:
            try:
                self._edf_writer.close()
            except Exception as e:
                import sys
                print(f"Warning: Error closing EDF file: {type(e).__name__}: {e}", file=sys.stderr)
            finally:
                self._edf_writer = None

        return False


    #we have a "useless" timestamp paramater here so we implement the same function "interface".
    #TODO: check if sink is open
    def flush(self, timestamp: int, packet: DataPacket) -> None:
        """
        :meta private:
        """
        try:
            for _ts, values in self._layout.expand(timestamp, packet):
                for i, value in enumerate(values):
                    self._buffer[i].append(value)
        except (AttributeError, ValueError, TypeError) as e:
            import sys
            print(f"Warning: Skipping packet due to invalid data: {type(e).__name__}: {e}", file=sys.stderr)
            return

        if len(self._buffer[0]) >= self._pod.sample_rate:
            self._write_buffer_to_edf()

    def _write_buffer_to_edf(self) -> None:
        # Validate buffer before writing
        if not self._buffer or len(self._buffer) == 0:
            print("returned, nothing to write")
            return

        # Check that all buffers have the same length
        buffer_lengths = [len(b) for b in self._buffer]
        if not buffer_lengths or len(set(buffer_lengths)) != 1:
            import sys
            print(
                f"Warning: Skipping EDF write due to mismatched buffer lengths: {buffer_lengths}",
                file=sys.stderr
            )
            self._buffer = [[] for _ in self._channels]
            return

        try:
            samples_per_record = self._pod.sample_rate

            # Write complete EDF records only
            while len(self._buffer[0]) >= samples_per_record:

                arrays = [
                    np.array(
                        buf[:samples_per_record],
                        dtype=np.float64
                    )
                    for buf in self._buffer
                ]

                missing = np.any(~np.isfinite(arrays), axis=0)
                for arr in arrays:
                    np.nan_to_num(arr, copy=False, nan=0.0, posinf=0.0, neginf=0.0)

                self._edf_writer.writeSamples(arrays)

                # Remove written samples and keep overflow
                record_start = self._samples_written
                self._samples_written += samples_per_record
                for i in range(len(self._buffer)):
                    self._buffer[i] = self._buffer[i][samples_per_record:]

                # Use the file's sample timeline, not host arrival/writer speed.
                # Only annotate values in records that were actually written.
                for offset in np.flatnonzero(missing):
                    sample = record_start + int(offset)
                    last = self._last_missing_annotation_sample
                    if last is None or sample - last >= 10 * samples_per_record:
                        self._edf_writer.writeAnnotation(
                            sample / samples_per_record, -1,
                            "Missing values detected; replaced with zeros",
                        )
                        self._last_missing_annotation_sample = sample

        except OSError as e:
            import sys
            print(
                f"Warning: EDF write error (dropping buffer): {type(e).__name__}: {e}",
                file=sys.stderr
            )

        except Exception as e:
            import sys
            print(
                f"Warning: Unexpected error writing to EDF (dropping buffer): {type(e).__name__}: {e}",
                file=sys.stderr
            )

    def get_dict(self):
        return {
            'file_path': self.file_path,
            'observe_on_scheduler': self.observe_on_scheduler,
        }
