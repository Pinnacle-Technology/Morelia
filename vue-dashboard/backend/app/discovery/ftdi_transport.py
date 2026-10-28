"""Provide D2XX and VCP port access for managed PODs in the backend.

managed_pods uses the SDK for POD commands and decoding. Importing this module
does not enumerate or open USB devices.
"""

from __future__ import annotations

import math
import sys
import time
from contextlib import contextmanager
from ctypes import byref
from threading import RLock

from app.diagnostic_events import descriptor_evidence, diagnostic_operation

from .native_gate import native_driver_gate

try:
    import ftd2xx as ftd
    from ftd2xx import ftd2xx as _native_wrapper
except (ImportError, OSError):
    ftd = None
    _native_wrapper = None

_cleanup_lock = RLock()
_pending_cleanup = {}


class NativeHandleCleanupError(RuntimeError):
    """Retain a failed native close for retry before releasing device ownership.

    Preserve both the operation error and the cleanup error for diagnosis.
    """

    def __init__(self, operation_error, cleanup_error, close, *, serial=None):
        super().__init__(
            f"Native setup failed ({operation_error}); handle cleanup failed ({cleanup_error})"
        )
        self.operation_error = operation_error
        self.cleanup_error = cleanup_error
        self._close = close
        self.serial = serial
        self._release_callbacks = []

    def add_release_callback(self, callback):
        """Release ownership after the handle closes, or now if already closed."""
        with _cleanup_lock:
            if self._close is not None:
                self._release_callbacks.append(callback)
                return
        callback()

    def retry_close(self):
        with native_driver_gate():
            if self._close is not None:
                try:
                    self._close()
                except BaseException as cleanup_error:
                    self.cleanup_error = cleanup_error
                    raise self from cleanup_error
                with _cleanup_lock:
                    self._close = None
                    if _pending_cleanup.get(self.serial) is self:
                        del _pending_cleanup[self.serial]
                    callbacks, self._release_callbacks = self._release_callbacks, []
                for callback in callbacks:
                    callback()


def pending_cleanup(serial):
    """Return the failed close retained for this exact device serial."""
    with _cleanup_lock:
        return _pending_cleanup.get(serial)


def retry_pending_cleanup(serial):
    """Close any retained handle before the caller opens this device again."""
    with native_driver_gate():
        owner = pending_cleanup(serial)
        if owner is not None:
            owner.retry_close()


def _remember_cleanup(operation_error, cleanup_error, close, serial):
    with _cleanup_lock:
        # Preserve existing cleanup callbacks if a later attempt times out
        # waiting for the driver gate.
        owner = _pending_cleanup.get(serial)
        if owner is not None:
            return owner
        owner = NativeHandleCleanupError(operation_error, cleanup_error, close, serial=serial)
        _pending_cleanup[serial] = owner
    return owner


def _close_after_failure(close, operation_error, serial):
    try:
        close()
    except BaseException as cleanup_error:
        raise _remember_cleanup(operation_error, cleanup_error, close, serial) from operation_error


def _open_native_serial(serial):
    """Keep the raw handle available for cleanup if wrapper construction fails.

    ftd2xx.openEx can leave the handle open if FTD2XX's getDeviceInfo call fails.
    Open it here so that failure can close the handle or retain it for retry.
    This function depends on wrapper internals, which may vary by version.
    """
    if _native_wrapper is None:
        raise ImportError("Native D2XX requires ftd2xx and the FTDI native library")
    with native_driver_gate():
        retry_pending_cleanup(serial)
        native = _native_wrapper._ft
        raw_handle = native.FT_HANDLE()
        _native_wrapper.call_ft(
            native.FT_OpenEx, serial.encode("utf-8"), native.DWORD(1), byref(raw_handle),
        )
        try:
            return _native_wrapper.FTD2XX(raw_handle, update=False)
        except BaseException as operation_error:
            _close_after_failure(
                lambda: _native_wrapper.call_ft(native.FT_Close, raw_handle), operation_error, serial,
            )
            raise


def serial_text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else str(value or "")


def list_d2xx_devices(*, native_only=True):
    """Read one D2XX inventory snapshot without changing device serial strings."""
    with diagnostic_operation("native_inventory", transport="d2xx") as evidence:
        evidence.update(stage="driver_available", rows=[], expected_count=None, returned_count=0,
                        python_executable=sys.executable,
                        driver_module=getattr(ftd, "__file__", None), platform=sys.platform)
        if ftd is None:
            raise ImportError("Native D2XX requires ftd2xx and the FTDI native library")
        evidence["stage"] = "driver_gate"
        with native_driver_gate():
            rows = []
            evidence["stage"] = "create_device_info_list"
            count = ftd.createDeviceInfoList()
            evidence["expected_count"] = count
            for index in range(count):
                evidence.update(stage="get_device_info_detail", failed_index=index)
                info = ftd.getDeviceInfoDetail(index, update=False)
                evidence["rows"].append(descriptor_evidence(index, info))
                evidence["stage"] = "decode_descriptor"
                if not isinstance(info, dict):
                    raise ValueError("Unrecognized FTDI device descriptor")
                rows.append({
                    "index": index, "serial": serial_text(info.get("serial")),
                    "description": serial_text(info.get("description")),
                    "flags": info.get("flags", 0), "type": info.get("type"),
                    "id": info.get("id"), "location": info.get("location", info.get("locid")),
                    "backend": "ftd2xx",
                })
                evidence["returned_count"] = len(rows)
            evidence.update(stage="complete", failed_index=None, returned_count=len(rows))
            return rows


class _DeadlinePort:
    """Share one deadline across reads so SDK retries cannot restart the timeout."""

    _deadline = None

    @contextmanager
    def deadline(self, seconds):
        previous = self._deadline
        expires = time.monotonic() + seconds
        self._deadline = min(previous, expires) if previous is not None else expires
        try:
            yield
        finally:
            self._deadline = previous

    def _read_seconds(self, timeout_sec):
        seconds = self.read_timeout_ms / 1000 if timeout_sec is None else timeout_sec
        if self._deadline is not None:
            seconds = min(seconds, self._deadline - time.monotonic())
        if seconds <= 0:
            raise TimeoutError("POD packet deadline expired")
        return seconds

    def is_serial_closed(self):
        return not self.is_serial_open()

    def read_line(self):
        return self.read_until(b"\n")

    def read_until(self, eol):
        data = b""
        with self.deadline(self.read_timeout_ms / 1000):
            while True:
                chunk = self.read(1)
                if not chunk:
                    return None
                data += chunk
                if eol in data:
                    return data


def _validate_settings(flow_control, read_timeout_ms, write_timeout_ms):
    if flow_control not in ("none", "rts_cts"):
        raise ValueError("Unsupported flow control")
    if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
           for value in (read_timeout_ms, write_timeout_ms)):
        raise ValueError("Connection timeouts must be positive integer milliseconds")


class D2XXPortIO(_DeadlinePort):
    """Open D2XX devices by exact serial, excluding indexes, descriptions and COM aliases."""

    def __init__(self, port, baudrate=9600, *, d2xx_serial_only=True,
                 flow_control=None, read_timeout_ms=5000, write_timeout_ms=5000):
        self._ftd2xx_handle = None
        self._cached_timeouts = None
        self.flow_control = flow_control or "rts_cts"
        _validate_settings(self.flow_control, read_timeout_ms, write_timeout_ms)
        self.read_timeout_ms, self.write_timeout_ms = read_timeout_ms, write_timeout_ms
        self.port, self.baudrate = port, baudrate
        if not isinstance(port, str) or not port or port != port.strip():
            raise ValueError("Managed D2XX connections require a nonempty exact serial string")
        with diagnostic_operation("native_open", transport="d2xx", port=port) as evidence:
            evidence.update(stage="driver_gate", baudrate=baudrate, flow_control=self.flow_control,
                            read_timeout_ms=read_timeout_ms, write_timeout_ms=write_timeout_ms)
            with native_driver_gate():
                evidence["stage"] = "pending_cleanup"
                retry_pending_cleanup(port)
                try:
                    # Open the exact serial and verify the handle before configuring it.
                    evidence["stage"] = "open_by_serial"
                    self._ftd2xx_handle = _open_native_serial(port)
                    evidence["stage"] = "verify_identity"
                    info = self._ftd2xx_handle.getDeviceInfo()
                    evidence["rows"] = [descriptor_evidence(None, info)]
                    if not isinstance(info, dict) or serial_text(info.get("serial")) != port:
                        raise ValueError("Opened FTDI identity differs from requested serial")
                    handle = self._ftd2xx_handle
                    evidence["stage"] = "configure_handle"
                    handle.setBaudRate(baudrate)
                    handle.setDataCharacteristics(8, 0, 0)
                    handle.setFlowControl(256 if self.flow_control == "rts_cts" else 0, 0x11, 0x13)
                    handle.setTimeouts(read_timeout_ms, write_timeout_ms)
                    self._cached_timeouts = (read_timeout_ms, write_timeout_ms)
                    handle.purge(3)
                    evidence["stage"] = "complete"
                except BaseException as operation_error:
                    try:
                        _close_after_failure(self._close_handle, operation_error, port)
                    finally:
                        evidence["handle_retained"] = self._ftd2xx_handle is not None
                        evidence["cleanup_pending"] = pending_cleanup(port) is not None
                    raise

    def is_serial_open(self):
        return self._ftd2xx_handle is not None

    def close_serial_port(self):
        with diagnostic_operation("native_close", transport="d2xx", port=self.port) as evidence:
            try:
                evidence["stage"] = "driver_gate"
                with native_driver_gate():
                    evidence["stage"] = "close_handle"
                    owner = pending_cleanup(self.port)
                    if owner is not None:
                        owner.retry_close()
                    self._close_handle()
                    evidence["stage"] = "complete"
            except NativeHandleCleanupError:
                raise
            except BaseException as cleanup_error:
                raise _remember_cleanup(
                    cleanup_error, cleanup_error, self._close_handle, self.port,
                ) from cleanup_error
            finally:
                evidence["handle_retained"] = self._ftd2xx_handle is not None
                evidence["cleanup_pending"] = pending_cleanup(self.port) is not None

    def _close_handle(self):
        """Close the handle while the caller holds the native driver gate."""
        if self._ftd2xx_handle is not None:
            self._ftd2xx_handle.close()
            self._ftd2xx_handle = None
        self._cached_timeouts = None

    def read(self, numBytes, timeout_sec=None):
        if self.is_serial_closed():
            raise OSError("FTDI device is closed")
        timeouts = (
            max(1, math.ceil(self._read_seconds(timeout_sec) * 1000)), self.write_timeout_ms,
        )
        try:
            if self._cached_timeouts != timeouts:
                # Set this handle's remaining timeout without the driver gate.
                # Waiting for inventory or opens could exceed the read deadline.
                self._ftd2xx_handle.setTimeouts(*timeouts)
                self._cached_timeouts = timeouts
            return self._ftd2xx_handle.read(numBytes) or None
        except OSError:
            raise
        except Exception as exc:
            # managed_pods preserves OSError through the SDK's packet retries.
            # Convert FTDI errors so driver failures are not reported as timeouts.
            raise OSError(f"Native FTDI read failed: {exc}") from exc

    def write(self, message):
        if self.is_serial_closed():
            raise OSError("FTDI device is closed")
        count = self._ftd2xx_handle.write(message)
        if count != len(message):
            raise OSError(f"Partial FTDI write: {count}/{len(message)}")

    def purge_rx(self):
        with native_driver_gate():
            self._ftd2xx_handle.purge(1)
        return True

    def flush(self):
        with native_driver_gate():
            self._ftd2xx_handle.purge(3)
        return True

    def set_baudrate(self, baudrate):
        with native_driver_gate():
            self._ftd2xx_handle.setBaudRate(baudrate)
            self.baudrate = baudrate
        return True

    def get_port_name(self):
        return self.port if self.is_serial_open() else None


class VcpPortIO(_DeadlinePort):
    """Provide pyserial port access with the same read deadlines as D2XX."""

    def __init__(self, port, baudrate=9600, *, d2xx_serial_only=False,
                 flow_control=None, read_timeout_ms=5000, write_timeout_ms=5000):
        from serial import Serial

        self.flow_control = flow_control or "none"
        _validate_settings(self.flow_control, read_timeout_ms, write_timeout_ms)
        self.read_timeout_ms, self.write_timeout_ms = read_timeout_ms, write_timeout_ms
        self.port, self.baudrate = port, baudrate
        self._serial_inst = Serial(port=port, baudrate=baudrate,
                                   timeout=read_timeout_ms / 1000,
                                   write_timeout=write_timeout_ms / 1000,
                                   rtscts=self.flow_control == "rts_cts")

    def is_serial_open(self):
        return self._serial_inst.is_open

    def close_serial_port(self):
        self._serial_inst.close()

    def read(self, numBytes, timeout_sec=None):
        seconds = max(0.001, math.ceil(self._read_seconds(timeout_sec) * 1000) / 1000)
        if self._serial_inst.timeout != seconds:
            self._serial_inst.timeout = seconds
        return self._serial_inst.read(numBytes) or None

    def write(self, message):
        count = self._serial_inst.write(message)
        if count != len(message):
            raise OSError(f"Partial serial write: {count}/{len(message)}")

    def purge_rx(self):
        self._serial_inst.reset_input_buffer()
        return True

    def flush(self):
        self._serial_inst.reset_input_buffer()
        self._serial_inst.reset_output_buffer()
        return True

    def set_baudrate(self, baudrate):
        self._serial_inst.baudrate = baudrate
        self.baudrate = baudrate
        return True

    def get_port_name(self):
        return self.port if self.is_serial_open() else None
