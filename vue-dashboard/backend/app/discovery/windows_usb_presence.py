"""Observe attached FTDI USB devices without opening their D2XX connections."""

from __future__ import annotations

import ctypes
import re
import sys

from app.diagnostic_events import diagnostic_operation

_PRESENT_USB = 0x00000100 | 0x00000001
_CR_BUFFER_SMALL = 0x0000001A
_USB_PARENT = re.compile(r"USB\\VID_0403&PID_([0-9A-F]{4})\\([^\\]+)", re.IGNORECASE)
_CHANNELS = {0x6001: "", 0x6010: "AB", 0x6011: "ABCD", 0x6014: ""}


def _configuration_manager():
    if sys.platform != "win32":
        raise OSError("Windows USB presence requires Windows")
    library = ctypes.WinDLL("cfgmgr32.dll")
    library.CM_Get_Device_ID_List_SizeW.argtypes = [
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.c_wchar_p,
        ctypes.c_ulong,
    ]
    library.CM_Get_Device_ID_List_SizeW.restype = ctypes.c_ulong
    library.CM_Get_Device_ID_ListW.argtypes = [
        ctypes.c_wchar_p,
        ctypes.POINTER(ctypes.c_wchar),
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    library.CM_Get_Device_ID_ListW.restype = ctypes.c_ulong
    return library


def _present_usb_instance_ids():
    library = _configuration_manager()
    # Replug can enlarge the device list between its size query and the read.
    for _attempt in range(3):
        size = ctypes.c_ulong()
        result = library.CM_Get_Device_ID_List_SizeW(ctypes.byref(size), "USB", _PRESENT_USB)
        if result != 0:
            raise OSError(f"Windows USB list size failed: CONFIGRET 0x{result:08X}")
        length = max(2, size.value)
        if length > 1_048_576:
            raise OSError("Windows USB device list exceeds the observation limit")
        buffer = ctypes.create_unicode_buffer(length)
        result = library.CM_Get_Device_ID_ListW("USB", buffer, length, _PRESENT_USB)
        if result == _CR_BUFFER_SMALL:
            continue
        if result != 0:
            raise OSError(f"Windows USB enumeration failed: CONFIGRET 0x{result:08X}")
        return [value for value in buffer[:].split("\0") if value]
    raise OSError("Windows USB inventory kept changing during enumeration")


def list_present_native_endpoints():
    """Map currently attached FTDI USB parents to native serial selectors."""
    with diagnostic_operation("windows_usb_inventory", transport="d2xx") as evidence:
        evidence.update(stage="enumerate_present_usb", presence_source="windows_pnp")
        instances = _present_usb_instance_ids()
        rows = []
        for instance in instances:
            match = _USB_PARENT.fullmatch(instance)
            if match is None:
                continue
            pid, serial = int(match[1], 16), match[2]
            channels = _CHANNELS.get(pid)
            # A generated location identifier cannot identify an EEPROM serial.
            known_identity = channels is not None and "&" not in serial
            selectors = [serial + channel for channel in channels] if channels else [serial]
            for selector in selectors:
                rows.append(
                    {
                        "address": selector if known_identity else None,
                        "hardware_id": selector if known_identity else None,
                        "instance_id": instance,
                        "vid": 0x0403,
                        "pid": pid,
                        "backend": "windows_pnp",
                    }
                )
        evidence.update(stage="complete", rows=rows, returned_count=len(rows))
        return rows
