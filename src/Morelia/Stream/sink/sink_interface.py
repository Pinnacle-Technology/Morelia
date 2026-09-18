"""Interface for dataflow sink."""

__author__      = 'James Hurd'
__maintainer__  = 'Thresa Kelly'
__credits__     = ['James Hurd', 'Sam Groth', 'Thresa Kelly', 'Seth Gabbert']
__license__     = 'New BSD License'
__copyright__   = 'Copyright (c) 2024, Thresa Kelly'
__email__       = 'sales@pinnaclet.com'

import abc
from typing import Any
from Morelia.packet.data import DataPacket

class SinkInterface(metaclass=abc.ABCMeta):
    """
    Interface that data sinks **must** implement.
    """

    @classmethod
    def __subclasshook__(cls, subclass) -> None:
        return ( hasattr(subclass, 'flush') and callable(subclass.flush) ) or NotImplemented

    @abc.abstractmethod
    def flush(self, timestamp: int, packet: DataPacket) -> None:
        """Send data to destination (e.g. and EDF file)."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_dict(self) -> dict[str, Any]:
        """Obtains sink __init__ argument values to use for process pickling"""
        pass
