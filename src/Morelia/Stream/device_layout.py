"""Shared stream channel schema and packet expansion for sinks.

Device-specific channel names and packet→sample expansion live here so sinks
only handle destination formatting (CSV, EDF, UDP, plot, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Sequence

from Morelia.Devices import (
    AcquisitionDevice,
    Pod8206,
    Pod8206HR,
    Pod8274D,
    Pod8401HR,
)
from Morelia.packet.data import DataPacket

ChannelKind = Literal["analog", "ext", "ttl"]
LayoutProfile = Literal["analog", "with_digital"]


def ilp_channel_tag(name: str) -> str:
    """Sanitize a channel name for InfluxDB / QuestDB line-protocol tags."""
    return name.replace("/", "_").replace(" ", "_")


@dataclass(frozen=True)
class ChannelSpec:
    """One output column / series produced by a stream layout."""

    name: str
    unit: str = "uV"
    kind: ChannelKind = "analog"


def _sample_period_ns(sample_rate: float) -> int:
    rate = float(sample_rate) if sample_rate else 1000.0
    return int(1e9 / rate)


ExpandFn = Callable[[int, DataPacket], list[tuple[int, tuple[float, ...]]]]


@dataclass(frozen=True)
class StreamLayout:
    """Channel schema and packet expansion for one device + profile."""

    channels: tuple[ChannelSpec, ...]
    expand: ExpandFn
    batched: bool = False
    # Analog packet attribute names (e.g. ch0/ch1/ch2 or ch5/ch6/ch7).
    analog_attrs: tuple[str, ...] = ()

    @property
    def channel_names(self) -> tuple[str, ...]:
        return tuple(ch.name for ch in self.channels)

    @property
    def units(self) -> tuple[str, ...]:
        return tuple(ch.unit for ch in self.channels)

    def analog_packet_values(self, packet: DataPacket) -> tuple:
        """Raw analog channel attributes from ``packet`` (scalars or sample lists)."""
        return tuple(getattr(packet, attr) for attr in self.analog_attrs)


def _expand_8206(timestamp_ns: int, packet: DataPacket, sample_rate: float) -> list[tuple[int, tuple[float, ...]]]:
    period = _sample_period_ns(sample_rate)
    return [
        (timestamp_ns + i * period, (float(ch0), float(ch1), float(ch2)))
        for i, (ch0, ch1, ch2) in enumerate(zip(packet.ch0, packet.ch1, packet.ch2))
    ]


def _expand_8206hr_analog(timestamp_ns: int, packet: DataPacket) -> list[tuple[int, tuple[float, ...]]]:
    return [(timestamp_ns, (float(packet.ch0), float(packet.ch1), float(packet.ch2)))]


def _expand_8206hr_digital(timestamp_ns: int, packet: DataPacket) -> list[tuple[int, tuple[float, ...]]]:
    return [
        (
            timestamp_ns,
            (
                float(packet.ch0),
                float(packet.ch1),
                float(packet.ch2),
                float(packet.ttl1),
                float(packet.ttl2),
                float(packet.ttl3),
                float(packet.ttl4),
            ),
        )
    ]


def _8401_analog_names(pod: Pod8401HR) -> tuple[str, ...]:
    labels = getattr(pod, "channel_labels", None)
    if labels is not None:
        return tuple(labels)
    if pod.preamp is not None:
        preamp_map = Pod8401HR.get_channel_map_for_preamp_device(pod.preamp)
        if preamp_map is not None:
            return tuple(preamp_map.values())
    return ("A", "B", "C", "D")


def _expand_8401_analog(timestamp_ns: int, packet: DataPacket) -> list[tuple[int, tuple[float, ...]]]:
    return [
        (
            timestamp_ns,
            (
                float(packet.ch0),
                float(packet.ch1),
                float(packet.ch2),
                float(packet.ch3),
            ),
        )
    ]


def _expand_8401_digital(timestamp_ns: int, packet: DataPacket) -> list[tuple[int, tuple[float, ...]]]:
    return [
        (
            timestamp_ns,
            (
                float(packet.ch0),
                float(packet.ch1),
                float(packet.ch2),
                float(packet.ch3),
                float(packet.ext0),
                float(packet.ext1),
                float(packet.ttl1),
                float(packet.ttl2),
                float(packet.ttl3),
                float(packet.ttl4),
            ),
        )
    ]


def _expand_8274(timestamp_ns: int, packet: DataPacket, sample_rate: float) -> list[tuple[int, tuple[float, ...]]]:
    period = getattr(packet, "sample_period_ns", _sample_period_ns(sample_rate))
    return [
        (timestamp_ns + i * period, (float(ch5), float(ch6), float(ch7)))
        for i, (ch5, ch6, ch7) in enumerate(zip(packet.ch5, packet.ch6, packet.ch7))
    ]


def _specs(names: Sequence[str], kind: ChannelKind = "analog", unit: str = "uV") -> tuple[ChannelSpec, ...]:
    return tuple(ChannelSpec(name=n, unit=unit, kind=kind) for n in names)


_TTL_SPECS = _specs(("TTL1", "TTL2", "TTL3", "TTL4"), kind="ttl", unit="")
_EXT_SPECS = _specs(("EXT0", "EXT1"), kind="ext", unit="uV")


def resolve_layout(pod: AcquisitionDevice, *, profile: LayoutProfile = "analog") -> StreamLayout:
    """Return the stream layout for ``pod`` and the requested channel profile.

    :param profile: ``analog`` — primary analog channels only.
        ``with_digital`` — analog plus EXT/TTL when the packet family has them.
    Non-finite samples are preserved; each sink owns its missing-data policy.
    """
    if profile not in ("analog", "with_digital"):
        raise ValueError(f'Unknown layout profile "{profile}"')

    sample_rate = float(getattr(pod, "sample_rate", None) or 1000.0)

    if isinstance(pod, Pod8206):
        channels = _specs(("EEG1", "EEG2", "EMG"))
        return StreamLayout(
            channels=channels,
            expand=lambda ts, pkt: _expand_8206(ts, pkt, sample_rate),
            batched=True,
            analog_attrs=("ch0", "ch1", "ch2"),
        )

    if isinstance(pod, Pod8206HR):
        analog = _specs(("EEG1", "EEG2", "EEG3/EMG"))
        attrs = ("ch0", "ch1", "ch2")
        if profile == "with_digital":
            return StreamLayout(
                channels=analog + _TTL_SPECS,
                expand=_expand_8206hr_digital,
                batched=False,
                analog_attrs=attrs,
            )
        return StreamLayout(
            channels=analog,
            expand=_expand_8206hr_analog,
            batched=False,
            analog_attrs=attrs,
        )

    if isinstance(pod, Pod8401HR):
        analog = _specs(_8401_analog_names(pod))
        attrs = ("ch0", "ch1", "ch2", "ch3")
        if profile == "with_digital":
            return StreamLayout(
                channels=analog + _EXT_SPECS + _TTL_SPECS,
                expand=_expand_8401_digital,
                batched=False,
                analog_attrs=attrs,
            )
        return StreamLayout(
            channels=analog,
            expand=_expand_8401_analog,
            batched=False,
            analog_attrs=attrs,
        )

    if isinstance(pod, Pod8274D):
        channels = _specs(("Ch5", "Ch6", "Ch7"))
        return StreamLayout(
            channels=channels,
            expand=lambda ts, pkt: _expand_8274(ts, pkt, sample_rate),
            batched=True,
            analog_attrs=("ch5", "ch6", "ch7"),
        )

    name = getattr(pod, "device_name", type(pod).__name__)
    raise ValueError(f'Device "{name}" is not supported by stream layouts.')
