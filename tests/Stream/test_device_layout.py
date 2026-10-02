"""Unit tests for shared stream device layouts."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from Morelia.Devices import Pod8206, Pod8206HR, Pod8274D, Pod8401HR, Preamp
from Morelia.Stream.device_layout import ilp_channel_tag, resolve_layout


def _pod(cls, **attrs):
    pod = MagicMock(spec=cls)
    pod.sample_rate = attrs.pop("sample_rate", 1000)
    pod.device_name = attrs.pop("device_name", "mock")
    for key, value in attrs.items():
        setattr(pod, key, value)
    # Ensure isinstance checks against the real class succeed.
    pod.__class__ = cls
    return pod


def test_ilp_channel_tag_sanitizes():
    assert ilp_channel_tag("EEG3/EMG") == "EEG3_EMG"
    assert ilp_channel_tag("Ch 5") == "Ch_5"


def test_8206_analog_batch_expand():
    pod = _pod(Pod8206, sample_rate=1000)
    layout = resolve_layout(pod, profile="analog")
    assert layout.channel_names == ("EEG1", "EEG2", "EMG")
    assert layout.batched is True

    packet = SimpleNamespace(ch0=(1.0, 2.0), ch1=(3.0, 4.0), ch2=(5.0, 6.0))
    samples = layout.expand(1_000_000_000, packet)
    assert len(samples) == 2
    assert samples[0] == (1_000_000_000, (1.0, 3.0, 5.0))
    assert samples[1][0] == 1_000_000_000 + 1_000_000
    assert samples[1][1] == (2.0, 4.0, 6.0)


def test_8206hr_with_digital_includes_ttl():
    pod = _pod(Pod8206HR, sample_rate=2000)
    layout = resolve_layout(pod, profile="with_digital")
    assert layout.channel_names == ("EEG1", "EEG2", "EEG3/EMG", "TTL1", "TTL2", "TTL3", "TTL4")

    packet = SimpleNamespace(ch0=1.0, ch1=2.0, ch2=float("nan"), ttl1=1, ttl2=0, ttl3=1, ttl4=0)
    samples = layout.expand(10, packet)
    assert samples[0][0] == 10
    assert samples[0][1][:2] == (1.0, 2.0)
    assert samples[0][1][2] != samples[0][1][2]  # NaN survives layout expansion.
    assert samples[0][1][3:] == (1.0, 0.0, 1.0, 0.0)


def test_8401_preamp_names_and_digital():
    pod = _pod(Pod8401HR, sample_rate=2000, preamp=Preamp.Preamp8406_SE4, channel_labels=None)
    layout = resolve_layout(pod, profile="with_digital")
    assert layout.channel_names[:4] == ("EEG4", "EEG1", "EEG3", "EEG2")
    assert layout.channel_names[4:] == ("EXT0", "EXT1", "TTL1", "TTL2", "TTL3", "TTL4")

    packet = SimpleNamespace(
        ch0=1, ch1=2, ch2=3, ch3=4, ext0=5, ext1=6, ttl1=1, ttl2=0, ttl3=1, ttl4=0
    )
    assert layout.expand(0, packet)[0][1] == (1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 1.0, 0.0, 1.0, 0.0)


def test_8401_channel_labels_override():
    pod = _pod(
        Pod8401HR,
        sample_rate=2000,
        preamp=None,
        channel_labels=("L1", "L2", "L3", "L4"),
    )
    layout = resolve_layout(pod, profile="analog")
    assert layout.channel_names == ("L1", "L2", "L3", "L4")


def test_8274_batch_expand():
    pod = _pod(Pod8274D, sample_rate=1000)
    layout = resolve_layout(pod, profile="analog")
    assert layout.channel_names == ("Ch5", "Ch6", "Ch7")
    assert layout.batched is True

    packet = SimpleNamespace(ch5=[1.0, 2.0], ch6=[3.0, 4.0], ch7=[5.0, 6.0])
    samples = layout.expand(0, packet)
    assert samples[0] == (0, (1.0, 3.0, 5.0))
    assert samples[1][0] == 1_000_000


def test_unsupported_device_raises():
    pod = MagicMock()
    pod.device_name = "unknown"
    pod.sample_rate = 1000
    with pytest.raises(ValueError, match="not supported"):
        resolve_layout(pod, profile="analog")
