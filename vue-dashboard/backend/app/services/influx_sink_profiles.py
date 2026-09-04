"""Validated lifecycle for immutable Influx sink-profile revisions."""

from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import re
import socket
import ssl
from dataclasses import dataclass
from math import ceil
from urllib.parse import urlsplit, urlunsplit

from flask import current_app
from sqlalchemy.exc import IntegrityError

from app.database import db
from app.models.influx_sink_profile import (
    InfluxSinkProfile,
    InfluxSinkProfileRevision,
)
from app.repositories.influx_sink_profiles import (
    ArchiveAdvanceFailed,
    InfluxSinkProfileRepository,
    RevisionAdvanceFailed,
)

_repo = InfluxSinkProfileRepository()
_ENV_REFERENCE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_CONFIG_FIELDS = (
    "url",
    "org",
    "bucket",
    "write_token_env",
    "read_token_env",
    "observe_on_scheduler",
    "buffer_max_age_seconds",
    "buffer_max_bytes",
)
_CLOUD_METADATA_ADDRESSES = {
    ipaddress.ip_address("169.254.169.254"),
    ipaddress.ip_address("fd00:ec2::254"),
}


class InfluxSinkProfileError(Exception):
    code = "influx_sink_profile_error"
    status = 422


class InfluxSinkProfileNotFound(InfluxSinkProfileError):
    code = "influx_sink_profile_not_found"
    status = 404


class InfluxSinkProfileNameExists(InfluxSinkProfileError):
    code = "influx_sink_profile_name_exists"
    status = 409


class StaleInfluxSinkProfileRevision(InfluxSinkProfileError):
    code = "stale_influx_profile_revision"
    status = 409


class InfluxSinkProfileArchived(InfluxSinkProfileError):
    code = "influx_profile_archived"
    status = 409


class InvalidInfluxSinkProfile(InfluxSinkProfileError):
    code = "invalid_influx_sink_profile"
    status = 422


class InfluxDestinationNotAllowed(InfluxSinkProfileError):
    code = "influx_destination_not_allowed"
    status = 422


@dataclass(frozen=True)
class ValidatedDestination:
    url: str
    hostname: str
    port: int
    addresses: tuple[str, ...]


@dataclass(frozen=True)
class FrozenInfluxSinkProfile:
    """One exact active profile revision projected for a new session run.

    These values contain credential *reference names*, never credential values.
    The public profile DTO deliberately omits those names; this internal shape
    exists only at the run-materialization boundary.
    """

    profile_id: str
    revision: int
    content_hash: str
    url: str
    org: str
    bucket: str
    write_token_env: str
    read_token_env: str
    observe_on_scheduler: str | None
    buffer_max_age_seconds: float
    buffer_max_bytes: int


def _normalize_address(address: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    parsed = ipaddress.ip_address(address)
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


def _configured_networks():
    values = current_app.config.get("INFLUX_SINK_PROFILE_ALLOWED_NETWORKS", ())
    try:
        return tuple(ipaddress.ip_network(value, strict=False) for value in values)
    except ValueError as exc:
        raise InfluxDestinationNotAllowed("destination network policy is invalid") from exc


def _resolve(hostname: str, port: int) -> tuple[str, ...]:
    configured = current_app.config.get("INFLUX_SINK_PROFILE_RESOLVER")
    try:
        if configured is not None:
            addresses = tuple(str(value) for value in configured(hostname, port))
        else:
            addresses = tuple(
                sorted(
                    {
                        result[4][0]
                        for result in socket.getaddrinfo(
                            hostname,
                            port,
                            type=socket.SOCK_STREAM,
                        )
                    }
                )
            )
    except (OSError, ValueError, TypeError) as exc:
        raise InfluxDestinationNotAllowed("destination could not be safely resolved") from exc
    if not addresses:
        raise InfluxDestinationNotAllowed("destination could not be safely resolved")
    return addresses


def validate_destination(raw_url: str) -> ValidatedDestination:
    """Canonicalize and resolve a URL against the deployment egress policy."""

    try:
        parts = urlsplit(str(raw_url).strip())
        port = parts.port
    except (TypeError, ValueError) as exc:
        raise InfluxDestinationNotAllowed("destination URL is invalid") from exc

    allowed_schemes = {
        str(value).lower()
        for value in current_app.config.get(
            "INFLUX_SINK_PROFILE_ALLOWED_SCHEMES",
            ("http", "https"),
        )
    }
    scheme = parts.scheme.lower()
    if scheme not in allowed_schemes or scheme not in {"http", "https"}:
        raise InfluxDestinationNotAllowed("destination scheme is not allowed")
    if not parts.hostname or parts.username is not None or parts.password is not None:
        raise InfluxDestinationNotAllowed("destination authority is invalid")
    if parts.query or parts.fragment:
        raise InfluxDestinationNotAllowed("destination URL parameters are not allowed")

    hostname = parts.hostname.rstrip(".").lower()
    port = port or (443 if scheme == "https" else 80)
    allowed_hosts = {
        str(value).rstrip(".").lower()
        for value in current_app.config.get("INFLUX_SINK_PROFILE_ALLOWED_HOSTS", ())
    }
    networks = _configured_networks()

    try:
        literal_address = _normalize_address(hostname)
    except ValueError:
        literal_address = None
        if hostname not in allowed_hosts:
            raise InfluxDestinationNotAllowed("destination host is not allowed") from None

    if literal_address is not None:
        if hostname not in allowed_hosts:
            raise InfluxDestinationNotAllowed("destination host is not allowed")
        if (
            literal_address.is_unspecified
            or literal_address.is_multicast
            or literal_address in _CLOUD_METADATA_ADDRESSES
        ):
            raise InfluxDestinationNotAllowed("destination address is not allowed")
        if not any(literal_address in network for network in networks):
            raise InfluxDestinationNotAllowed("destination address is not allowed")
        addresses = (str(literal_address),)
    else:
        addresses = _resolve(hostname, port)

    parsed_addresses = []
    for address in addresses:
        try:
            parsed = _normalize_address(address)
        except ValueError as exc:
            raise InfluxDestinationNotAllowed("destination resolution was invalid") from exc
        if parsed.is_unspecified or parsed.is_multicast or parsed in _CLOUD_METADATA_ADDRESSES:
            raise InfluxDestinationNotAllowed("resolved destination is not allowed")
        if not any(parsed in network for network in networks):
            raise InfluxDestinationNotAllowed("resolved destination is outside allowed networks")
        parsed_addresses.append(str(parsed))

    host_for_url = f"[{hostname}]" if ":" in hostname else hostname
    default_port = 443 if scheme == "https" else 80
    authority = host_for_url if port == default_port else f"{host_for_url}:{port}"
    path = parts.path.rstrip("/")
    canonical_url = urlunsplit((scheme, authority, path, "", ""))
    return ValidatedDestination(
        url=canonical_url,
        hostname=hostname,
        port=port,
        addresses=tuple(parsed_addresses),
    )


