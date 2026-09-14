"""Pre-configuration names for physical devices."""

from __future__ import annotations

import re

from app.database import transaction
from app.domain.enums import DeviceType
from app.domain.errors import DeviceNicknameExists, InvalidHardwareId
from app.models.device_config import DeviceConfig
from app.models.device_registration import DeviceRegistration
from app.repositories.device_registrations import DeviceRegistrationRepository

_repository = DeviceRegistrationRepository()
# Keep in step with ``device_configs._HARDWARE_ID_PATTERN``.
_HARDWARE_ID_PATTERN = re.compile(r"^[0-9a-zA-Z]{1,10}$")


def _normalize_nickname(nickname: str) -> str:
    normalized = nickname.strip()
    if not normalized:
        raise ValueError("device name is required")
    return normalized


def _validate_hardware_id(hardware_id: str) -> None:
    if not _HARDWARE_ID_PATTERN.fullmatch(hardware_id):
        raise InvalidHardwareId(hardware_id)


def register(
    *,
    device_type: DeviceType,
    hardware_id: str,
    nickname: str,
) -> DeviceRegistration:
    """Create or update an operator name for a physical device identity."""
    _validate_hardware_id(hardware_id)
    normalized = _normalize_nickname(nickname)
    existing_name = _repository.get_by_nickname(normalized)
    existing_identity = _repository.get_by_identity(device_type, hardware_id)

    if existing_name is not None and (
        existing_name.device_type != device_type
        or existing_name.hardware_id != hardware_id
    ):
        raise DeviceNicknameExists(normalized)

    config = _repository.get_config_by_identity(device_type, hardware_id)
    if existing_identity is None:
        row = _repository.create(
            device_type=device_type,
            hardware_id=hardware_id,
            nickname=normalized,
            device_config_id=config.id if config is not None else None,
        )
        if config is not None:
            with transaction():
                config.nickname = normalized
        return row

    with transaction():
        existing_identity.nickname = normalized
        if config is not None:
            existing_identity.device_config_id = config.id
            config.nickname = normalized
    return existing_identity


def get_by_nickname(nickname: str) -> DeviceRegistration | None:
    return _repository.get_by_nickname(nickname.strip())


def list() -> list[DeviceRegistration]:  # noqa: A001 - resource API name
    return _repository.list()


def ensure_nickname_available(
    *,
    device_type: DeviceType,
    hardware_id: str,
    nickname: str | None,
) -> None:
    if nickname is None:
        return
    normalized = _normalize_nickname(nickname)
    existing = _repository.get_by_nickname(normalized)
    if existing is not None and (
        existing.device_type != device_type or existing.hardware_id != hardware_id
    ):
        raise DeviceNicknameExists(normalized)


def bind_config(config: DeviceConfig) -> DeviceRegistration | None:
    """Link a configured row to its registered name, if one exists."""
    registration = _repository.get_by_identity(DeviceType(config.device_type), config.hardware_id)
    if registration is None:
        if config.nickname:
            return register(
                device_type=DeviceType(config.device_type),
                hardware_id=config.hardware_id,
                nickname=config.nickname,
            )
        return None
    with transaction():
        registration.device_config_id = config.id
        registration.nickname = config.nickname or registration.nickname
        config.nickname = registration.nickname
    return registration


__all__ = [
    "bind_config",
    "ensure_nickname_available",
    "get_by_nickname",
    "list",
    "register",
]
