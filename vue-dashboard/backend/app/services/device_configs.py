"""Port-bound device config service.

A *device config* is one physical device made runnable: ``device_type`` +
``hardware_id`` (its unique identity), a ``port`` attribute, a snapshot of
``parameters``, an optional ``nickname``, and optional ``source_template``
provenance. Configs move between ``FREE`` and ``CLAIMED`` as sessions attach and
detach. Parameters are validated through the typed registry, exactly like the
device-template library.

Mirrors ``app.services.device_templates`` in shape: module-level functions, a
module ``_repository``, and ``with transaction():`` for mutations. No Flask, no
HTTP — adapters map the typed errors below at their boundary.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import inspect

from app.database import db, transaction
from app.domain.enums import DeviceClaimState, DeviceType
from app.domain.errors import (
    DeviceClaimConflict,
    DeviceConfigExists,
    DeviceConfigNotFound,
    DeviceConfigNotFree,
    DeviceTemplateNotFound,
    InvalidHardwareId,
)
from app.models.device_config import DeviceConfig
from app.models.device_template import DeviceTemplate
from app.repositories.device_configs import DeviceConfigRepository
from app.services import device_registrations, device_templates
from app.services.registry import lookup_device

_repository = DeviceConfigRepository()

STARTING_CLAIM_LEASE_SECONDS = 120

# Plan decision C ("Physical identity", resolved 2026-07-02): hardware_id
# mirrors the reported FTDI serial verbatim, matched case-sensitively and
# exactly (no case-folding).
#
# Packet 10 ("hardware-id validation contract", resolved 2026-07-30): the legal
# value is 1-8 ASCII alphanumeric characters. Leading zeros and letter case are
# significant, so the value stays a string and is never normalized or coerced.
_HARDWARE_ID_PATTERN = re.compile(r"^[0-9a-zA-Z]{1,10}$")


def _canonical_parameters(device_type: DeviceType, raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate params for *device_type* through the registry, return canonical dict."""
    # Accept either an enum member or the raw string SQLAlchemy may hand back.
    type_key = DeviceType(device_type).value
    spec = lookup_device(type_key, raw or {})
    return spec.as_dict()


def create(
    *,
    device_type: DeviceType,
    hardware_id: str,
    port: str,
    parameters: Mapping[str, Any] | None = None,
    nickname: str | None = None,
    source_template: str | None = None,
    source_template_hash: str | None = None,
) -> DeviceConfig:
    """Persist a new port-bound device config after validating its parameters.

    Identity is ``device_type + hardware_id`` and must be unique; a duplicate
    raises ``DeviceConfigExists`` rather than surfacing a raw IntegrityError.

    ``hardware_id`` must be 1-8 ASCII alphanumeric characters; anything else
    raises ``InvalidHardwareId`` before the duplicate-identity check runs.
    """
    if not _HARDWARE_ID_PATTERN.fullmatch(hardware_id):
        raise InvalidHardwareId(hardware_id)

    device_registrations.ensure_nickname_available(
        device_type=device_type,
        hardware_id=hardware_id,
        nickname=nickname,
    )

    if _repository.get_by_identity(device_type, hardware_id) is not None:
        raise DeviceConfigExists(device_type.value, hardware_id)

    canonical = _canonical_parameters(device_type, parameters)
    row = _repository.create(
        device_type=device_type,
        hardware_id=hardware_id,
        port=port,
        parameters=canonical,
        nickname=nickname,
        source_template=source_template,
        source_template_hash=source_template_hash,
    )
    device_registrations.bind_config(row)
    return row


def create_from_template(
    template: DeviceTemplate,
    *,
    hardware_id: str,
    port: str,
    nickname: str | None = None,
) -> DeviceConfig:
    """Build a config by snapshot-copying *template*'s params.

    The params are copied by value, so later edits to the template never change
    this config. ``source_template`` records the provenance label.
    """
    return create(
        device_type=DeviceType(template.content["type"]),
        hardware_id=hardware_id,
        port=port,
        parameters=template.content.get("parameters", {}),
        nickname=nickname,
        source_template=template.file_path,
        source_template_hash=template.content_hash,
    )


def get_by_id(config_id: int) -> DeviceConfig | None:
    """Return a device config by primary key, or None."""
    return _repository.get(config_id)


def rename(
    *,
    device_type: DeviceType,
    hardware_id: str,
    nickname: str,
) -> DeviceConfig:
    """Assign the stable operator-facing name for a physical device config."""
    if not _HARDWARE_ID_PATTERN.fullmatch(hardware_id):
        raise InvalidHardwareId(hardware_id)
    normalized_nickname = nickname.strip()
    if not normalized_nickname:
        raise ValueError("device name is required")

    row = _repository.get_by_identity(device_type, hardware_id)
    if row is None:
        raise DeviceConfigNotFound(f"{device_type.value}:{hardware_id}")

    with transaction():
        row.nickname = normalized_nickname
    device_registrations.register(
        device_type=device_type,
        hardware_id=hardware_id,
        nickname=normalized_nickname,
    )
    return row


def list() -> list[DeviceConfig]:  # noqa: A001 - public API named to match device_templates
    """Return all device configs ordered by type then hardware id."""
    return _repository.list()


def find_by_hardware_id(hardware_id: str) -> list[DeviceConfig]:
    """Return every config that shares *hardware_id* (across device types)."""
    return _repository.find_by_hardware_id(hardware_id)


def _require(config_id: int) -> DeviceConfig:
    row = _repository.get(config_id)
    if row is None:
        raise DeviceConfigNotFound(config_id)
    return row


def _resolve_template(reference: str, device_type: DeviceType) -> DeviceTemplate:
    """Resolve a relink target by ``file_path`` first, then by portable name.

    Session templates store the ``device-templates/foo.toml`` form while the
    CLI's portable form is the bare name, and both reach this service.
    """
    try:
        template = device_templates.get_by_path(reference)
    except DeviceTemplateNotFound:
        template = None
    if template is None:
        template = device_templates.get_by_name(reference)
    if template is None:
        raise DeviceTemplateNotFound(reference)
    # A cross-type link would claim provenance the parameters can never satisfy:
    # they are validated against the *config's* type, not the template's.
    if template.type != DeviceType(device_type).value:
        raise ValueError(
            f"device template {template.name!r} is for {template.type}, "
            f"not {DeviceType(device_type).value}"
        )
    return template


def edit(
    config_id: int,
    *,
    parameters: Mapping[str, Any],
    update_source_template: bool,
    source_template: str | None = None,
) -> DeviceConfig:
    """Rewrite a config's parameters. Allowed only while the config is FREE.

    Editing a claimed device would desync a running manifest, so this raises
    ``DeviceConfigNotFree`` when the config is CLAIMED.

    Provenance (Option C — a dedicated history column):
      • update_source_template=True  → also rewrite the linked template's content
        and KEEP the live ``source_template`` link.
      • source_template=<path|name>  → relink to that template WITHOUT touching
        its content, preserving any previous link in ``source_template_history``.
        This is the "adopt a template's settings" direction: the template is
        authoritative and the config is brought into line with it.
      • neither → the config becomes "custom": the live link is severed and its
        old value is preserved in ``source_template_history``.

    The first two are mutually exclusive — they disagree about which side is the
    source of truth, so accepting both would make the outcome order-dependent.

    ``source_template_hash`` records the template revision the link was made
    against, exactly as ``create_from_template`` does. Callers relinking are
    responsible for passing parameters that actually match that template;
    nothing here forces them to agree, because a relink is also how an operator
    records "these settings came from here" for a hand-tuned config.
    """
    if update_source_template and source_template is not None:
        raise ValueError(
            "update_source_template and source_template are mutually exclusive"
        )

    row = _require(config_id)
    if row.claim_state is not DeviceClaimState.FREE:
        raise DeviceConfigNotFree(config_id)

    canonical = _canonical_parameters(row.device_type, parameters)

    # Resolve before opening the transaction so an unknown template aborts the
    # edit outright rather than leaving the parameters rewritten but unlinked.
    relink_target = (
        _resolve_template(source_template, row.device_type) if source_template is not None else None
    )

    with transaction():
        # Set params first so the nested template write below commits both the
        # config row and the template in one flush (keeps the branch atomic).
        row.parameters = canonical
        if relink_target is not None:
            if row.source_template is not None and row.source_template != relink_target.file_path:
                row.source_template_history = row.source_template
            row.source_template = relink_target.file_path
            row.source_template_hash = relink_target.content_hash
        elif update_source_template:
            if row.source_template is not None:
                # Push the edited params back into the (mutable) library template.
                template = device_templates.get_by_path(row.source_template)
                if template is None:
                    raise ValueError(f"source device template not found: {row.source_template}")
                updated_template = device_templates.update(
                    template.name,
                    {"type": DeviceType(row.device_type).value, "parameters": canonical},
                )
                row.source_template_hash = updated_template.content_hash
        elif row.source_template is not None:
            # Diverged from its template: sever the live link, keep the breadcrumb.
            row.source_template_history = row.source_template
            row.source_template = None
            row.source_template_hash = None

    return row


def delete(config_id: int) -> None:
    """Delete a config. Allowed only while FREE."""
    row = _require(config_id)
    if row.claim_state is not DeviceClaimState.FREE:
        raise DeviceConfigNotFree(config_id)
    _repository.delete(row)


def claim(
    config_id: int,
    session_id: int,
    *,
    force: bool = False,
    starting: bool = False,
    lease_seconds: int = STARTING_CLAIM_LEASE_SECONDS,
) -> DeviceConfig:
    """Attach *session_id* to a config, optionally as a leased startup claim."""
    row = _require(config_id)
    now = datetime.now(UTC)
    expiry = row.claim_expires_at
    if expiry is not None and expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=UTC)
    if row.claim_state is DeviceClaimState.STARTING and expiry is not None and expiry <= now:
        with transaction():
            row.claim_state = DeviceClaimState.FREE
            row.claimed_session_id = None
            row.claim_expires_at = None
    if row.claim_state is not DeviceClaimState.FREE:
        if row.claimed_session_id == session_id:
            if starting and row.claim_state is DeviceClaimState.STARTING:
                with transaction():
                    row.claim_expires_at = now + timedelta(seconds=lease_seconds)
            return row
        if not force:
            raise DeviceClaimConflict(config_id, claimed_session_id=row.claimed_session_id)

    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be greater than zero")

    with transaction():
        row.claim_state = DeviceClaimState.STARTING if starting else DeviceClaimState.CLAIMED
        row.claimed_session_id = session_id
        row.claim_expires_at = (
            datetime.now(UTC) + timedelta(seconds=lease_seconds) if starting else None
        )

    return row


def activate(config_id: int, session_id: int) -> DeviceConfig:
    """Promote a valid startup lease to a non-expiring running claim."""
    row = _require(config_id)
    if row.claimed_session_id != session_id or row.claim_state not in (
        DeviceClaimState.STARTING,
        DeviceClaimState.CLAIMED,
    ):
        raise DeviceClaimConflict(config_id, claimed_session_id=row.claimed_session_id)
    with transaction():
        row.claim_state = DeviceClaimState.CLAIMED
        row.claim_expires_at = None
    return row


def release_expired_starting_claims(*, now: datetime | None = None) -> int:
    """Release startup claims whose lease elapsed before the host became ready."""
    if "claim_expires_at" not in {
        column["name"] for column in inspect(db.engine).get_columns("device_configs")
    }:
        # A migration command constructs the app before applying its pending
        # migrations.  Do not make that bootstrap path query a column that is
        # about to be created.
        return 0
    cutoff = now or datetime.now(UTC)
    with transaction():
        rows = db.session.scalars(
            db.select(DeviceConfig).where(
                DeviceConfig.claim_state == DeviceClaimState.STARTING,
                DeviceConfig.claim_expires_at.is_not(None),
                DeviceConfig.claim_expires_at <= cutoff,
            )
        ).all()
        for row in rows:
            row.claim_state = DeviceClaimState.FREE
            row.claimed_session_id = None
            row.claim_expires_at = None
    return len(rows)


def release(config_id: int) -> DeviceConfig:
    """Detach any session and move the config back to FREE.

    Idempotent: releasing an already-FREE config is a harmless no-op, so teardown
    and crash-recovery paths can call it blindly.
    """
    row = _require(config_id)
    if row.claim_state is DeviceClaimState.FREE:
        return row

    with transaction():
        row.claim_state = DeviceClaimState.FREE
        row.claimed_session_id = None
        row.claim_expires_at = None

    return row


__all__ = [
    "InvalidHardwareId",
    "activate",
    "claim",
    "create",
    "create_from_template",
    "delete",
    "edit",
    "find_by_hardware_id",
    "get_by_id",
    "list",
    "rename",
    "release",
    "release_expired_starting_claims",
]
