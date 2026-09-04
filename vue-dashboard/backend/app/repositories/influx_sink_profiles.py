"""Persistence operations for append-only Influx profile revisions."""

from __future__ import annotations

from datetime import UTC, datetime

from app.database import db, transaction
from app.models.influx_sink_profile import (
    InfluxSinkProfile,
    InfluxSinkProfileRevision,
)


class RevisionAdvanceFailed(Exception):
    """The profile pointer changed or became archived during a revision write."""


class ArchiveAdvanceFailed(Exception):
    """The profile changed while an archive request was being applied."""


class RunProfileSelectionFailed(Exception):
    """A run selection ceased to be the active profile revision."""

    def __init__(self, profile_id: str, expected_revision: int):
        self.profile_id = profile_id
        self.expected_revision = expected_revision
        super().__init__(profile_id)


class InfluxSinkProfileRepository:
    def get(self, profile_id: str) -> InfluxSinkProfile | None:
        return db.session.get(InfluxSinkProfile, profile_id)

    def get_by_name(self, name: str) -> InfluxSinkProfile | None:
        query = db.select(InfluxSinkProfile).where(
            db.func.lower(InfluxSinkProfile.name) == name.lower()
        )
        return db.session.scalars(query).first()

    def get_revision(
        self,
        profile_id: str,
        revision: int,
    ) -> InfluxSinkProfileRevision | None:
        return db.session.get(InfluxSinkProfileRevision, (profile_id, revision))

    def assert_run_selections_current(
        self,
        selections: set[tuple[str, int]],
    ) -> None:
        """Serialize accepted selections against revise/archive CAS writes.

        The no-op conditional update is intentional: on SQLite it obtains the
        same write ordering as profile revision/archive updates without changing
        immutable profile content.
        """

        for profile_id, expected_revision in sorted(selections):
            statement = (
                db.update(InfluxSinkProfile)
                .where(
                    InfluxSinkProfile.id == profile_id,
                    InfluxSinkProfile.current_revision == expected_revision,
                    InfluxSinkProfile.archived_at.is_(None),
                )
                .values(current_revision=InfluxSinkProfile.current_revision)
            )
            if db.session.execute(statement).rowcount != 1:
                raise RunProfileSelectionFailed(profile_id, expected_revision)

    def list_page(
        self,
        *,
        include_archived: bool,
        page: int,
        page_size: int,
    ) -> tuple[list[tuple[InfluxSinkProfile, InfluxSinkProfileRevision]], int]:
        filters = []
        if not include_archived:
            filters.append(InfluxSinkProfile.archived_at.is_(None))

        count_query = db.select(db.func.count()).select_from(InfluxSinkProfile)
        query = db.select(InfluxSinkProfile, InfluxSinkProfileRevision).join(
            InfluxSinkProfileRevision,
            db.and_(
                InfluxSinkProfileRevision.profile_id == InfluxSinkProfile.id,
                InfluxSinkProfileRevision.revision == InfluxSinkProfile.current_revision,
            ),
        )
        if filters:
            count_query = count_query.where(*filters)
            query = query.where(*filters)
        query = (
            query.order_by(InfluxSinkProfile.updated_at.desc(), InfluxSinkProfile.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        return list(db.session.execute(query).tuples()), int(db.session.scalar(count_query) or 0)

    def create(
        self,
        *,
        name: str,
        revision_values: dict,
    ) -> tuple[InfluxSinkProfile, InfluxSinkProfileRevision]:
        with transaction():
            now = datetime.now(UTC)
            profile = InfluxSinkProfile(
                name=name,
                current_revision=1,
                created_at=now,
                updated_at=now,
            )
            db.session.add(profile)
            db.session.flush()
            revision = InfluxSinkProfileRevision(
                profile_id=profile.id,
                revision=1,
                created_at=now,
                **revision_values,
            )
            db.session.add(revision)
            db.session.flush()
        return profile, revision

    def append_revision(
        self,
        *,
        profile_id: str,
        expected_revision: int,
        revision_values: dict,
    ) -> tuple[InfluxSinkProfile, InfluxSinkProfileRevision]:
        next_revision = expected_revision + 1
        now = datetime.now(UTC)
        with transaction():
            statement = (
                db.update(InfluxSinkProfile)
                .where(
                    InfluxSinkProfile.id == profile_id,
                    InfluxSinkProfile.current_revision == expected_revision,
                    InfluxSinkProfile.archived_at.is_(None),
                )
                .values(current_revision=next_revision, updated_at=now)
            )
            if db.session.execute(statement).rowcount != 1:
                raise RevisionAdvanceFailed
            revision = InfluxSinkProfileRevision(
                profile_id=profile_id,
                revision=next_revision,
                created_at=now,
                **revision_values,
            )
            db.session.add(revision)
            db.session.flush()

        profile = self.get(profile_id)
        assert profile is not None
        return profile, revision

    def archive(
        self,
        *,
        profile_id: str,
        expected_revision: int,
    ) -> InfluxSinkProfile:
        now = datetime.now(UTC)
        with transaction():
            statement = (
                db.update(InfluxSinkProfile)
                .where(
                    InfluxSinkProfile.id == profile_id,
                    InfluxSinkProfile.current_revision == expected_revision,
                    InfluxSinkProfile.archived_at.is_(None),
                )
                .values(archived_at=now, updated_at=now)
            )
            if db.session.execute(statement).rowcount != 1:
                raise ArchiveAdvanceFailed

        profile = self.get(profile_id)
        assert profile is not None
        return profile
