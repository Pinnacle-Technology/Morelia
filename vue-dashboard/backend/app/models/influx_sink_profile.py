"""Immutable Influx destination profiles and their append-only revisions."""

from datetime import UTC, datetime
from uuid import uuid4

from app.database import db


class InfluxSinkProfile(db.Model):
    """Stable profile identity and lifecycle pointer."""

    __tablename__ = "influx_sink_profiles"
    __table_args__ = (
        db.CheckConstraint("current_revision > 0", name="ck_influx_profiles_revision_positive"),
        db.Index("ix_influx_profiles_archived_name", "archived_at", "name"),
    )

    id = db.Column(db.String(64), primary_key=True, default=lambda: uuid4().hex)
    name = db.Column(
        db.String(255, collation="NOCASE"),
        nullable=False,
        unique=True,
        index=True,
    )
    current_revision = db.Column(db.Integer, nullable=False, default=1)
    archived_at = db.Column(db.DateTime(timezone=True), nullable=True)
    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )


class InfluxSinkProfileRevision(db.Model):
    """One immutable, non-secret configuration snapshot."""

    __tablename__ = "influx_sink_profile_revisions"
    __table_args__ = (
        db.CheckConstraint("revision > 0", name="ck_influx_profile_revisions_positive"),
        db.CheckConstraint(
            "config_schema_version > 0",
            name="ck_influx_profile_schema_version_positive",
        ),
        db.CheckConstraint(
            "buffer_max_age_seconds > 0",
            name="ck_influx_profile_buffer_age_positive",
        ),
        db.CheckConstraint(
            "buffer_max_bytes > 0",
            name="ck_influx_profile_buffer_bytes_positive",
        ),
        db.CheckConstraint(
            "observe_on_scheduler IS NULL OR observe_on_scheduler IN ('thread_pool', 'new_thread')",
            name="ck_influx_profile_observe_scheduler",
        ),
        db.CheckConstraint(
            "length(content_hash) = 64",
            name="ck_influx_profile_content_hash_length",
        ),
        db.Index(
            "ix_influx_profile_revisions_content_hash",
            "profile_id",
            "content_hash",
        ),
    )

    profile_id = db.Column(
        db.String(64),
        db.ForeignKey("influx_sink_profiles.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    revision = db.Column(db.Integer, primary_key=True)
    config_schema_version = db.Column(db.Integer, nullable=False, default=1)
    content_hash = db.Column(db.String(64), nullable=False)
    url = db.Column(db.String(2048), nullable=False)
    org = db.Column(db.String(255), nullable=False)
    bucket = db.Column(db.String(255), nullable=False)
    write_token_env = db.Column(db.String(128), nullable=False)
    read_token_env = db.Column(db.String(128), nullable=False)
    observe_on_scheduler = db.Column(db.String(32), nullable=True)
    buffer_max_age_seconds = db.Column(db.Float, nullable=False)
    buffer_max_bytes = db.Column(db.BigInteger, nullable=False)
    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
