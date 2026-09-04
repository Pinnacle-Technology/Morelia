"""Current disposable database schema.

The application intentionally starts from a clean database format. Device
templates are files under ``instance/device-templates`` and are not stored in
SQLite; the registry stores only identity and reconciliation metadata.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "experiments",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(255, collation="NOCASE"), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("name", name="uq_experiments_name"),
    )
    op.create_index("ix_experiments_name", "experiments", ["name"], unique=False)

    op.create_table(
        "influx_sink_profiles",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(255, collation="NOCASE"), nullable=False),
        sa.Column("current_revision", sa.Integer(), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "current_revision > 0",
            name="ck_influx_profiles_revision_positive",
        ),
        sa.UniqueConstraint("name", name="uq_influx_sink_profiles_name"),
    )
    op.create_index(
        "ix_influx_sink_profiles_name",
        "influx_sink_profiles",
        ["name"],
        unique=False,
    )
    op.create_index(
        "ix_influx_profiles_archived_name",
        "influx_sink_profiles",
        ["archived_at", "name"],
        unique=False,
    )

    op.create_table(
        "influx_sink_profile_revisions",
        sa.Column("profile_id", sa.String(64), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("config_schema_version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("url", sa.String(2048), nullable=False),
        sa.Column("org", sa.String(255), nullable=False),
        sa.Column("bucket", sa.String(255), nullable=False),
        sa.Column("write_token_env", sa.String(128), nullable=False),
        sa.Column("read_token_env", sa.String(128), nullable=False),
        sa.Column("observe_on_scheduler", sa.String(32), nullable=True),
        sa.Column("buffer_max_age_seconds", sa.Float(), nullable=False),
        sa.Column("buffer_max_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "revision > 0",
            name="ck_influx_profile_revisions_positive",
        ),
        sa.CheckConstraint(
            "config_schema_version > 0",
            name="ck_influx_profile_schema_version_positive",
        ),
        sa.CheckConstraint(
            "buffer_max_age_seconds > 0",
            name="ck_influx_profile_buffer_age_positive",
        ),
        sa.CheckConstraint(
            "buffer_max_bytes > 0",
            name="ck_influx_profile_buffer_bytes_positive",
        ),
        sa.CheckConstraint(
            "observe_on_scheduler IS NULL OR observe_on_scheduler IN ('thread_pool', 'new_thread')",
            name="ck_influx_profile_observe_scheduler",
        ),
        sa.CheckConstraint(
            "length(content_hash) = 64",
            name="ck_influx_profile_content_hash_length",
        ),
        sa.ForeignKeyConstraint(
            ["profile_id"],
            ["influx_sink_profiles.id"],
            name="fk_influx_profile_revisions_profile",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "profile_id",
            "revision",
            name="pk_influx_sink_profile_revisions",
        ),
    )
    op.create_index(
        "ix_influx_profile_revisions_content_hash",
        "influx_sink_profile_revisions",
        ["profile_id", "content_hash"],
        unique=False,
    )

    op.create_table(
        "sessions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="preparing"),
        sa.Column("policy", sa.String(20), nullable=False, server_default="recommend"),
        sa.Column(
            "experiment_id",
            sa.String(64),
            sa.ForeignKey("experiments.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("schedule", sa.JSON(), nullable=True),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schedule_claim_token", sa.String(64), nullable=True),
        sa.Column("schedule_claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancellation_details", sa.JSON(), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("device_flows", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("command_in_flight", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("command_id", sa.String(64), nullable=True),
        sa.Column("dataflow_id", sa.String(64), nullable=True),
        sa.Column("watchdog_id", sa.String(64), nullable=True),
        sa.Column("runtime_port", sa.Integer(), nullable=True),
        sa.Column("runtime_token", sa.String(128), nullable=True),
        sa.Column("source_template_id", sa.String(64), nullable=True),
        sa.Column("source_template_name", sa.String(255), nullable=True),
        sa.Column("source_template_ref", sa.String(1024), nullable=True),
        sa.Column("source_template_hash", sa.String(64), nullable=True),
        sa.Column("source_template_snapshot", sa.JSON(), nullable=True),
        sa.Column("creation_request_key", sa.String(128), nullable=True),
        sa.Column("creation_request_fingerprint", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_sessions_source_template_history",
        "sessions",
        ["source_template_id", "created_at", "id"],
    )
    op.create_index("ix_sessions_scheduled_for", "sessions", ["scheduled_for"])
    op.create_index(
        "ix_sessions_schedule_claim_expires_at",
        "sessions",
        ["schedule_claim_expires_at"],
    )
    op.create_index(
        "ix_sessions_status_scheduled_for",
        "sessions",
        ["status", "scheduled_for"],
    )
    op.create_index(
        "ix_sessions_creation_request_key",
        "sessions",
        ["creation_request_key"],
        unique=True,
    )

    op.create_table(
        "session_sink_bindings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("session_id", sa.Integer(), nullable=False),
        sa.Column("flow_index", sa.Integer(), nullable=False),
        sa.Column("sink_index", sa.Integer(), nullable=False),
        sa.Column("sink_name", sa.String(255), nullable=False),
        sa.Column("profile_id", sa.String(64), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("profile_content_hash", sa.String(64), nullable=False),
        sa.Column("writer_parameters", sa.JSON(), nullable=False),
        sa.Column("reader_parameters", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("flow_index >= 0", name="ck_session_sink_bindings_flow_index"),
        sa.CheckConstraint("sink_index >= 0", name="ck_session_sink_bindings_sink_index"),
        sa.CheckConstraint(
            "profile_revision > 0",
            name="ck_session_sink_bindings_profile_revision",
        ),
        sa.CheckConstraint(
            "length(profile_content_hash) = 64",
            name="ck_session_sink_bindings_profile_hash_length",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["sessions.id"],
            name="fk_session_sink_bindings_session",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["profile_id", "profile_revision"],
            [
                "influx_sink_profile_revisions.profile_id",
                "influx_sink_profile_revisions.revision",
            ],
            name="fk_session_sink_bindings_profile_revision",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "session_id",
            "flow_index",
            "sink_index",
            name="uq_session_sink_bindings_coordinate",
        ),
    )
    op.create_index(
        "ix_session_sink_bindings_session_id",
        "session_sink_bindings",
        ["session_id"],
        unique=False,
    )

    op.create_table(
        "session_notes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "session_id",
            sa.Integer(),
            sa.ForeignKey("sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("show_timestamp", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_session_notes_session_created",
        "session_notes",
        ["session_id", "created_at", "id"],
    )

    op.create_table(
        "session_activity_entries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("activity_id", sa.String(64), nullable=False),
        sa.Column(
            "session_id",
            sa.Integer(),
            sa.ForeignKey("sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dataflow_id", sa.String(64), nullable=True),
        sa.Column("kind", sa.String(64), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("title", sa.String(160), nullable=False),
        sa.Column("summary", sa.String(1024), nullable=False),
        sa.Column("source_type", sa.String(64), nullable=False),
        sa.Column("source_id", sa.String(128), nullable=False),
        sa.Column("operation_id", sa.String(64), nullable=True),
        sa.Column("incident_id", sa.String(64), nullable=True),
        sa.Column("gap_id", sa.String(64), nullable=True),
        sa.Column("command_id", sa.String(64), nullable=True),
        sa.Column("recovery_id", sa.String(64), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("activity_id", name="uq_session_activity_activity_id"),
        sa.UniqueConstraint(
            "session_id",
            "source_type",
            "source_id",
            "kind",
            name="uq_session_activity_session_source_kind",
        ),
    )
    for name, columns in (
        ("ix_session_activity_activity_id", ["activity_id"]),
        ("ix_session_activity_session_id", ["session_id"]),
        ("ix_session_activity_dataflow_id", ["dataflow_id"]),
        ("ix_session_activity_kind", ["kind"]),
        ("ix_session_activity_category", ["category"]),
        ("ix_session_activity_severity", ["severity"]),
        ("ix_session_activity_operation_id", ["operation_id"]),
        ("ix_session_activity_incident_id", ["incident_id"]),
        ("ix_session_activity_gap_id", ["gap_id"]),
        ("ix_session_activity_command_id", ["command_id"]),
        ("ix_session_activity_recovery_id", ["recovery_id"]),
        (
            "ix_session_activity_session_occurred",
            ["session_id", "occurred_at", "id"],
        ),
    ):
        op.create_index(name, "session_activity_entries", columns)

    op.create_table(
        "incidents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("incident_id", sa.String(64), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("dataflow_id", sa.String(64), nullable=False),
        sa.Column("device_id", sa.String(255), nullable=True),
        sa.Column("sink_id", sa.String(255), nullable=True),
        sa.Column("runtime_id", sa.String(64), nullable=True),
        sa.Column("operation_id", sa.String(64), nullable=True),
        sa.Column("recovery_id", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("reason", sa.String(255), nullable=False),
        sa.Column("policy", sa.String(32), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", sa.String(255), nullable=True),
        sa.Column("acknowledgement_note", sa.Text(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution", sa.String(255), nullable=True),
    )
    _create_incident_indexes()

    op.create_table(
        "recovery_gaps",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("gap_id", sa.String(64), nullable=False),
        sa.Column("incident_id", sa.String(64), sa.ForeignKey("incidents.incident_id"), nullable=True),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("dataflow_id", sa.String(64), nullable=False),
        sa.Column("device_id", sa.String(255), nullable=True),
        sa.Column("sink_id", sa.String(255), nullable=True),
        sa.Column("operation_id", sa.String(64), nullable=True),
        sa.Column("recovery_id", sa.String(64), nullable=True),
        sa.Column("previous_segment_id", sa.String(128), nullable=True),
        sa.Column("next_segment_id", sa.String(128), nullable=True),
        sa.Column("reason", sa.String(255), nullable=False),
        sa.Column("policy", sa.String(32), nullable=True),
        sa.Column("confidence", sa.String(32), nullable=False),
        sa.Column("gap_start", sa.JSON(), nullable=True),
        sa.Column("gap_end", sa.JSON(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        # Versioned boundary payload: boundaries no longer overload the
        # offset-only segment-id strings above.
        sa.Column("boundary_kind", sa.String(32), nullable=True),
        sa.Column("boundary_version", sa.Integer(), nullable=True),
        sa.Column("output_id", sa.String(36), nullable=True),
        sa.Column("previous_output_id", sa.String(36), nullable=True),
        sa.Column("next_output_id", sa.String(36), nullable=True),
        sa.Column("pre_offset", sa.JSON(), nullable=True),
        sa.Column("post_offset", sa.JSON(), nullable=True),
        sa.Column("boundary_payload", sa.JSON(), nullable=True),
    )
    _create_recovery_gap_indexes()

    op.create_table(
        "runtime_manifests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("hash", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(8), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("content", sa.JSON(), nullable=False),
    )
    op.create_index("ix_runtime_manifests_hash", "runtime_manifests", ["hash"], unique=True)
    op.create_index("ix_runtime_manifests_session_id", "runtime_manifests", ["session_id"])

    # One logical sink output owns several linked physical components, each with
    # explicit acquisition, artifact/finalization, delivery, and loss state.
    op.create_table(
        "output_files",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("output_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=True),
        sa.Column("dataflow_id", sa.String(255), nullable=False),
        sa.Column("device_id", sa.String(255), nullable=True),
        sa.Column("sink_id", sa.String(255), nullable=True),
        sa.Column("sink_type", sa.String(64), nullable=False),
        sa.Column("path", sa.String(1024), nullable=False),
        sa.Column("schema_hash", sa.String(64), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("byte_offset", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("row_offset", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Lifecycle. No server defaults: the ORM supplies values for new rows and
        # stays the single source of truth.
        sa.Column("logical_sink_id", sa.String(36), nullable=False),
        sa.Column("segment_index", sa.Integer(), nullable=False),
        sa.Column("previous_output_id", sa.String(36), nullable=True),
        sa.Column("final_output_id", sa.String(36), nullable=True),
        sa.Column("acquisition_state", sa.String(16), nullable=False),
        sa.Column("artifact_state", sa.String(16), nullable=False),
        sa.Column("termination_reason", sa.String(32), nullable=True),
        sa.Column("delivery_state", sa.String(16), nullable=True),
        sa.Column("byte_loss", sa.Integer(), nullable=False),
        sa.Column("sample_loss", sa.Integer(), nullable=False),
        sa.Column("finalization_id", sa.String(64), nullable=True),
        sa.Column("finalizer_fence_token", sa.BigInteger(), nullable=True),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("logical_sink_id", "segment_index", name="uq_output_files_logical_segment"),
        sa.UniqueConstraint("logical_sink_id", "path", name="uq_output_files_logical_path"),
        sa.CheckConstraint(
            "previous_output_id IS NULL OR previous_output_id <> output_id",
            name="ck_output_files_no_self_predecessor",
        ),
    )
    op.create_index("ix_output_files_output_id", "output_files", ["output_id"], unique=True)
    op.create_index("ix_output_files_path", "output_files", ["path"], unique=True)
    op.create_index("ix_output_files_session_id", "output_files", ["session_id"])
    op.create_index("ix_output_files_dataflow_id", "output_files", ["dataflow_id"])
    op.create_index("ix_output_files_logical_sink_id", "output_files", ["logical_sink_id"])

    op.create_table(
        "operations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("operation_id", sa.String(64), nullable=False),
        sa.Column("request_key", sa.String(128), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("dataflow_id", sa.String(64), nullable=False),
        sa.Column("scope", sa.String(16), nullable=False),
        sa.Column("target_device_id", sa.String(255), nullable=True),
        sa.Column("command", sa.String(64), nullable=False),
        sa.Column("request_id", sa.String(64), nullable=True),
        sa.Column("command_id", sa.String(64), nullable=False),
        sa.Column("watchdog_id", sa.String(64), nullable=True),
        sa.Column("recovery_id", sa.String(64), nullable=True),
        sa.Column("manifest_hash", sa.String(64), nullable=True),
        sa.Column("state", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("queued_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("running_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verifying_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.String(1024), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("resolved_by", sa.String(255), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_note", sa.String(1024), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("runtime_id", sa.String(64), nullable=True),
        sa.UniqueConstraint("dataflow_id", "request_key", name="uq_operations_dataflow_request_key"),
    )
    _create_operation_indexes()

    op.create_table(
        "runtime_ownerships",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("runtime_id", sa.String(64), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("dataflow_id", sa.String(64), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("token", sa.String(128), nullable=True),
        sa.Column("state", sa.String(16), nullable=False, server_default="starting"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("adopted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("watchdog_id", sa.String(64), nullable=True),
        sa.Column("watchdog_token_hash", sa.String(128), nullable=True),
        sa.Column("watchdog_pid", sa.Integer(), nullable=True),
        sa.Column("watchdog_control_port", sa.Integer(), nullable=True),
        sa.Column("watchdog_state", sa.String(16), nullable=True),
        sa.Column("watchdog_outbox_path", sa.String(255), nullable=True),
        sa.Column("watchdog_last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watchdog_adopted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watchdog_exit_details", sa.JSON(), nullable=True),
    )
    _create_runtime_ownership_indexes()

    op.create_table(
        "backend_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("dataflow_id", sa.String(64), nullable=False),
        sa.Column("runtime_id", sa.String(64), nullable=True),
        sa.Column("watchdog_id", sa.String(64), nullable=True),
        sa.Column("report_id", sa.String(64), nullable=True),
        sa.Column("recovery_id", sa.String(64), nullable=True),
        sa.Column("sequence", sa.Integer(), nullable=True),
        sa.Column("phase", sa.String(32), nullable=True),
        sa.Column("comms", sa.String(32), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("dataflow_id", "sequence", name="uq_backend_events_dataflow_sequence"),
        sa.UniqueConstraint("report_id", name="uq_backend_events_report_id"),
    )
    op.create_index("ix_backend_events_session_id", "backend_events", ["session_id"])
    op.create_index("ix_backend_events_dataflow_id", "backend_events", ["dataflow_id"])
    op.create_index("ix_backend_events_recovery_id", "backend_events", ["recovery_id"])
    op.create_index("ix_backend_events_watchdog_id", "backend_events", ["watchdog_id"])
    op.create_index("ix_backend_events_session_id_id", "backend_events", ["session_id", "id"])

    op.create_table(
        "device_configs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("device_type", sa.String(64), nullable=False),
        # The FTDI EEPROM serial with the Windows channel letter already stripped
        # by discovery — 1-8 digits. Stays a string: leading zeros are
        # significant (``0002`` is a real serial).
        sa.Column("hardware_id", sa.String(255), nullable=False),
        sa.Column("port", sa.String(255), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("color", sa.String(7), nullable=False),
        sa.Column("nickname", sa.String(255), nullable=True),
        sa.Column("source_template", sa.String(1024), nullable=True),
        sa.Column("source_template_hash", sa.String(64), nullable=True),
        sa.Column("source_template_history", sa.String(1024), nullable=True),
        sa.Column("claim_state", sa.String(16), nullable=False, server_default="free"),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_session_id", sa.Integer(), sa.ForeignKey("sessions.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("device_type", "hardware_id", name="uq_device_configs_device_type_hardware_id"),
    )
    op.create_index("ix_device_configs_device_type", "device_configs", ["device_type"])
    op.create_index("ix_device_configs_hardware_id", "device_configs", ["hardware_id"])
    op.create_index("ix_device_configs_claim_state", "device_configs", ["claim_state"])
    op.create_index("ix_device_configs_claim_expires_at", "device_configs", ["claim_expires_at"])
    op.create_index("ix_device_configs_claimed_session_id", "device_configs", ["claimed_session_id"])

    op.create_table(
        "device_seen",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("physical_device_id", sa.String(255), nullable=False),
        sa.Column("scan_id", sa.String(64), nullable=False),
        sa.Column("seen_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("port", sa.String(255), nullable=False),
        sa.Column("availability", sa.String(16), nullable=False),
        sa.Column("display_label", sa.String(255), nullable=True),
        sa.Column("warnings_json", sa.JSON(), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=True),
    )
    op.create_index("ix_device_seen_physical_device_id", "device_seen", ["physical_device_id"])
    op.create_index("ix_device_seen_scan_id", "device_seen", ["scan_id"])

    op.create_table(
        "session_templates",
        sa.Column("template_id", sa.String(64), primary_key=True),
        sa.Column("relative_path", sa.String(1024), nullable=False),
        sa.Column("registered_hash", sa.String(64), nullable=False),
        sa.Column("observed_hash", sa.String(64), nullable=True),
        sa.Column("filesystem_identity", sa.String(255), nullable=True),
        sa.Column("lifecycle_state", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("integrity_state", sa.String(32), nullable=False, server_default="UNKNOWN"),
        sa.Column("lineage_parent_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["lineage_parent_id"], ["session_templates.template_id"],
            name="fk_session_templates_lineage_parent_id", ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN ('PENDING', 'ACTIVE', 'ARCHIVED', 'REPLACED')",
            name="ck_session_templates_lifecycle_state",
        ),
        sa.CheckConstraint(
            "integrity_state IN ('UNKNOWN', 'MATCHED', 'CHANGED', 'MISSING', 'INVALID')",
            name="ck_session_templates_integrity_state",
        ),
    )
    op.create_index("ix_session_templates_relative_path", "session_templates", ["relative_path"])
    op.create_index("ix_session_templates_registered_hash", "session_templates", ["registered_hash"])
    op.create_index("ix_session_templates_observed_hash", "session_templates", ["observed_hash"])
    op.create_index("ix_session_templates_lifecycle_state", "session_templates", ["lifecycle_state"])
    op.create_index("ix_session_templates_integrity_state", "session_templates", ["integrity_state"])
    op.create_index("ix_session_templates_lineage_parent_id", "session_templates", ["lineage_parent_id"])
    op.create_index(
        "uq_session_templates_current_path",
        "session_templates",
        ["relative_path"],
        unique=True,
        sqlite_where=sa.text("lifecycle_state != 'REPLACED'"),
    )
    op.create_index(
        "uq_session_templates_current_hash",
        "session_templates",
        ["registered_hash"],
        unique=True,
        sqlite_where=sa.text("lifecycle_state != 'REPLACED'"),
    )

    op.create_table(
        "session_template_dependencies",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("template_id", sa.String(64), nullable=False),
        sa.Column("relative_path", sa.String(1024), nullable=False),
        sa.Column("resolved_hash", sa.String(64), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["template_id"], ["session_templates.template_id"],
            name="fk_session_template_dependencies_template_id", ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "template_id", "relative_path", name="uq_session_template_dependencies_template_path"
        ),
    )
    op.create_index("ix_session_template_dependencies_template_id", "session_template_dependencies", ["template_id"])
    op.create_index("ix_session_template_dependencies_relative_path", "session_template_dependencies", ["relative_path"])
    op.create_index("ix_session_template_dependencies_fingerprint", "session_template_dependencies", ["fingerprint"])

    op.create_table(
        "device_registrations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("device_type", sa.String(64), nullable=False),
        sa.Column("hardware_id", sa.String(255), nullable=False),
        sa.Column("nickname", sa.String(255), nullable=False),
        sa.Column("device_config_id", sa.Integer(), sa.ForeignKey("device_configs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("device_type", "hardware_id", name="uq_device_registrations_device_type_hardware_id"),
        sa.UniqueConstraint("nickname", name="uq_device_registrations_nickname"),
        sa.UniqueConstraint("device_config_id", name="uq_device_registrations_device_config_id"),
    )
    op.create_index("ix_device_registrations_device_type", "device_registrations", ["device_type"])
    op.create_index("ix_device_registrations_hardware_id", "device_registrations", ["hardware_id"])
    op.create_index("ix_device_registrations_nickname", "device_registrations", ["nickname"])
    op.create_index("ix_device_registrations_device_config_id", "device_registrations", ["device_config_id"])

def _create_incident_indexes() -> None:
    op.create_index("ix_incidents_incident_id", "incidents", ["incident_id"], unique=True)
    op.create_index("ix_incidents_session_id", "incidents", ["session_id"])
    op.create_index("ix_incidents_dataflow_id", "incidents", ["dataflow_id"])
    op.create_index("ix_incidents_device_id", "incidents", ["device_id"])
    op.create_index("ix_incidents_recovery_id", "incidents", ["recovery_id"])
    op.create_index("ix_incidents_status", "incidents", ["status"])


def _create_recovery_gap_indexes() -> None:
    op.create_index("ix_recovery_gaps_gap_id", "recovery_gaps", ["gap_id"], unique=True)
    op.create_index("ix_recovery_gaps_incident_id", "recovery_gaps", ["incident_id"])
    op.create_index("ix_recovery_gaps_session_id", "recovery_gaps", ["session_id"])
    op.create_index("ix_recovery_gaps_dataflow_id", "recovery_gaps", ["dataflow_id"])
    op.create_index("ix_recovery_gaps_device_id", "recovery_gaps", ["device_id"])
    op.create_index("ix_recovery_gaps_recovery_id", "recovery_gaps", ["recovery_id"])
    op.create_index("ix_recovery_gaps_confidence", "recovery_gaps", ["confidence"])
    op.create_index("ix_recovery_gaps_boundary_kind", "recovery_gaps", ["boundary_kind"])
    op.create_index("ix_recovery_gaps_output_id", "recovery_gaps", ["output_id"])


def _create_operation_indexes() -> None:
    op.create_index("ix_operations_operation_id", "operations", ["operation_id"], unique=True)
    op.create_index("ix_operations_session_id", "operations", ["session_id"])
    op.create_index("ix_operations_dataflow_id", "operations", ["dataflow_id"])
    op.create_index("ix_operations_scope", "operations", ["scope"])
    op.create_index("ix_operations_target_device_id", "operations", ["target_device_id"])
    op.create_index("ix_operations_command_id", "operations", ["command_id"])
    op.create_index("ix_operations_recovery_id", "operations", ["recovery_id"])
    op.create_index("ix_operations_state", "operations", ["state"])
    op.create_index(
        "ux_operations_active_dataflow_scope",
        "operations",
        ["dataflow_id"],
        unique=True,
        sqlite_where=sa.text(
            "scope = 'dataflow' AND state IN "
            "('queued', 'claimed', 'dispatched', 'running', 'verifying')"
        ),
    )
    op.create_index(
        "ux_operations_active_stream_scope",
        "operations",
        ["dataflow_id", "target_device_id"],
        unique=True,
        sqlite_where=sa.text(
            "scope = 'stream' AND state IN "
            "('queued', 'claimed', 'dispatched', 'running', 'verifying')"
        ),
    )
    op.create_index("ix_operations_runtime_id", "operations", ["runtime_id"])


def _create_runtime_ownership_indexes() -> None:
    op.create_index("ix_runtime_ownerships_runtime_id", "runtime_ownerships", ["runtime_id"], unique=True)
    op.create_index("ix_runtime_ownerships_session_id", "runtime_ownerships", ["session_id"])
    op.create_index("ix_runtime_ownerships_dataflow_id", "runtime_ownerships", ["dataflow_id"])
    op.create_index("ix_runtime_ownerships_manifest_hash", "runtime_ownerships", ["manifest_hash"])
    op.create_index("ix_runtime_ownerships_port", "runtime_ownerships", ["port"])
    op.create_index("ix_runtime_ownerships_state", "runtime_ownerships", ["state"])
    op.create_index("ix_runtime_ownerships_watchdog_id", "runtime_ownerships", ["watchdog_id"])
    op.create_index(
        "ix_runtime_ownerships_watchdog_control_port",
        "runtime_ownerships",
        ["watchdog_control_port"],
    )
    op.create_index("ix_runtime_ownerships_watchdog_state", "runtime_ownerships", ["watchdog_state"])


def downgrade() -> None:
    for index, table in (
        ("ix_device_registrations_device_config_id", "device_registrations"),
        ("ix_device_registrations_nickname", "device_registrations"),
        ("ix_device_registrations_hardware_id", "device_registrations"),
        ("ix_device_registrations_device_type", "device_registrations"),
        ("ix_session_template_dependencies_fingerprint", "session_template_dependencies"),
        ("ix_session_template_dependencies_relative_path", "session_template_dependencies"),
        ("ix_session_template_dependencies_template_id", "session_template_dependencies"),
        ("uq_session_templates_current_hash", "session_templates"),
        ("uq_session_templates_current_path", "session_templates"),
        ("ix_session_templates_lineage_parent_id", "session_templates"),
        ("ix_session_templates_integrity_state", "session_templates"),
        ("ix_session_templates_lifecycle_state", "session_templates"),
        ("ix_session_templates_observed_hash", "session_templates"),
        ("ix_session_templates_registered_hash", "session_templates"),
        ("ix_session_templates_relative_path", "session_templates"),
        ("ix_device_seen_scan_id", "device_seen"),
        ("ix_device_seen_physical_device_id", "device_seen"),
        ("ix_device_configs_claimed_session_id", "device_configs"),
        ("ix_device_configs_claim_expires_at", "device_configs"),
        ("ix_device_configs_claim_state", "device_configs"),
        ("ix_device_configs_hardware_id", "device_configs"),
        ("ix_device_configs_device_type", "device_configs"),
        ("ix_backend_events_session_id_id", "backend_events"),
        ("ix_backend_events_watchdog_id", "backend_events"),
        ("ix_backend_events_recovery_id", "backend_events"),
        ("ix_backend_events_dataflow_id", "backend_events"),
        ("ix_backend_events_session_id", "backend_events"),
        ("ix_runtime_ownerships_watchdog_state", "runtime_ownerships"),
        ("ix_runtime_ownerships_watchdog_control_port", "runtime_ownerships"),
        ("ix_runtime_ownerships_watchdog_id", "runtime_ownerships"),
        ("ix_runtime_ownerships_state", "runtime_ownerships"),
        ("ix_runtime_ownerships_port", "runtime_ownerships"),
        ("ix_runtime_ownerships_manifest_hash", "runtime_ownerships"),
        ("ix_runtime_ownerships_dataflow_id", "runtime_ownerships"),
        ("ix_runtime_ownerships_session_id", "runtime_ownerships"),
        ("ix_runtime_ownerships_runtime_id", "runtime_ownerships"),
        ("ix_operations_runtime_id", "operations"),
        ("ux_operations_active_stream_scope", "operations"),
        ("ux_operations_active_dataflow_scope", "operations"),
        ("ix_operations_state", "operations"),
        ("ix_operations_recovery_id", "operations"),
        ("ix_operations_command_id", "operations"),
        ("ix_operations_target_device_id", "operations"),
        ("ix_operations_scope", "operations"),
        ("ix_operations_dataflow_id", "operations"),
        ("ix_operations_session_id", "operations"),
        ("ix_operations_operation_id", "operations"),
        ("ix_output_files_logical_sink_id", "output_files"),
        ("ix_output_files_dataflow_id", "output_files"),
        ("ix_output_files_session_id", "output_files"),
        ("ix_output_files_path", "output_files"),
        ("ix_output_files_output_id", "output_files"),
        ("ix_runtime_manifests_session_id", "runtime_manifests"),
        ("ix_runtime_manifests_hash", "runtime_manifests"),
        ("ix_recovery_gaps_output_id", "recovery_gaps"),
        ("ix_recovery_gaps_boundary_kind", "recovery_gaps"),
        ("ix_recovery_gaps_confidence", "recovery_gaps"),
        ("ix_recovery_gaps_recovery_id", "recovery_gaps"),
        ("ix_recovery_gaps_device_id", "recovery_gaps"),
        ("ix_recovery_gaps_dataflow_id", "recovery_gaps"),
        ("ix_recovery_gaps_session_id", "recovery_gaps"),
        ("ix_recovery_gaps_incident_id", "recovery_gaps"),
        ("ix_recovery_gaps_gap_id", "recovery_gaps"),
        ("ix_incidents_status", "incidents"),
        ("ix_incidents_recovery_id", "incidents"),
        ("ix_incidents_device_id", "incidents"),
        ("ix_incidents_dataflow_id", "incidents"),
        ("ix_incidents_session_id", "incidents"),
        ("ix_incidents_incident_id", "incidents"),
        ("ix_session_notes_session_created", "session_notes"),
        ("ix_session_sink_bindings_session_id", "session_sink_bindings"),
        (
            "ix_influx_profile_revisions_content_hash",
            "influx_sink_profile_revisions",
        ),
        ("ix_influx_profiles_archived_name", "influx_sink_profiles"),
        ("ix_influx_sink_profiles_name", "influx_sink_profiles"),
    ):
        op.drop_index(index, table_name=table)

    for table in (
        "session_sink_bindings",
        "influx_sink_profile_revisions",
        "influx_sink_profiles",
        "device_registrations",
        "session_template_dependencies",
        "session_templates",
        "device_seen",
        "device_configs",
        "backend_events",
        "runtime_ownerships",
        "operations",
        "output_files",
        "runtime_manifests",
        "recovery_gaps",
        "incidents",
        "session_activity_entries",
        "session_notes",
        "sessions",
    ):
        op.drop_table(table)

    op.drop_index("ix_experiments_name", table_name="experiments")
    op.drop_table("experiments")
