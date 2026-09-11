"""Request/response schemas (marshmallow).

Field NAMES here are part of the contract (acceptance criterion: "stable field
names"), so treat a rename like a breaking change. Enum fields pull their legal
values from app.domain.enums via ``fields.Enum(..., by_value=True)`` — so the wire
values are exactly the controlled vocabulary defined once in that module.

Layering reminder: schemas validate transport shape. Lifecycle and runtime
business rules live in the service layer.
"""

from datetime import UTC, datetime

from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from app.domain.enums import (
    DeviceClaimState,
    DeviceType,
    IncidentStatus,
    OperationScope,
    OperationState,
    PolicyMode,
    RuntimeOwnershipState,
    SessionStatus,
    SinkType,
    WatchdogProcessState,
)


class JSONDateTime(fields.DateTime):
    """DateTime request field that can also dump an ISO string stored in JSON."""

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, str):
            return value
        return super()._serialize(value, attr, obj, **kwargs)


class SinkSchema(Schema):
    """One output sink in a device flow's ordered ``sinks`` collection.

    Mirrors the canonical service contract (app.services.session_config): a
    stable ``sink_name`` (defaults to the sink type when omitted), a required
    ``sink_type`` from the controlled sink vocabulary, an optional
    ``sink_location`` permitted only for file-category sinks (csv/edf/pvfs),
    and a ``sink_parameters`` map that is always present ({} when the sink
    takes none). Secret *values* are rejected by the service layer; only
    non-secret values and secret *references* (e.g. Influx ``api_token_env``,
    an environment-variable NAME) are accepted.
    """

    sink_name = fields.String(load_default=None, validate=validate.Length(min=1))
    sink_type = fields.Enum(SinkType, by_value=True, required=True)
    sink_location = fields.String(load_default=None, validate=validate.Length(min=1))
    sink_parameters = fields.Dict(keys=fields.String(), values=fields.Raw(), load_default=dict)


class DeviceFlowSchema(Schema):
    """One device/source plus its non-empty ordered collection of sinks.

    Device identity is supplied either by ``device_config_id`` (an already
    configured device) or by ``device_template_path`` + ``hardware_id`` +
    ``port`` (instantiate a config from a device template). This is the
    documented wire contract; the create/template routes accept raw flow
    mappings (``fields.Raw``) and delegate canonicalization plus legacy
    flattened-sink normalization to the session-config service, so a single
    source of truth validates both request shapes.
    """

    nickname = fields.String(load_default=None)
    device_config_id = fields.Integer(load_default=None)
    device_template_path = fields.String(load_default=None)
    device_template_content_hash = fields.String(load_default=None)
    hardware_id = fields.String(load_default=None)
    port = fields.String(load_default=None)
    sinks = fields.List(
        fields.Nested(SinkSchema),
        required=True,
        validate=validate.Length(min=1, error="A device flow needs at least one sink."),
    )


class ScheduledDeviceRequirementSchema(Schema):
    flow_index = fields.Integer(dump_only=True)
    preferred_device_config_id = fields.Integer(dump_only=True)
    required_device_type = fields.String(dump_only=True)
    required_device_template_path = fields.String(dump_only=True)
    required_configuration_hash = fields.String(dump_only=True)
    required_parameters = fields.Dict(dump_only=True)
    preferred_hardware_id = fields.String(dump_only=True, allow_none=True)
    preferred_parameters = fields.Dict(dump_only=True)
    selected_device_config_id = fields.Integer(dump_only=True, allow_none=True)
    match = fields.String(dump_only=True, allow_none=True)


class ScheduleCancellationSchema(Schema):
    code = fields.String(dump_only=True)
    detail = fields.String(dump_only=True)
    cancelled_at = fields.DateTime(dump_only=True)
    unresolved_flows = fields.List(fields.Integer(), dump_only=True)


class ScheduleSchema(Schema):
    """Durable one-shot schedule returned with a session."""

    mode = fields.String(dump_only=True)
    start_at = JSONDateTime(dump_only=True)
    fallback_policy = fields.String(dump_only=True)
    requirements = fields.List(fields.Nested(ScheduledDeviceRequirementSchema), dump_only=True)
    cancellation = fields.Nested(ScheduleCancellationSchema, dump_only=True, allow_none=True)


class SinkLocationAssignmentSchema(Schema):
    """One sink index per one file path location
    """

    sink_index = fields.Integer(required=True, validate=validate.Range(min=0))
    sink_location = fields.String(required=True, validate=validate.Length(min=1))


class FlowAssignmentSchema(Schema):
    """The reviewed assignment for one template flow: one flow index with multiple sink indexes : location pairs."""

    flow_index = fields.Integer(required=True, validate=validate.Range(min=0))
    device_config_id = fields.Integer(required=True)
    sink_locations = fields.List(
        fields.Nested(SinkLocationAssignmentSchema),
        load_default=list,
    )


class SessionRunTemplateInputSchema(Schema):
    """Template revision, assignments, and metadata shared by both run modes."""

    source_template_id = fields.String(required=True, validate=validate.Length(min=1))
    expected_template_hash = fields.String(
        required=True,
        validate=validate.Regexp(r"^[0-9a-f]{64}$", error="must be a SHA-256 hex digest"),
    )
    assignments = fields.List(
        fields.Nested(FlowAssignmentSchema),
        required=True,
        validate=validate.Length(min=1),
    )
    # This is a run label, not the final stored name. The server composes
    # "<template>.<label> <template run number>"; blank uses the label "Run".
    name = fields.String(load_default=None, validate=validate.Length(min=1, max=120))
    experiment_id = fields.String(load_default=None)
    notes = fields.String(load_default=None, allow_none=True)


class SessionRunExecutionSchema(Schema):
    mode = fields.String(
        required=True,
        validate=validate.OneOf(["immediate", "scheduled"]),
    )
    start_at = fields.DateTime(load_default=None)
    @validates_schema
    def check_execution(self, data, **kwargs):
        mode = data.get("mode")
        start_at = data.get("start_at")
        if mode == "immediate":
            if start_at is not None:
                raise ValidationError(
                    "Must be omitted for an immediate run.", field_name="start_at"
                )
            return
        if start_at is None:
            raise ValidationError("Required for a scheduled run.", field_name="start_at")
        aware = start_at if start_at.tzinfo else start_at.replace(tzinfo=UTC)
        if aware <= datetime.now(UTC):
            raise ValidationError("Must be in the future.", field_name="start_at")


class CreateSessionRunSchema(SessionRunTemplateInputSchema):
    """Atomic immediate run or durable future schedule."""

    idempotency_key = fields.String(
        required=True,
        validate=validate.Length(min=8, max=128),
    )
    execution = fields.Nested(SessionRunExecutionSchema, required=True)
    force = fields.Boolean(load_default=False)

    @validates_schema
    def check_force(self, data, **kwargs):
        if data.get("force") and data.get("execution", {}).get("mode") != "immediate":
            raise ValidationError(
                "Force is available only when starting an immediate run.",
                field_name="force",
            )


class SessionNameSuggestionQuerySchema(Schema):
    source_template_id = fields.String(required=True, validate=validate.Length(min=1))


class SessionNameSuggestionSchema(Schema):
    """Template-scoped default name preview computed by the backend."""

    name = fields.String(dump_only=True)


class SessionSchema(Schema):
    """How a stored session is represented on the wire (response shape)."""

    id = fields.String(dump_only=True)
    name = fields.String()
    status = fields.Enum(SessionStatus, by_value=True)
    policy = fields.Enum(PolicyMode, by_value=True)
    experiment_id = fields.String(allow_none=True)
    notes = fields.String(allow_none=True)
    schedule = fields.Nested(ScheduleSchema, allow_none=True)
    device_flows = fields.List(fields.Raw())
    command_id = fields.String(allow_none=True)
    # Copied provenance, not a foreign key: a session stays readable — and its
    # run reconstructable — after the template is edited, archived, or the
    # registry is rebuilt from scratch.
    source_template_id = fields.String(dump_only=True, allow_none=True)
    source_template_name = fields.String(dump_only=True, allow_none=True)
    source_template_ref = fields.String(dump_only=True, allow_none=True)
    source_template_hash = fields.String(dump_only=True, allow_none=True)
    source_template_snapshot = fields.Raw(dump_only=True, allow_none=True)
    scheduled_for = fields.DateTime(dump_only=True, allow_none=True)
    cancellation_details = fields.Raw(dump_only=True, allow_none=True)
    cancelled_at = fields.DateTime(dump_only=True, allow_none=True)
    created_at = fields.DateTime(dump_only=True)


class SessionNoteSchema(Schema):
    id = fields.Integer(dump_only=True)
    session_id = fields.Integer(dump_only=True)
    body = fields.String(dump_only=True)
    show_timestamp = fields.Boolean(dump_only=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class SessionNoteListQuerySchema(Schema):
    limit = fields.Integer(load_default=100, validate=validate.Range(min=1, max=100))
    before_id = fields.Integer(load_default=None, allow_none=True, validate=validate.Range(min=1))


class SessionNotePageSchema(Schema):
    items = fields.List(fields.Nested(SessionNoteSchema), dump_only=True)
    has_more = fields.Boolean(dump_only=True)
    next_before_id = fields.Integer(dump_only=True, allow_none=True)


class CreateSessionNoteSchema(Schema):
    body = fields.String(required=True, validate=validate.Length(min=1, max=4000))
    show_timestamp = fields.Boolean(load_default=False)

    @validates_schema
    def validate_body(self, data, **kwargs):
        if not data["body"].strip():
            raise ValidationError("Must not be blank.", field_name="body")


class UpdateSessionNoteSchema(Schema):
    body = fields.String(validate=validate.Length(min=1, max=4000))
    show_timestamp = fields.Boolean()

    @validates_schema
    def validate_update(self, data, **kwargs):
        if not data:
            raise ValidationError("Provide body or show_timestamp.")
        if "body" in data and not data["body"].strip():
            raise ValidationError("Must not be blank.", field_name="body")


class FleetSessionRowSchema(Schema):
    """One fleet row, including context for an operator-actionable incident."""

    id = fields.Integer()
    name = fields.String()
    status = fields.Enum(SessionStatus, by_value=True)
    phase = fields.String(allow_none=True)
    health = fields.String(allow_none=True)
    attention_reason = fields.String(allow_none=True)
    attention_since = fields.DateTime(allow_none=True)


class FleetOverviewSchema(Schema):
    """Fleet-wide session overview (6f)."""

    running_count = fields.Integer()
    total_count = fields.Integer()
    sessions = fields.List(fields.Nested(FleetSessionRowSchema))


class LatestStreamEventDeviceSchema(Schema):
    device_id = fields.String(allow_none=True)
    stream_status = fields.String(allow_none=True)
    action = fields.String(allow_none=True)
    reason = fields.String(allow_none=True)
    recovery_stage = fields.String(allow_none=True)
    # The watchdog's ACTUAL auto-restart budget for this stream, spent and total
    # (``Watchdog._recovery_attempt``). Distinct from the staleness fields below:
    # this counts restarts tried, those measure how long the stream has been
    # down. Reporting a tick streak under ``recovery_attempt`` is what previously
    # made "attempt 2 of 3" unrenderable.
    recovery_attempt = fields.Integer(allow_none=True)
    recovery_attempt_max = fields.Integer(allow_none=True)
    # Physical-disconnect observation window reported by the watchdog. These
    # remain optional for non-physical failures where no window was measured.
    disconnect_elapsed_seconds = fields.Float(allow_none=True)
    disconnect_remaining_seconds = fields.Float(allow_none=True)
    disconnect_window_seconds = fields.Float(allow_none=True)
    # Consecutive non-healthy watchdog reports, and that streak converted to
    # seconds using the watchdog's own cadence — so the frontend never has to
    # know the report interval to say "down for 45s".
    nonhealthy_ticks = fields.Integer(allow_none=True)
    nonhealthy_seconds = fields.Float(allow_none=True)
    pending_recovery = fields.Boolean()
    recovery_policy = fields.String(allow_none=True)
    recovery_state = fields.String()
    requires_approval = fields.Boolean()
    hardware_present = fields.Boolean(allow_none=True)
    control_available = fields.Boolean()
    allowed_recovery_actions = fields.List(fields.String())


class LatestStreamEventSchema(Schema):
    """The newest persisted runtime report surfaced in a status snapshot (6g)."""

    event_id = fields.Integer()
    sequence = fields.Integer()
    phase = fields.String(allow_none=True)
    comms = fields.String(allow_none=True)
    recovery_id = fields.String(allow_none=True)
    received_at = fields.DateTime(allow_none=True)
    devices = fields.List(fields.Nested(LatestStreamEventDeviceSchema))
    diagnostics = fields.Raw(allow_none=True)


class OperationListQuerySchema(Schema):
    state = fields.Enum(OperationState, by_value=True, load_default=None)
    session_id = fields.Integer(load_default=None, data_key="session")
    dataflow_id = fields.String(load_default=None, data_key="dataflow")


class ResolveOperationSchema(Schema):
    outcome = fields.String(required=True, validate=validate.OneOf(["succeeded", "failed"]))
    resolved_by = fields.String(required=True, validate=validate.Length(min=1, max=255))
    resolution_note = fields.String(required=True, validate=validate.Length(min=1, max=1024))


class OperationSchema(Schema):
    id = fields.Integer(dump_only=True)
    operation_id = fields.String(dump_only=True)
    request_key = fields.String()
    session_id = fields.Integer()
    dataflow_id = fields.String()
    scope = fields.Enum(OperationScope, by_value=True)
    target_device_id = fields.String(allow_none=True)
    command = fields.String()
    request_id = fields.String(allow_none=True)
    command_id = fields.String()
    watchdog_id = fields.String(allow_none=True)
    recovery_id = fields.String(allow_none=True)
    manifest_hash = fields.String(allow_none=True)
    runtime_id = fields.String(allow_none=True)
    state = fields.Enum(OperationState, by_value=True)
    queued_at = fields.DateTime(allow_none=True)
    claimed_at = fields.DateTime(allow_none=True)
    dispatched_at = fields.DateTime(allow_none=True)
    running_at = fields.DateTime(allow_none=True)
    verifying_at = fields.DateTime(allow_none=True)
    finished_at = fields.DateTime(allow_none=True)
    error_code = fields.String(allow_none=True)
    error_message = fields.String(allow_none=True)
    details = fields.Raw(allow_none=True)
    resolved_by = fields.String(allow_none=True)
    resolved_at = fields.DateTime(allow_none=True)
    resolution_note = fields.String(allow_none=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class RuntimeOwnershipSchema(Schema):
    id = fields.Integer(dump_only=True)
    runtime_id = fields.String()
    session_id = fields.Integer()
    dataflow_id = fields.String()
    manifest_hash = fields.String()
    pid = fields.Integer(allow_none=True)
    port = fields.Integer(allow_none=True)
    state = fields.Enum(RuntimeOwnershipState, by_value=True)
    started_at = fields.DateTime(dump_only=True)
    last_seen_at = fields.DateTime(allow_none=True)
    adopted_at = fields.DateTime(allow_none=True)
    stopped_at = fields.DateTime(allow_none=True)
    details = fields.Raw(allow_none=True)
    watchdog_id = fields.String(allow_none=True)
    watchdog_state = fields.Enum(WatchdogProcessState, by_value=True, allow_none=True)
    watchdog_control_port = fields.Integer(allow_none=True)
    watchdog_last_seen_at = fields.DateTime(allow_none=True)


class RuntimeRecoveryStatusSchema(Schema):
    phase = fields.String()
    reason = fields.String()
    attempt = fields.Integer()
    next_retry_at = fields.String(allow_none=True)
    hardware_access = fields.String()
    evidence = fields.Raw()
    operator_message = fields.String()


class ReconciliationSummarySchema(Schema):
    succeeded_operations = fields.Integer()
    failed_operations = fields.Integer()
    uncertain_operations = fields.Integer()
    adopted_runtimes = fields.Integer()
    stopped_runtimes = fields.Integer()
    uncertain_runtimes = fields.Integer()
    deferred_runtimes = fields.Integer()
    released_orphan_sessions = fields.Integer()


class DeviceTemplateContentSchema(Schema):
    type = fields.String(required=True, validate=validate.Length(min=1))
    parameters = fields.Dict(keys=fields.String(), values=fields.Raw(), load_default=dict)


class CreateDeviceTemplateSchema(DeviceTemplateContentSchema):
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))


class DeviceTemplateSchema(Schema):
    name = fields.String()
    file_path = fields.String()
    type = fields.String()
    content = fields.Raw()
    content_hash = fields.String()
    created_at = fields.DateTime(dump_only=True)
    modified_at = fields.DateTime(dump_only=True, allow_none=True)
    status = fields.String(dump_only=True)
    validation_error = fields.String(dump_only=True, allow_none=True)


class DeviceTemplateTomlSchema(Schema):
    """An in-memory device-template TOML draft."""

    toml = fields.String(required=True, validate=validate.Length(min=1, max=1_000_000))


class DeviceTemplateSourceSchema(DeviceTemplateTomlSchema):
    reference = fields.String(dump_only=True)


class DeviceTemplateTomlValidationSchema(Schema):
    content = fields.Raw(dump_only=True)
    parameter_count = fields.Integer(dump_only=True)
    content_hash = fields.String(dump_only=True)
    matches = fields.List(fields.Nested("DeviceTemplateSchema"), dump_only=True)


class DeviceTemplateMatchSchema(Schema):
    content = fields.Raw(dump_only=True)
    content_hash = fields.String(dump_only=True)
    matches = fields.List(fields.Nested("DeviceTemplateSchema"), dump_only=True)


class DeviceTemplateTypeSchema(Schema):
    type = fields.String(dump_only=True)
    required_parameters = fields.List(fields.String(), dump_only=True)
    optional_parameters = fields.List(fields.String(), dump_only=True)


class RenameDeviceTemplateSchema(Schema):
    new_name = fields.String(required=True, validate=validate.Length(min=1, max=255))


class ReferencingSessionSchema(Schema):
    id = fields.String(dump_only=True)
    name = fields.String()
    status = fields.Enum(SessionStatus, by_value=True)
    policy = fields.Enum(PolicyMode, by_value=True)
    experiment_id = fields.String(allow_none=True)
    schedule = fields.Raw(allow_none=True)
    device_flows = fields.Raw()
    created_at = fields.DateTime(dump_only=True)


class ReferencingTemplateSchema(Schema):
    """A compact template reference returned by device-template safeguards."""

    id = fields.String(dump_only=True)
    name = fields.String(dump_only=True)
    reference = fields.String(dump_only=True)
    state = fields.String(dump_only=True)


class DeviceTemplateRenameResponseSchema(Schema):
    device_template = fields.Nested(DeviceTemplateSchema)
    referencing_sessions = fields.List(fields.Nested("ReferencingTemplateSchema"))
    warning = fields.String()


class DeviceTemplateDeleteResponseSchema(Schema):
    deleted_name = fields.String()
    referencing_sessions = fields.List(fields.Nested("ReferencingTemplateSchema"))
    warning = fields.String()


class CreateDeviceConfigSchema(Schema):
    device_type = fields.Enum(DeviceType, by_value=True, required=True, data_key="type")
    hardware_id = fields.String(required=True, validate=validate.Length(min=1, max=255))
    port = fields.String(required=True, validate=validate.Length(min=1, max=255))
    parameters = fields.Dict(keys=fields.String(), values=fields.Raw(), load_default=dict)
    nickname = fields.String(allow_none=True, load_default=None)


class CreateDeviceConfigFromTemplateSchema(Schema):
    template_name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    hardware_id = fields.String(required=True, validate=validate.Length(min=1, max=255))
    port = fields.String(required=True, validate=validate.Length(min=1, max=255))
    nickname = fields.String(allow_none=True, load_default=None)


class NameDeviceConfigSchema(Schema):
    device_type = fields.Enum(DeviceType, by_value=True, required=True, data_key="type")
    hardware_id = fields.String(required=True, validate=validate.Length(min=1, max=255))
    nickname = fields.String(required=True, validate=validate.Length(min=1, max=255))


class RegisterDeviceNameSchema(NameDeviceConfigSchema):
    """Input for naming a physical device before configuration."""


class DeviceRegistrationSchema(Schema):
    id = fields.Integer(dump_only=True)
    device_type = fields.Enum(DeviceType, by_value=True, data_key="type")
    hardware_id = fields.String()
    nickname = fields.String()
    device_config_id = fields.Integer(allow_none=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class EditDeviceConfigSchema(Schema):
    """Edit parameters; optionally push them to the linked template or relink to another (not both)."""

    parameters = fields.Dict(keys=fields.String(), values=fields.Raw(), required=True)
    update_source_template = fields.Boolean(load_default=False)
    source_template = fields.String(load_default=None, validate=validate.Length(min=1))

    @validates_schema
    def _reject_conflicting_provenance(self, data, **kwargs):
        if data.get("update_source_template") and data.get("source_template"):
            raise ValidationError(
                "Cannot both save edits back to the current template "
                "(update_source_template) and switch to a different one (source_template).",
                "source_template",
            )


class DeviceConfigSchema(Schema):
    id = fields.Integer(dump_only=True)
    device_type = fields.Enum(DeviceType, by_value=True, data_key="type")
    hardware_id = fields.String()
    port = fields.String()
    parameters = fields.Raw()
    color = fields.String(validate=validate.Regexp(r"^#[0-9a-fA-F]{6}$"))
    nickname = fields.String(allow_none=True)
    source_template = fields.String(allow_none=True)
    source_template_hash = fields.String(allow_none=True)
    source_template_history = fields.String(allow_none=True)
    claim_state = fields.Enum(DeviceClaimState, by_value=True)
    claimed_session_id = fields.Integer(allow_none=True)
    claim_expires_at = fields.DateTime(allow_none=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class DeviceConfigDeleteResponseSchema(Schema):
    deleted_id = fields.Integer()


class CreateSessionTemplateSchema(Schema):
    """What a user/client submit to create a template. device_flows already contain device template path and sink info"""
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    policy = fields.Enum(PolicyMode, by_value=True, load_default=PolicyMode.RECOMMEND)
    device_flows = fields.List(fields.Raw(), required=True)


class SessionTemplateTomlSchema(Schema):
    """An in-memory TOML draft submitted for validation."""

    toml = fields.String(required=True, validate=validate.Length(min=1, max=1_000_000))


class SessionTemplateSourceSchema(SessionTemplateTomlSchema):
    """Editable source for one flat-library session template."""

    reference = fields.String(dump_only=True)


class CreateSessionTemplateFromTomlSchema(SessionTemplateTomlSchema):
    """A named TOML draft submitted for persistence."""

    name = fields.String(required=True, validate=validate.Length(min=1, max=255))


class SessionTemplateTomlSummarySchema(Schema):
    device_flows = fields.Integer(dump_only=True)
    sinks = fields.Integer(dump_only=True)
    hardware_preferences = fields.Integer(dump_only=True)
    policy = fields.String(dump_only=True)


class SessionTemplateTomlValidationSchema(Schema):
    content = fields.Raw(dump_only=True)
    summary = fields.Nested(SessionTemplateTomlSummarySchema, dump_only=True)


class SessionTemplateRunSchema(Schema):
    """Enough of a session run to name it in a template listing."""

    id = fields.Integer(dump_only=True)
    name = fields.String(dump_only=True)
    status = fields.Enum(SessionStatus, by_value=True, dump_only=True)
    created_at = fields.DateTime(dump_only=True, allow_none=True)


class SessionTemplateRunBlockerSchema(Schema):
    """A server-owned reason a template cannot enter assignment or run creation."""

    code = fields.String(dump_only=True)
    message = fields.String(dump_only=True)
    flow_index = fields.Integer(dump_only=True, allow_none=True)
    device_template_path = fields.String(dump_only=True, allow_none=True)
    expected_hash = fields.String(dump_only=True, allow_none=True)
    actual_hash = fields.String(dump_only=True, allow_none=True)
    recovery_action = fields.String(dump_only=True)


class SessionTemplateSchema(Schema):
    """A flat-file template definition joined with its registry state."""

    template_id = fields.String(dump_only=True, allow_none=True)
    name = fields.String(dump_only=True)
    reference = fields.String(dump_only=True)
    registered_hash = fields.String(dump_only=True, allow_none=True)
    observed_hash = fields.String(dump_only=True, allow_none=True)
    state = fields.String(dump_only=True)
    lifecycle_state = fields.String(dump_only=True, allow_none=True)
    integrity_state = fields.String(dump_only=True, allow_none=True)
    lineage_parent_id = fields.String(dump_only=True, allow_none=True)
    duplicate_of_template_id = fields.String(dump_only=True, allow_none=True)
    content = fields.Raw(dump_only=True, allow_none=True)
    warnings = fields.List(fields.String(), dump_only=True, dump_default=list)
    runnable = fields.Boolean(dump_only=True)
    run_blockers = fields.List(
        fields.Nested(SessionTemplateRunBlockerSchema),
        dump_only=True,
        dump_default=list,
    )
    allowed_actions = fields.List(fields.String(), dump_only=True, dump_default=list)
    # Run history is joined by /catalog only. `null` therefore means "this route
    # did not count runs", which is not the same as zero — a registered template
    # nobody has started reports 0 with a null latest_session.
    run_count = fields.Integer(dump_only=True, allow_none=True)
    latest_session = fields.Nested(SessionTemplateRunSchema, dump_only=True, allow_none=True)
    created_at = fields.DateTime(dump_only=True, allow_none=True)
    updated_at = fields.DateTime(dump_only=True, allow_none=True)


class ResolveSessionTemplateRenameSchema(Schema):
    selected_relative_path = fields.String(
        required=True,
        validate=[
            validate.Length(min=1, max=1024),
            validate.Regexp(r"^(?!.*\.\.)[^/\\]+$"),
        ],
    )


class ExperimentSchema(Schema):
    id = fields.String(dump_only=True)
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    description = fields.String(
        allow_none=True,
        load_default=None,
        validate=validate.Length(max=4000),
    )
    archived_at = fields.DateTime(allow_none=True, dump_only=True)
    created_at = fields.DateTime(dump_only=True)
    updated_at = fields.DateTime(dump_only=True)


class ExperimentCreateSchema(Schema):
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    description = fields.String(
        allow_none=True,
        load_default=None,
        validate=validate.Length(max=4000),
    )


class ExperimentUpdateSchema(Schema):
    name = fields.String(validate=validate.Length(min=1, max=255))
    description = fields.String(allow_none=True, validate=validate.Length(max=4000))

    @validates_schema
    def validate_update(self, data, **kwargs):
        if not data:
            raise ValidationError("Provide name or description.")


class AssignmentCandidateSchema(Schema):
    device_config_id = fields.Integer()
    device_type = fields.String()
    hardware_id = fields.String(allow_none=True)
    port = fields.String()


class AssignmentSchema(AssignmentCandidateSchema):
    flow_index = fields.Integer()
    match = fields.String(validate=validate.OneOf(["exact", "generic"]))


class AssignmentWarningSchema(Schema):
    flow_index = fields.Integer()
    code = fields.String(validate=validate.OneOf(["identity_unavailable"]))
    message = fields.String()
    requested_hardware_id = fields.String(allow_none=True)
    alternatives = fields.List(fields.Nested(AssignmentCandidateSchema))


class UnresolvedAssignmentSchema(Schema):
    flow_index = fields.Integer()
    code = fields.String(validate=validate.OneOf(["no_compatible_device", "identity_unavailable"]))
    message = fields.String()
    device_type = fields.String(allow_none=True)
    requested_hardware_id = fields.String(allow_none=True)


class AssignmentPlanSchema(Schema):
    template_name = fields.String()
    scan_id = fields.String()
    scanned_at = fields.DateTime()
    assignments = fields.List(fields.Nested(AssignmentSchema))
    warnings = fields.List(fields.Nested(AssignmentWarningSchema))
    unresolved_requirements = fields.List(fields.Nested(UnresolvedAssignmentSchema))
    complete = fields.Boolean()


class ExportSessionTemplateSchema(Schema):
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    binding_mode = fields.String(
        load_default="generic",
        validate=validate.OneOf(["generic", "device-hardcoded"]),
    )


class RecoverSessionSchema(Schema):
    """Input for a targeted per-stream recovery command."""

    device_id = fields.String(required=True, validate=validate.Length(min=1))
    action = fields.String(
        required=True,
        validate=validate.OneOf(["restart"]),
    )


class StopSessionSchema(Schema):
    force = fields.Boolean(load_default=False)


class IncidentListQuerySchema(Schema):
    session = fields.Integer(allow_none=True, load_default=None)
    status = fields.Enum(IncidentStatus, by_value=True, load_default=None)
    page_size = fields.Integer(load_default=50, validate=validate.Range(min=1, max=200))
    cursor = fields.String(allow_none=True, load_default=None)


class AckIncidentSchema(Schema):
    acknowledged_by = fields.String(load_default=None, validate=validate.Length(min=1, max=255))
    note = fields.String(load_default=None, validate=validate.Length(min=1, max=1024))


class IncidentSchema(Schema):
    id = fields.Integer(dump_only=True)
    incident_id = fields.String()
    session_id = fields.Integer()
    dataflow_id = fields.String()
    device_id = fields.String(allow_none=True)
    sink_id = fields.String(allow_none=True)
    runtime_id = fields.String(allow_none=True)
    operation_id = fields.String(allow_none=True)
    recovery_id = fields.String(allow_none=True)
    status = fields.String()
    reason = fields.String()
    policy = fields.String(allow_none=True)
    details = fields.Raw(allow_none=True)
    opened_at = fields.DateTime(allow_none=True)
    acknowledged_at = fields.DateTime(allow_none=True)
    acknowledged_by = fields.String(allow_none=True)
    acknowledgement_note = fields.String(allow_none=True)
    resolved_at = fields.DateTime(allow_none=True)
    resolution = fields.String(allow_none=True)
    # Which operator surface this belongs to: ``data_path`` (a device or sink
    # stopped moving data — the same axis that produces recovery gaps) or
    # ``control_plane`` (processes, telemetry, commands). Derived from ``reason``
    # so the two surfaces cannot drift apart, and served rather than re-derived
    # in the client so the vocabulary lives in exactly one place.
    axis = fields.Method("_axis", dump_only=True)
    # Whether this is waiting on a PERSON, as opposed to on the system. A crashed
    # watchdog respawns itself and an unreachable host is reconciled, so those are
    # recorded but never counted against an operator; the reasons that represent
    # self-healing having FAILED (crash loop, outbox overflow) are.
    needs_action = fields.Method("_needs_action", dump_only=True)

    def _axis(self, incident) -> str:
        # Imported here rather than at module scope: app.services.incidents pulls
        # in repositories and models, and this module is imported by the API
        # blueprints those services are reached through.
        from app.services.incidents import axis_for_reason

        return axis_for_reason(getattr(incident, "reason", None))

    def _needs_action(self, incident) -> bool:
        from app.services.incidents import requires_action_for_reason

        return requires_action_for_reason(getattr(incident, "reason", None))


class IncidentPageSchema(Schema):
    items = fields.List(fields.Nested(IncidentSchema))
    next_cursor = fields.String(allow_none=True)
    has_more = fields.Boolean()


class DirectoryQuerySchema(Schema):
    """Absolute folder to list. Omitted/blank starts at the configured OUTPUT_DIR."""

    path = fields.String(load_default="")


class NewDirectorySchema(Schema):
    """One new folder under ``path``. ``name`` is a single segment, not a path."""

    path = fields.String(load_default="")
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))


class DirectoryEntrySchema(Schema):
    name = fields.String()
    path = fields.String()


class RootListingSchema(Schema):
    """Filesystem roots — drive letters on Windows, a single "/" on POSIX."""

    roots = fields.List(fields.Nested(DirectoryEntrySchema))


class DirectoryListingSchema(Schema):
    """Absolute host paths for one folder in the sink-location folder picker."""

    path = fields.String()
    parent = fields.String(allow_none=True)
    name = fields.String()
    separator = fields.String()
    exists = fields.Boolean()
    writable = fields.Boolean()
    directories = fields.List(fields.Nested(DirectoryEntrySchema))


class GapListQuerySchema(Schema):
    session = fields.Integer(allow_none=True, load_default=None)
    confidence = fields.String(allow_none=True, load_default=None, validate=validate.OneOf(["confirmed", "estimated", "uncertain"]))
    page_size = fields.Integer(load_default=50, validate=validate.Range(min=1, max=200))
    cursor = fields.String(allow_none=True, load_default=None)


class RecoveryGapSchema(Schema):
    id = fields.Integer(dump_only=True)
    gap_id = fields.String()
    incident_id = fields.String(allow_none=True)
    session_id = fields.Integer()
    dataflow_id = fields.String()
    device_id = fields.String(allow_none=True)
    sink_id = fields.String(allow_none=True)
    operation_id = fields.String(allow_none=True)
    recovery_id = fields.String(allow_none=True)
    previous_segment_id = fields.String(allow_none=True)
    next_segment_id = fields.String(allow_none=True)
    reason = fields.String()
    policy = fields.String(allow_none=True)
    confidence = fields.String()
    gap_start = fields.Raw(allow_none=True)
    gap_end = fields.Raw(allow_none=True)
    details = fields.Raw(allow_none=True)
    created_at = fields.DateTime(dump_only=True)


class GapPageSchema(Schema):
    items = fields.List(fields.Nested(RecoveryGapSchema))
    next_cursor = fields.String(allow_none=True)
    has_more = fields.Boolean()


class SessionActivityListQuerySchema(Schema):
    page_size = fields.Integer(
        load_default=100, validate=validate.Range(min=1, max=200)
    )
    cursor = fields.String(allow_none=True, load_default=None)


class SessionActivityEntrySchema(Schema):
    activity_id = fields.String()
    session_id = fields.Integer()
    dataflow_id = fields.String(allow_none=True)
    kind = fields.String()
    category = fields.String()
    severity = fields.String()
    title = fields.String()
    summary = fields.String()
    source_type = fields.String()
    source_id = fields.String()
    operation_id = fields.String(allow_none=True)
    incident_id = fields.String(allow_none=True)
    gap_id = fields.String(allow_none=True)
    command_id = fields.String(allow_none=True)
    recovery_id = fields.String(allow_none=True)
    details = fields.Raw(allow_none=True)
    occurred_at = fields.DateTime()
    created_at = fields.DateTime()


class SessionActivityPageSchema(Schema):
    items = fields.List(fields.Nested(SessionActivityEntrySchema))
    next_cursor = fields.String(allow_none=True)
    has_more = fields.Boolean()


class SessionDiagnosticListQuerySchema(Schema):
    page_size = fields.Integer(
        load_default=500, validate=validate.Range(min=1, max=2000)
    )
    cursor = fields.String(load_default=None, allow_none=True)


class SessionDiagnosticExportQuerySchema(Schema):
    view = fields.String(
        load_default="default", validate=validate.OneOf(["default", "verbose"])
    )


class SessionDiagnosticPageSchema(Schema):
    items = fields.List(
        fields.Dict(keys=fields.String(), values=fields.Raw()), required=True
    )
    has_more = fields.Boolean(required=True)
    next_cursor = fields.String(allow_none=True)


class SinkOutputComponentSchema(Schema):
    """One retained physical component in a recoverable file output."""

    output_id = fields.String()
    segment_index = fields.Integer()
    path = fields.String()
    acquisition_state = fields.String()
    termination_reason = fields.String(allow_none=True)
    captured_samples = fields.Integer()
    captured_bytes = fields.Integer()


class SinkOutputStateSchema(Schema):
    """Persisted merge/delivery facts for a sink's output (not the live status snapshot)."""

    logical_sink_id = fields.String(allow_none=True)
    sink_type = fields.String(allow_none=True)
    artifact_state = fields.String(allow_none=True)
    delivery_state = fields.String(allow_none=True)
    base_path = fields.String(allow_none=True)
    canonical_path = fields.String(allow_none=True)
    final_output_id = fields.String(allow_none=True)
    finalized_at = fields.DateTime(allow_none=True)
    finalization_attempts = fields.Integer()
    verified = fields.Boolean()
    component_count = fields.Integer()
    recovery_count = fields.Integer()
    captured_samples = fields.Integer()
    captured_bytes = fields.Integer()
    sample_loss = fields.Integer(allow_none=True)
    byte_loss = fields.Integer(allow_none=True)
    components = fields.List(fields.Nested(SinkOutputComponentSchema))


class SinkStatusSchema(Schema):
    """Live sink health and delivery state, tracked separately from its source."""

    source_id = fields.String(allow_none=True)
    sink_id = fields.String(allow_none=True)
    sink_class = fields.String(allow_none=True)
    status = fields.String()
    last_update = fields.DateTime(allow_none=True)
    health = fields.String(allow_none=True)
    delivery = fields.String(allow_none=True)
    finalization = fields.String(allow_none=True)
    component = fields.String(allow_none=True)
    buffered_samples = fields.Integer(allow_none=True)
    buffered_bytes = fields.Integer(allow_none=True)
    sample_loss = fields.Integer(allow_none=True)
    byte_loss = fields.Integer(allow_none=True)
    sink_sequence = fields.Integer(allow_none=True)
    diagnostics = fields.Raw(allow_none=True)
    open_incidents = fields.List(fields.Nested(IncidentSchema))
    output = fields.Nested(SinkOutputStateSchema, allow_none=True)


class SessionStatusSnapshotSchema(Schema):
    """Richer-than-``GET /sessions/<id>`` aggregate detail snapshot (6g)."""

    session = fields.Nested(SessionSchema)
    health = fields.String(allow_none=True)
    phase = fields.String(allow_none=True)
    latest_report = fields.Nested(LatestStreamEventSchema, allow_none=True)
    runtimes = fields.List(fields.Nested(RuntimeOwnershipSchema))
    operations = fields.List(fields.Nested(OperationSchema))
    incidents = fields.List(fields.Nested(IncidentSchema))
    gaps = fields.List(fields.Nested(RecoveryGapSchema))
    # Per-sink runtime status on a SEPARATE axis from source/stream health: a
    # failing or finalizing sink surfaces here and never as source failure or
    # recovery (gaps SINK-08/SINK-23). Keyed by ``(source_id, sink_id)``.
    sinks = fields.List(fields.Nested(SinkStatusSchema))
    # Newest runtime-ownership row for this session, in ANY state — not just
    # the "active" ones — so a stop-proof-missing limbo (UNCERTAIN) still
    # surfaces its last-known identity. See app.services.session_status
    # ._active_runtime_view.
    runtime_id = fields.String(allow_none=True)
    watchdog_id = fields.String(allow_none=True)
    watchdog_state = fields.String(allow_none=True)
    recovery = fields.Nested(RuntimeRecoveryStatusSchema, allow_none=True)
    # Freshness of the latest DIRECT watchdog-process telemetry — same
    # classification app.control.event_poller.telemetry_freshness uses to
    # trigger stale-telemetry/outbox-overflow incidents (packet contract:
    # monitoring truth is latest direct watchdog-process telemetry plus
    # freshness windows).
    last_report_at = fields.DateTime(allow_none=True)
    outbox_health = fields.String()
    telemetry_diagnostics = fields.Raw(allow_none=True)


class GrafanaViewQuerySchema(Schema):
    target_id = fields.String(
        load_default=None,
        allow_none=True,
        validate=validate.Length(min=1, max=32),
    )
    data_detail = fields.String(
        load_default=None,
        allow_none=True,
        validate=validate.OneOf(
            ["raw", "auto", "10ms", "25ms", "50ms", "100ms", "250ms", "500ms", "1s"]
        ),
    )
    show_ttl = fields.List(
        fields.String(validate=validate.OneOf(["__none__", "TTL1", "TTL2", "TTL3", "TTL4"])),
        load_default=None,
        allow_none=True,
    )
    show_ch = fields.List(
        fields.String(validate=validate.OneOf(["__none__", "CH0", "CH1", "CH2"])),
        load_default=None,
        allow_none=True,
    )


class GrafanaViewTargetSchema(Schema):
    id = fields.String(required=True)
    label = fields.String(required=True)
    device = fields.String(required=True)
    channels = fields.List(fields.String(), required=True)


class GrafanaViewSchema(Schema):
    state = fields.String(
        required=True,
        validate=validate.OneOf(
            ["ready", "unavailable", "not_configured", "not_applicable"]
        ),
    )
    scope = fields.String(required=True)
    notice = fields.String(required=True)
    targets = fields.List(fields.Nested(GrafanaViewTargetSchema), required=True)
    selected_target_id = fields.String(allow_none=True)
    data_detail_options = fields.List(fields.String(), required=True)
    selected_data_detail = fields.String(required=True)
    ttl_options = fields.List(fields.String(), required=True)
    selected_ttl = fields.List(fields.String(), required=True)
    ch_options = fields.List(fields.String(), required=True)
    selected_ch = fields.List(fields.String(), required=True)
    embed_url = fields.Url(allow_none=True)
    open_url = fields.Url(allow_none=True)
    retry_after_seconds = fields.Integer(required=True)
    message = fields.String(allow_none=True)


class DiscoveredDeviceSchema(Schema):
    type = fields.Enum(DeviceType, by_value=True)
    port = fields.String()
    hardware_id = fields.String(allow_none=True)
    label = fields.String()
    availability = fields.String(
        validate=validate.OneOf(
            ["available", "unopenable", "not_found"]
        )
    )


class ScanResultSchema(Schema):
    scan_id = fields.String()
    scanned_at = fields.DateTime()
    devices = fields.List(fields.Nested(DiscoveredDeviceSchema))


class DevicePoolRowSchema(Schema):
    id = fields.Integer(allow_none=True)
    type = fields.String()
    port = fields.String()
    hardware_id = fields.String(allow_none=True)
    color = fields.String(allow_none=True, validate=validate.Regexp(r"^#[0-9a-fA-F]{6}$"))
    availability = fields.String(
        validate=validate.OneOf(
            ["available", "unopenable", "not_found"]
        )
    )
    status = fields.String(validate=validate.OneOf(["free", "claimed", "unconfigured"]))
    owner = fields.Integer(allow_none=True)
    nickname = fields.String(allow_none=True)
    label = fields.String(allow_none=True)
    source_template = fields.String(allow_none=True)
    source_template_hash = fields.String(allow_none=True)
    configuration_hash = fields.String(allow_none=True)


class DevicePoolSchema(Schema):
    scan_id = fields.String()
    scanned_at = fields.DateTime()
    devices = fields.List(fields.Nested(DevicePoolRowSchema))
