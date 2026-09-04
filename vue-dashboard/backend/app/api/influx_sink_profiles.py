"""Management API for immutable Influx sink-profile revisions."""

from flask import current_app, request
from flask_smorest import Blueprint, abort

import app.services.influx_sink_profiles as service
from app.api.schemas import (
    ArchiveInfluxSinkProfileSchema,
    CreateInfluxSinkProfileRevisionSchema,
    CreateInfluxSinkProfileSchema,
    InfluxSinkProfileListQuerySchema,
    InfluxSinkProfilePageSchema,
    InfluxSinkProfileProbeSchema,
    InfluxSinkProfileSchema,
)

blp = Blueprint(
    "influx-sink-profiles",
    __name__,
    url_prefix="/api/v1/influx-sink-profiles",
    description="Manage deployment-local, immutable Influx sink profiles.",
)


def _require_operator_policy() -> None:
    """Delegate authorization to the deployment; absence always denies."""

    authorizer = current_app.config.get("INFLUX_SINK_PROFILE_AUTHORIZER")
    try:
        authorized = authorizer is not None and bool(authorizer(request))
    except Exception:
        authorized = False
    if not authorized:
        abort(
            403,
            message="Influx sink profile management is not authorized.",
            code="influx_profile_management_forbidden",
        )


def _call(action, *args, **kwargs):
    try:
        return action(*args, **kwargs)
    except service.InfluxSinkProfileError as exc:
        abort(exc.status, message=str(exc), code=exc.code)


@blp.before_request
def authorize_profile_mutations():
    # Authorization runs before request-body deserialization, so an anonymous
    # caller cannot use validation behavior as a profile-management oracle.
    if request.method != "GET":
        _require_operator_policy()


@blp.route("", methods=["GET"])
@blp.arguments(InfluxSinkProfileListQuerySchema, location="query")
@blp.response(200, InfluxSinkProfilePageSchema)
def list_profiles(query):
    return _call(
        service.list_page,
        include_archived=query["include_archived"],
        page=query["page"],
        page_size=query["page_size"],
    )


@blp.route("", methods=["POST"])
@blp.arguments(CreateInfluxSinkProfileSchema)
@blp.response(201, InfluxSinkProfileSchema)
def create_profile(payload):
    return _call(service.create, **payload)


@blp.route("/<string:profile_id>", methods=["GET"])
@blp.response(200, InfluxSinkProfileSchema)
def get_profile(profile_id):
    return _call(service.get, profile_id)


@blp.route("/<string:profile_id>/revisions", methods=["POST"])
@blp.arguments(CreateInfluxSinkProfileRevisionSchema)
@blp.response(201, InfluxSinkProfileSchema)
def create_revision(payload, profile_id):
    expected_revision = payload.pop("expected_profile_revision")
    return _call(
        service.revise,
        profile_id,
        expected_profile_revision=expected_revision,
        **payload,
    )


@blp.route(
    "/<string:profile_id>/revisions/<int:revision>/actions/test",
    methods=["POST"],
)
@blp.response(200, InfluxSinkProfileProbeSchema)
def test_revision(profile_id, revision):
    return _call(service.test_revision, profile_id, revision)


@blp.route("/<string:profile_id>/actions/archive", methods=["POST"])
@blp.arguments(ArchiveInfluxSinkProfileSchema)
@blp.response(200, InfluxSinkProfileSchema)
def archive_profile(payload, profile_id):
    return _call(service.archive, profile_id, **payload)
