"""HTTP adapter for Grafana view."""

from flask import current_app
from flask_smorest import Blueprint, abort

import app.services.grafana_views as service
from app.api.schemas import GrafanaViewQuerySchema, GrafanaViewSchema

blp = Blueprint(
    "grafana-views",
    __name__,
    url_prefix="/api/v1/sessions",
    description="Expose secret-free M4 comparison Grafana embed descriptors.",
)


@blp.route("/<int:session_id>/grafana-view", methods=["GET"])
@blp.arguments(GrafanaViewQuerySchema, location="query")
@blp.response(200, GrafanaViewSchema)
def grafana_view(query, session_id):
    try:
        return service.describe(
            session_id,
            config=current_app.config,
            target_id=query.get("target_id"),
            data_detail=query.get("data_detail"),
            show_ttl=query.get("show_ttl"),
            show_ch=query.get("show_ch"),
        )
    except service.InvalidGrafanaSelection as exc:
        abort(422, message=str(exc), code="invalid_grafana_selection")
