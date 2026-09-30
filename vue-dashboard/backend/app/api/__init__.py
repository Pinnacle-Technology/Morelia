"""Versioned business API (/api/v1)."""

from app.api.device_configs import blp as device_configs_blp
from app.api.device_registrations import blp as device_registrations_blp
from app.api.device_templates import blp as device_templates_blp
from app.api.device_templates import source_blp as device_template_sources_blp
from app.api.devices import blp as devices_blp
from app.api.events_ingest import blp as events_ingest_blp
from app.api.events_stream import blp as events_stream_blp
from app.api.experiments import blp as experiments_blp
from app.api.filesystem import blp as filesystem_blp
from app.api.gaps import blp as gaps_blp
from app.api.grafana_views import blp as grafana_views_blp
from app.api.incidents import blp as incidents_blp
from app.api.operations import blp as operations_blp
from app.api.runtimes import blp as runtimes_blp
from app.api.session_templates import blp as session_templates_blp
from app.api.session_templates import source_blp as session_template_sources_blp
from app.api.sessions import blp as sessions_blp
from app.api.session_runs import blp as session_runs_blp
from app.control.supervisor import HostSupervisor
from app.services.discovery import (
    DeviceDiscoveryService,
    FakeDiscoveryProvider,
    SerialPodProvider,
    configured_device_types_from_db,
)


def register_routes(api, app):
    """Attach the placeholder store and register all /api/v1 blueprints on `api`.

    Blueprints register on `api` (not `app`) so they appear in the OpenAPI spec.
    The internal ingest blueprint registers on `app` directly to stay off-spec.
    """
    host_supervisor = app.config.get("HOST_SUPERVISOR")
    if host_supervisor is None:
        host_supervisor = HostSupervisor()
    app.extensions["host_supervisor"] = host_supervisor
    discovery_provider = FakeDiscoveryProvider() if app.testing else SerialPodProvider()
    configured_device_types = None if app.testing else configured_device_types_from_db
    app.extensions["device_discovery_provider"] = discovery_provider
    app.extensions["device_discovery_service"] = DeviceDiscoveryService(
        discovery_provider,
        configured_device_types,
    )
    api.register_blueprint(sessions_blp)
    api.register_blueprint(session_runs_blp)
    api.register_blueprint(operations_blp)
    api.register_blueprint(incidents_blp)
    api.register_blueprint(gaps_blp)
    api.register_blueprint(grafana_views_blp)
    api.register_blueprint(experiments_blp)
    api.register_blueprint(runtimes_blp)
    api.register_blueprint(devices_blp)
    api.register_blueprint(device_configs_blp)
    api.register_blueprint(device_registrations_blp)
    api.register_blueprint(device_templates_blp)
    api.register_blueprint(device_template_sources_blp)
    api.register_blueprint(session_templates_blp)
    api.register_blueprint(session_template_sources_blp)
    api.register_blueprint(filesystem_blp)
    # Internal — loopback-only, excluded from the OpenAPI spec.
    app.register_blueprint(events_ingest_blp)
    # SSE stream — text/event-stream, excluded from JSON-centric OpenAPI spec.
    app.register_blueprint(events_stream_blp)
