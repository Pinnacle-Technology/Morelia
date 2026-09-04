"""Phase 1 contract tests for immutable, operator-managed Influx profiles."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import URL, create_engine, inspect
from sqlalchemy.exc import IntegrityError

from app import create_app
from app.config import DevelopmentConfig
from app.database import db

PROFILE_URL = "https://influx.example.test:8086"
OPERATOR_HEADERS = {"X-Test-Operator": "allowed"}


def _operator_authorizer(request) -> bool:
    return request.headers.get("X-Test-Operator") == "allowed"


@pytest.fixture
def probe_calls() -> list[tuple[str, float]]:
    return []


@pytest.fixture
def profile_app(tmp_path, probe_calls):
    def resolver(host: str, port: int) -> tuple[str, ...]:
        assert (host, port) == ("influx.example.test", 8086)
        return ("203.0.113.10",)

    def probe(url: str, timeout_seconds: float) -> bool:
        probe_calls.append((url, timeout_seconds))
        return True

    application = create_app(
        "testing",
        config_overrides={
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{tmp_path / 'profiles.sqlite3'}",
            "INFLUX_SINK_PROFILE_AUTHORIZER": _operator_authorizer,
            "INFLUX_SINK_PROFILE_ALLOWED_HOSTS": ("influx.example.test",),
            "INFLUX_SINK_PROFILE_ALLOWED_NETWORKS": ("203.0.113.0/24",),
            "INFLUX_SINK_PROFILE_RESOLVER": resolver,
            "INFLUX_SINK_PROFILE_PROBE": probe,
            "INFLUX_SINK_PROFILE_PROBE_TIMEOUT_SECONDS": 1.25,
        },
    )
    with application.app_context():
        db.create_all()
    return application


@pytest.fixture
def profile_client(profile_app):
    return profile_app.test_client()


def _profile_payload(**overrides) -> dict:
    payload = {
        "name": "Lab Influx",
        "url": PROFILE_URL,
        "org": "morelia",
        "bucket": "sessions",
        "write_token_env": "MORELIA_INFLUX_WRITE_TOKEN",
        "read_token_env": "MORELIA_INFLUX_READ_TOKEN",
        "buffer_max_age_seconds": 300.0,
        "buffer_max_bytes": 1024 * 1024,
    }
    payload.update(overrides)
    return payload


def _create_profile(client, **overrides) -> dict:
    response = client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(**overrides),
        headers=OPERATOR_HEADERS,
    )
    assert response.status_code == 201, response.get_json()
    return response.get_json()


# -----------------------------------------------------------------------------
# Security and authorization
# -----------------------------------------------------------------------------


def test_management_defaults_to_deny_without_an_authorizer(tmp_path):
    """Verify profile management is denied when no authorizer is configured."""
    application = create_app(
        "testing",
        config_overrides={
            "SQLALCHEMY_DATABASE_URI": f"sqlite:///{tmp_path / 'deny.sqlite3'}",
            "INFLUX_SINK_PROFILE_ALLOWED_HOSTS": ("influx.example.test",),
            "INFLUX_SINK_PROFILE_ALLOWED_NETWORKS": ("203.0.113.0/24",),
        },
    )
    with application.app_context():
        db.create_all()

    response = application.test_client().post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(),
    )

    assert response.status_code == 403
    assert response.content_type == "application/problem+json"
    assert response.get_json()["code"] == "influx_profile_management_forbidden"

    invalid_response = application.test_client().post(
        "/api/v1/influx-sink-profiles",
        json={"api_token": "must-not-reach-validation"},
    )
    assert invalid_response.status_code == 403


def test_development_policy_uses_remote_addr_and_ignores_forwarding_headers():
    """Verify development access trusts direct loopback addresses only."""
    authorizer = DevelopmentConfig.INFLUX_SINK_PROFILE_AUTHORIZER

    assert authorizer(SimpleNamespace(remote_addr="127.0.0.1", headers={})) is True
    assert authorizer(SimpleNamespace(remote_addr="::1", headers={})) is True
    assert (
        authorizer(
            SimpleNamespace(
                remote_addr="198.51.100.20",
                headers={"X-Forwarded-For": "127.0.0.1"},
            )
        )
        is False
    )


# -----------------------------------------------------------------------------
# Profile management features
# -----------------------------------------------------------------------------


def test_authorized_create_and_list_return_only_safe_profile_metadata(profile_client):
    """Verify authorized create and list responses never expose secret values."""
    created = _create_profile(profile_client)

    assert created["name"] == "Lab Influx"
    assert created["revision"] == 1
    assert created["state"] == "active"
    assert len(created["content_hash"]) == 64
    assert created["destination"] == {
        "url": PROFILE_URL,
        "org": "morelia",
        "bucket": "sessions",
    }
    assert created["delivery"] == {
        "observe_on_scheduler": None,
        "buffer_max_age_seconds": 300.0,
        "buffer_max_bytes": 1024 * 1024,
    }
    assert created["credentials"] == {
        "write_configured": True,
        "read_configured": True,
    }
    serialized = str(created)
    assert "MORELIA_INFLUX_WRITE_TOKEN" not in serialized
    assert "MORELIA_INFLUX_READ_TOKEN" not in serialized

    response = profile_client.get("/api/v1/influx-sink-profiles")
    assert response.status_code == 200
    page = response.get_json()
    assert [item["id"] for item in page["items"]] == [created["id"]]
    assert page["pagination"] == {
        "page": 1,
        "page_size": 50,
        "total_items": 1,
        "total_pages": 1,
    }


def test_revision_append_is_immutable_and_stale_write_has_no_side_effect(
    profile_app,
    profile_client,
):
    """Verify revisions are immutable and stale updates leave state unchanged."""
    created = _create_profile(profile_client)
    profile_id = created["id"]

    revised_response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{profile_id}/revisions",
        json={
            "expected_profile_revision": 1,
            "buffer_max_bytes": 2 * 1024 * 1024,
            "observe_on_scheduler": "thread_pool",
        },
        headers=OPERATOR_HEADERS,
    )
    assert revised_response.status_code == 201
    revised = revised_response.get_json()
    assert revised["revision"] == 2
    assert revised["delivery"]["buffer_max_bytes"] == 2 * 1024 * 1024
    assert revised["delivery"]["observe_on_scheduler"] == "thread_pool"
    assert revised["content_hash"] != created["content_hash"]

    stale_response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{profile_id}/revisions",
        json={"expected_profile_revision": 1, "bucket": "redirected"},
        headers=OPERATOR_HEADERS,
    )
    assert stale_response.status_code == 409
    assert stale_response.get_json()["code"] == "stale_influx_profile_revision"

    from app.models.influx_sink_profile import InfluxSinkProfileRevision

    with profile_app.app_context():
        rows = db.session.scalars(
            db.select(InfluxSinkProfileRevision)
            .where(InfluxSinkProfileRevision.profile_id == profile_id)
            .order_by(InfluxSinkProfileRevision.revision)
        ).all()
        assert [
            (
                row.revision,
                row.bucket,
                row.buffer_max_bytes,
                row.observe_on_scheduler,
            )
            for row in rows
        ] == [
            (1, "sessions", 1024 * 1024, None),
            (2, "sessions", 2 * 1024 * 1024, "thread_pool"),
        ]


# -----------------------------------------------------------------------------
# Input and destination security
# -----------------------------------------------------------------------------


def test_raw_secret_and_disallowed_destination_fail_before_persistence_or_resolution(
    profile_app,
    profile_client,
):
    """Verify unsafe input is rejected before DNS lookup or persistence."""
    resolver_called = False

    def should_not_resolve(host: str, port: int) -> tuple[str, ...]:
        nonlocal resolver_called
        resolver_called = True
        return ("169.254.169.254",)

    profile_app.config["INFLUX_SINK_PROFILE_RESOLVER"] = should_not_resolve

    raw_secret = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(api_token="actual-secret-value"),
        headers=OPERATOR_HEADERS,
    )
    assert raw_secret.status_code == 422
    assert "actual-secret-value" not in raw_secret.get_data(as_text=True)

    disallowed = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(url="http://169.254.169.254/latest/meta-data"),
        headers=OPERATOR_HEADERS,
    )
    assert disallowed.status_code == 422
    assert disallowed.get_json()["code"] == "influx_destination_not_allowed"
    assert resolver_called is False

    from app.models.influx_sink_profile import InfluxSinkProfile

    with profile_app.app_context():
        assert db.session.scalar(db.select(db.func.count()).select_from(InfluxSinkProfile)) == 0


def test_duplicate_names_are_case_insensitive_and_delete_is_not_exposed(profile_client):
    """Verify names are unique case-insensitively and profiles cannot be deleted."""
    created = _create_profile(profile_client)

    duplicate = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(name="lab influx"),
        headers=OPERATOR_HEADERS,
    )
    assert duplicate.status_code == 409
    assert duplicate.get_json()["code"] == "influx_sink_profile_name_exists"
    assert (
        profile_client.delete(
            f"/api/v1/influx-sink-profiles/{created['id']}",
            headers=OPERATOR_HEADERS,
        ).status_code
        == 405
    )


# -----------------------------------------------------------------------------
# API contract features
# -----------------------------------------------------------------------------


def test_openapi_documents_profile_revision_and_action_routes(profile_client):
    """Verify OpenAPI includes profile, revision, archive, and test endpoints."""
    response = profile_client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.get_json()["paths"]
    assert "/api/v1/influx-sink-profiles" in paths
    assert "/api/v1/influx-sink-profiles/{profile_id}" in paths
    assert "/api/v1/influx-sink-profiles/{profile_id}/revisions" in paths
    assert ("/api/v1/influx-sink-profiles/{profile_id}/revisions/{revision}/actions/test") in paths
    assert "/api/v1/influx-sink-profiles/{profile_id}/actions/archive" in paths


# -----------------------------------------------------------------------------
# DNS and transport security
# -----------------------------------------------------------------------------


def test_mixed_safe_and_forbidden_dns_answers_fail_closed(profile_app, profile_client):
    """Verify any forbidden DNS answer causes destination validation to fail."""
    profile_app.config["INFLUX_SINK_PROFILE_RESOLVER"] = lambda host, port: (
        "203.0.113.10",
        "169.254.169.254",
    )

    response = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(),
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 422
    assert response.get_json()["code"] == "influx_destination_not_allowed"


@pytest.mark.parametrize(
    "url",
    [
        "http://user:password@influx.example.test:8086",
        "https://influx.example.test:8086?bucket=other",
        "https://influx.example.test:8086#fragment",
        "https://not-allowlisted.example.test:8086",
        "http://[fd00:ec2::254]:8086",
    ],
)
def test_destination_policy_rejects_unsafe_url_forms(profile_client, url):
    """Verify destination policy rejects unsafe or ambiguous URL forms."""
    response = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(url=url),
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 422
    assert "password" not in response.get_data(as_text=True)


def test_ip_literal_requires_exact_host_and_network_allowlisting(
    profile_app,
    profile_client,
):
    """Verify IP literals require explicit host and network allowlisting."""
    profile_app.config["INFLUX_SINK_PROFILE_ALLOWED_NETWORKS"] = ("10.0.0.0/8",)

    denied = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(url="http://10.0.0.5:8086"),
        headers=OPERATOR_HEADERS,
    )
    assert denied.status_code == 422
    assert denied.get_json()["code"] == "influx_destination_not_allowed"

    profile_app.config["INFLUX_SINK_PROFILE_ALLOWED_HOSTS"] = ("10.0.0.5",)
    allowed = profile_client.post(
        "/api/v1/influx-sink-profiles",
        json=_profile_payload(url="http://10.0.0.5:8086"),
        headers=OPERATOR_HEADERS,
    )
    assert allowed.status_code == 201


# -----------------------------------------------------------------------------
# Probe features and safety
# -----------------------------------------------------------------------------


def test_advisory_probe_is_revision_specific_bounded_and_does_not_check_credentials(
    profile_client,
    probe_calls,
):
    """Verify probes target one revision, use a timeout, and skip credentials."""
    created = _create_profile(profile_client)

    response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{created['id']}/revisions/1/actions/test",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "profile_id": created["id"],
        "tested_revision": 1,
        "status": "reachable",
        "credentials_checked": False,
    }
    assert probe_calls == [(f"{PROFILE_URL}/health", 1.25)]


def test_probe_revalidates_dns_policy_before_transport(
    profile_app,
    profile_client,
    probe_calls,
):
    """Verify a probe revalidates DNS policy immediately before transport."""
    created = _create_profile(profile_client)
    profile_app.config["INFLUX_SINK_PROFILE_RESOLVER"] = lambda host, port: ("169.254.169.254",)

    response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{created['id']}/revisions/1/actions/test",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 422
    assert response.get_json()["code"] == "influx_destination_not_allowed"
    assert probe_calls == []


def test_default_probe_connects_to_validated_address_not_hostname(
    profile_app,
    profile_client,
    monkeypatch,
):
    """Verify the default probe connects to the validated IP, not the hostname."""
    import app.services.influx_sink_profiles as profile_service

    connections = []

    class FakeResponse:
        status = 200

        def read(self, limit):
            return b"{}"

    class FakeConnection:
        def __init__(self, host, port, timeout):
            connections.append((host, port, timeout))

        def request(self, method, path, headers):
            assert (method, path) == ("GET", "/health")
            assert headers["Host"] == "influx.example.test:8086"

        def getresponse(self):
            return FakeResponse()

        def close(self):
            return None

    monkeypatch.setattr(profile_service.http.client, "HTTPConnection", FakeConnection)
    profile_app.config["INFLUX_SINK_PROFILE_PROBE"] = None
    created = _create_profile(profile_client, url="http://influx.example.test:8086")

    response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{created['id']}/revisions/1/actions/test",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 200
    assert response.get_json()["status"] == "reachable"
    assert connections == [("203.0.113.10", 8086, 1.25)]


# -----------------------------------------------------------------------------
# Profile lifecycle features
# -----------------------------------------------------------------------------


def test_archive_hides_profile_and_blocks_revisions_and_tests(profile_client):
    """Verify archiving hides a profile and blocks later mutations and probes."""
    created = _create_profile(profile_client)
    profile_id = created["id"]

    response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{profile_id}/actions/archive",
        json={"expected_profile_revision": 1},
        headers=OPERATOR_HEADERS,
    )
    assert response.status_code == 200
    assert response.get_json()["state"] == "archived"

    assert profile_client.get("/api/v1/influx-sink-profiles").get_json()["items"] == []
    archived_items = profile_client.get(
        "/api/v1/influx-sink-profiles?include_archived=true"
    ).get_json()["items"]
    assert [item["id"] for item in archived_items] == [profile_id]

    revise = profile_client.post(
        f"/api/v1/influx-sink-profiles/{profile_id}/revisions",
        json={"expected_profile_revision": 1, "bucket": "new"},
        headers=OPERATOR_HEADERS,
    )
    assert revise.status_code == 409
    assert revise.get_json()["code"] == "influx_profile_archived"

    test = profile_client.post(
        f"/api/v1/influx-sink-profiles/{profile_id}/revisions/1/actions/test",
        headers=OPERATOR_HEADERS,
    )
    assert test.status_code == 409
    assert test.get_json()["code"] == "influx_profile_archived"


# -----------------------------------------------------------------------------
# Persistence and migration
# -----------------------------------------------------------------------------


def test_baseline_migration_creates_profile_tables_and_downgrades_to_empty(tmp_path):
    """Verify the disposable baseline creates and fully removes profile tables."""
    backend_root = Path(__file__).resolve().parents[3] / "vue-dashboard" / "backend"
    database_path = tmp_path / "migration.sqlite3"
    config = Config()
    config.set_main_option("script_location", str(backend_root / "migrations"))
    config.set_main_option(
        "sqlalchemy.url",
        URL.create("sqlite", database=str(database_path)).render_as_string(hide_password=False),
    )

    command.upgrade(config, "head")

    engine = create_engine(URL.create("sqlite", database=str(database_path)))
    try:
        inspector = inspect(engine)
        assert {"influx_sink_profiles", "influx_sink_profile_revisions"} <= set(
            inspector.get_table_names()
        )
        foreign_keys = inspector.get_foreign_keys("influx_sink_profile_revisions")
        assert any(
            fk["constrained_columns"] == ["profile_id"]
            and fk["referred_table"] == "influx_sink_profiles"
            for fk in foreign_keys
        )
    finally:
        engine.dispose()

    command.downgrade(config, "base")
    engine = create_engine(URL.create("sqlite", database=str(database_path)))
    try:
        table_names = set(inspect(engine).get_table_names())
        assert "influx_sink_profiles" not in table_names
        assert "influx_sink_profile_revisions" not in table_names
        assert "experiments" not in table_names
    finally:
        engine.dispose()


def test_failed_revision_insert_rolls_back_current_pointer(profile_app, profile_client):
    """Verify a failed revision insert also rolls back the current pointer."""
    created = _create_profile(profile_client)

    from app.models.influx_sink_profile import InfluxSinkProfile, InfluxSinkProfileRevision
    from app.repositories.influx_sink_profiles import InfluxSinkProfileRepository

    with profile_app.app_context():
        current = db.session.get(InfluxSinkProfileRevision, (created["id"], 1))
        invalid_values = {
            "config_schema_version": current.config_schema_version,
            "content_hash": "too-short",
            "url": current.url,
            "org": current.org,
            "bucket": current.bucket,
            "write_token_env": current.write_token_env,
            "read_token_env": current.read_token_env,
            "observe_on_scheduler": current.observe_on_scheduler,
            "buffer_max_age_seconds": current.buffer_max_age_seconds,
            "buffer_max_bytes": current.buffer_max_bytes,
        }
        with pytest.raises(IntegrityError):
            InfluxSinkProfileRepository().append_revision(
                profile_id=created["id"],
                expected_revision=1,
                revision_values=invalid_values,
            )

        profile = db.session.get(InfluxSinkProfile, created["id"])
        revision_count = db.session.scalar(
            db.select(db.func.count())
            .select_from(InfluxSinkProfileRevision)
            .where(InfluxSinkProfileRevision.profile_id == created["id"])
        )
        assert profile.current_revision == 1
        assert revision_count == 1


# -----------------------------------------------------------------------------
# Advisory failure behavior
# -----------------------------------------------------------------------------


def test_probe_failure_is_advisory_and_redacted(profile_app, profile_client):
    """Verify probe failures stay advisory and redact sensitive error details."""
    def unreachable(url: str, timeout_seconds: float) -> bool:
        raise OSError("connect to 203.0.113.10 failed with token=actual-secret")

    profile_app.config["INFLUX_SINK_PROFILE_PROBE"] = unreachable
    created = _create_profile(profile_client)

    response = profile_client.post(
        f"/api/v1/influx-sink-profiles/{created['id']}/revisions/1/actions/test",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body == {
        "profile_id": created["id"],
        "tested_revision": 1,
        "status": "unreachable",
        "credentials_checked": False,
    }
    assert "203.0.113.10" not in str(body)
    assert "actual-secret" not in str(body)
