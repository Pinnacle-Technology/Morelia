from copy import deepcopy
from urllib.parse import parse_qs, urlsplit

import pytest

from app.database import db
from app.domain.enums import DeviceType
from app.models.runtime_manifest import RuntimeManifest
from app.repositories.device_configs import DeviceConfigRepository
from app.repositories.sessions import SessionRepository
from app.services import grafana_views


DESTINATION = {
    "url": "http://127.0.0.1:8086",
    "org": "morelia-mvp",
    "bucket": "recordings",
    "measurement": "default-measurement",
    "api_token_env": "MORELIA_INFLUX_TOKEN",
}


def _configure(app, probe):
    app.config.update(
        GRAFANA_PUBLIC_URL="http://127.0.0.1:3100",
        GRAFANA_HEALTH_URL="http://127.0.0.1:3100/api/health",
        GRAFANA_DASHBOARD_UID="morelia-m4-comparison",
        GRAFANA_DASHBOARD_SLUG="morelia-raw-vs-m4",
        GRAFANA_PANEL_ID=2,
        GRAFANA_INFLUX_URL=DESTINATION["url"],
        GRAFANA_INFLUX_ORG=DESTINATION["org"],
        GRAFANA_INFLUX_BUCKET=DESTINATION["bucket"],
        GRAFANA_INFLUX_MEASUREMENT=DESTINATION["measurement"],
        GRAFANA_HEALTH_PROBE=probe,
    )


def _session(
    app,
    *,
    device_type=DeviceType.POD8206HR,
    destination=None,
    source_template="device-templates/frozen pod.toml",
):
    frozen_path = "device-templates/frozen pod.toml"
    with app.app_context():
        device = DeviceConfigRepository().create(
            device_type=device_type,
            hardware_id=f"serial-{device_type.value}",
            port="COM7",
            nickname="Lab pod",
            source_template=source_template,
        )
        flow = {
            "device_config_id": device.id,
            "nickname": "North rig",
            "sinks": [
                {
                    "sink_name": "influx",
                    "sink_type": "influx",
                    "sink_parameters": dict(destination or DESTINATION),
                }
            ],
        }
        session = SessionRepository().create(
            {
                "name": f"Grafana {device_type.value}",
                "device_flows": [flow],
                "source_template_snapshot": {
                    "content": {
                        "device_flows": [
                            {"device_template_path": frozen_path, "sinks": []}
                        ]
                    }
                },
            }
        )
        return session.id


def test_ready_descriptor_is_allowlisted_encoded_secret_free_and_read_only(app):
    probes = []
    _configure(app, lambda url, timeout: probes.append((url, timeout)) or True)
    session_id = _session(app)
    with app.app_context():
        before = deepcopy(SessionRepository().get(session_id).device_flows)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view",
        query_string={"target_id": "0:0"},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["state"] == "ready"
    assert body["scope"] == "device_filters"
    assert "not isolated to this session" in body["notice"]
    assert body["ch_options"] == ["CH0", "CH1", "CH2"]
    assert body["selected_ch"] == ["CH0", "CH1", "CH2"]
    assert body["targets"] == [
        {
            "id": "0:0",
            "label": "North rig",
            "device": "device-templates/frozen pod.toml",
            "channels": ["CH0", "CH1", "CH2", "TTL1", "TTL2", "TTL3", "TTL4"],
        }
    ]
    query = parse_qs(urlsplit(body["embed_url"]).query)
    assert query["var-bucket"] == ["recordings"]
    assert query["var-measurement"] == ["default-measurement"]
    assert query["var-device"] == ["device-templates/frozen pod.toml"]
    assert "var-channel" not in query
    assert query["refresh"] == ["500ms"]
    assert query["var-show_ch"] == ["CH0", "CH1", "CH2"]
    assert query["panelId"] == ["2"]
    assert "/d-solo/morelia-m4-comparison/" in body["embed_url"]
    serialized = response.get_data(as_text=True)
    assert "MORELIA_INFLUX_TOKEN" not in serialized
    assert "api_token_env" not in serialized
    assert probes == [("http://127.0.0.1:3100/api/health", 1.0)]
    with app.app_context():
        assert SessionRepository().get(session_id).device_flows == before


# The M4 controls are echoed and encoded as allow-listed Grafana variables.
def test_m4_controls_are_encoded_into_the_grafana_urls(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view",
        query_string=[
            ("data_detail", "50ms"),
            ("show_ttl", "TTL1"),
            ("show_ttl", "TTL3"),
            ("show_ch", "CH2"),
            ("show_ch", "CH0"),
            ("show_ch", "CH2"),
        ],
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["data_detail_options"] == [
        "raw",
        "auto",
        "10ms",
        "25ms",
        "50ms",
        "100ms",
        "250ms",
        "500ms",
        "1s",
    ]
    assert body["selected_data_detail"] == "50ms"
    assert body["ttl_options"] == ["TTL1", "TTL2", "TTL3", "TTL4"]
    assert body["selected_ttl"] == ["TTL1", "TTL3"]
    assert body["selected_ch"] == ["CH0", "CH2"]
    query = parse_qs(urlsplit(body["embed_url"]).query)
    assert query["from"] == ["now-30s"]
    assert query["var-data_detail"] == ["50ms"]
    assert query["var-show_ttl"] == ["TTL1", "TTL3"]
    assert query["var-show_ch"] == ["CH0", "CH2"]
    open_query = parse_qs(urlsplit(body["open_url"]).query)
    assert open_query["var-show_ch"] == ["CH0", "CH2"]
    assert open_query["var-show_ttl"] == ["TTL1", "TTL3"]


# Clearing every TTL choice sends Grafana's explicit no-TTL sentinel.
def test_m4_controls_encode_an_empty_ttl_selection(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view",
        query_string={"data_detail": "250ms", "show_ttl": "__none__"},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["selected_ttl"] == []
    query = parse_qs(urlsplit(body["embed_url"]).query)
    assert query["var-show_ttl"] == ["__none__"]
    assert body["selected_ch"] == ["CH0", "CH1", "CH2"]
    assert query["var-show_ch"] == ["CH0", "CH1", "CH2"]


@pytest.mark.parametrize("ttl", ["TTL2", "__none__"])
def test_m4_controls_clear_ch_independently_of_ttl(app, ttl):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view",
        query_string={"show_ch": "__none__", "show_ttl": ttl},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["selected_ch"] == []
    assert body["selected_ttl"] == ([] if ttl == "__none__" else [ttl])
    for field in ("embed_url", "open_url"):
        query = parse_qs(urlsplit(body[field]).query)
        assert query["var-show_ch"] == ["__none__"]
        assert query["var-show_ttl"] == [ttl]


def test_service_empty_ch_list_means_none_and_leaves_ttl_default(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    with app.app_context():
        body = grafana_views.describe(session_id, config=app.config, show_ch=[])

    assert body["selected_ch"] == []
    assert body["selected_ttl"] == ["TTL1", "TTL2", "TTL3", "TTL4"]
    query = parse_qs(urlsplit(body["embed_url"]).query)
    assert query["var-show_ch"] == ["__none__"]
    assert query["var-show_ttl"] == ["TTL1", "TTL2", "TTL3", "TTL4"]


def test_pending_descriptor_matches_runtime_name_when_config_has_no_source_template(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app, source_template=None)

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    assert response.get_json()["targets"][0]["device"] == "pod8206hr"


def test_persisted_runtime_identity_survives_device_config_deletion(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)
    with app.app_context():
        session = SessionRepository().get(session_id)
        config = DeviceConfigRepository().get(session.device_flows[0]["device_config_id"])
        db.session.add(
            RuntimeManifest(
                hash="b" * 64,
                schema_version="2",
                session_id=session_id,
                content={
                    "device_flows": [
                        {
                            "device_id": "pod8206hr:serial-pod8206hr",
                            "name": "frozen-runtime-series",
                        }
                    ]
                },
            )
        )
        db.session.delete(config)
        db.session.commit()

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    body = response.get_json()
    assert body["state"] == "ready"
    assert body["targets"][0]["device"] == "frozen-runtime-series"


def test_8401_uses_established_device_specific_channel_names(app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app, device_type=DeviceType.POD8401HR)

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    assert response.get_json()["targets"][0]["channels"] == [
        "CHA",
        "CHB",
        "CHC",
        "CHD",
        "aEXT0",
        "aEXT1",
        "TTL1",
        "TTL2",
        "TTL3",
        "TTL4",
    ]


def test_unavailable_grafana_does_not_turn_the_descriptor_into_an_api_failure(app):
    _configure(app, lambda _url, _timeout: False)
    session_id = _session(app)

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    body = response.get_json()
    assert body["state"] == "unavailable"
    assert body["embed_url"] is None
    assert body["open_url"] is None
    assert "Acquisition is unaffected" in body["message"]
    assert body["targets"]


def test_destination_must_match_the_admin_datasource_exactly(app):
    probes = []
    _configure(app, lambda *_args: probes.append(True) or True)
    destination = {**DESTINATION, "bucket": "some-other-bucket"}
    session_id = _session(app, destination=destination)

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    assert response.get_json()["state"] == "not_applicable"
    assert probes == []


def test_matching_custom_measurement_is_passed_to_the_fixed_dashboard(app):
    _configure(app, lambda _url, _timeout: True)
    custom = {**DESTINATION, "measurement": "lab-voltage"}
    app.config["GRAFANA_INFLUX_MEASUREMENT"] = "lab-voltage"
    session_id = _session(app, destination=custom)

    response = app.test_client().get(f"/api/v1/sessions/{session_id}/grafana-view")

    assert response.status_code == 200
    assert response.get_json()["state"] == "ready"
    query = parse_qs(urlsplit(response.get_json()["embed_url"]).query)
    assert query["var-measurement"] == ["lab-voltage"]


@pytest.mark.parametrize(
    "query",
    [
        {"target_id": "9:9"},
    ],
)
def test_unknown_target_is_rejected(query, app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view", query_string=query
    )

    assert response.status_code == 422
    assert response.get_json()["code"] == "invalid_grafana_selection"


@pytest.mark.parametrize(
    "query",
    [
        {"data_detail": "5m"},
        [("show_ttl", "TTL1"), ("show_ttl", "__none__")],
        {"show_ttl": "TTL9"},
        {"show_ch": "CH3"},
        {"show_ch": "TTL1"},
        [("show_ch", "CH0"), ("show_ch", "__none__")],
        [("show_ch", "__none__"), ("show_ch", "__none__")],
    ],
)
# Unsupported M4 values cannot be forwarded into a Grafana URL.
def test_unknown_m4_control_is_rejected(query, app):
    _configure(app, lambda _url, _timeout: True)
    session_id = _session(app)

    response = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view", query_string=query
    )

    assert response.status_code == 422


def test_unconfigured_deployment_and_unknown_session_are_distinct(app):
    session_id = _session(app)
    app.config.update(
        GRAFANA_PUBLIC_URL="",
        GRAFANA_HEALTH_URL="",
        GRAFANA_DASHBOARD_UID="",
        GRAFANA_DASHBOARD_SLUG="",
        GRAFANA_INFLUX_URL="",
        GRAFANA_INFLUX_ORG="",
        GRAFANA_INFLUX_BUCKET="",
    )

    unconfigured = app.test_client().get(
        f"/api/v1/sessions/{session_id}/grafana-view"
    )
    missing = app.test_client().get("/api/v1/sessions/999999/grafana-view")

    assert unconfigured.status_code == 200
    assert unconfigured.get_json()["state"] == "not_configured"
    assert missing.status_code == 404
    assert missing.get_json()["code"] == "session_not_found"
