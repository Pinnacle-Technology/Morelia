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


