"""Configuration profiles for the application factory.

We define one class per environment and select between them at startup.
Classes (not dicts) let later profiles *inherit* shared defaults and override
only what differs — the same idea as subclassing.

The golden rule (see https://12factor.net/config): anything that varies between
environments or is secret comes from the *environment*, never hard-coded.
"""

import os
from ipaddress import ip_address
from pathlib import Path

_INSTANCE_DIR = Path(__file__).resolve().parent.parent / "instance"


def _authorize_loopback_profile_management(request) -> bool:
    """Use the existing localhost bind boundary for the local dev control plane."""

    try:
        return ip_address(request.remote_addr).is_loopback
    except (ValueError, TypeError):
        return False


class Config:
    """Base configuration shared by every environment."""

    # Used by Flask to sign session cookies / CSRF tokens. The insecure default
    # is fine for local dev; production overrides it from the environment.
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-insecure-change-me")

    DEBUG = False
    TESTING = False
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    LOG_FORMAT = "console"
    # Redacted JSONL is split by session and emitting process layer. These files
    # are the source behind the Session Detail diagnostic viewer and TXT export.
    DIAGNOSTIC_LOG_DIR = os.environ.get(
        "DIAGNOSTIC_LOG_DIR", str(_INSTANCE_DIR / "diagnostics")
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    CONTROL_PLANE_BASE_URL = os.environ.get("PINNACLE_DAEMON_URL", "http://127.0.0.1:5000")
    # "morelia" is the real hardware+sink driver and the only supported driver.
    # A session either writes real data or fails loudly at spawn time (see
    # ensure_runtime_driver_ready), never silently no-ops.
    RUNTIME_DRIVER = os.environ.get("RUNTIME_DRIVER", "morelia")
    # Base directory for sink output files. A relative sink_location on a
    # device flow (or one the segment allocator generates when sink_location
    # is omitted) is resolved against this directory. An absolute
    # sink_location is used as-is. Mirrors flask-sqlalchemy's convention of
    # resolving relative paths under the instance directory.
    OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
    # Portable session-template files live in a stable library directory so the
    # CLI can list and reuse them instead of scattering TOML/JSON under the
    # caller's current working directory.
    SESSION_TEMPLATE_DIR = os.environ.get("SESSION_TEMPLATE_DIR", "session-templates")
    # Portable device-template files live in a stable library directory for the
    # same reason: exports should be discoverable and reusable without relying
    # on whichever directory the operator happened to be in.
    DEVICE_TEMPLATE_DIR = os.environ.get("DEVICE_TEMPLATE_DIR", "device-templates")
    SESSION_SCHEDULER_ENABLED = (
        os.environ.get("SESSION_SCHEDULER_ENABLED", "1").strip().lower()
        not in {"0", "false", "no", "off"}
    )
    SESSION_SCHEDULER_INTERVAL_SECONDS = float(
        os.environ.get("SESSION_SCHEDULER_INTERVAL_SECONDS", "1.0")
    )
    HOST_SUPERVISOR = None
    # ---- Watchdog / runtime-host timing -----------------------------------
    # Keep every watchdog-control timing knob here so operators can inspect
    # and tune the actual limits through settings.toml (or .env) without
    # editing source. Prefer settings.toml for portable non-secret knobs.
    WATCHDOG_FAILURE_THRESHOLD = int(os.environ.get("WATCHDOG_FAILURE_THRESHOLD", "3"))
    WATCHDOG_MAX_HEARTBEAT_AGE_SECONDS = float(
        os.environ.get("WATCHDOG_MAX_HEARTBEAT_AGE_SECONDS", "10.0")
    )
    WATCHDOG_FIRST_PACKET_TIMEOUT_SECONDS = float(
        os.environ.get(
            "WATCHDOG_FIRST_PACKET_TIMEOUT_SECONDS",
            str(WATCHDOG_MAX_HEARTBEAT_AGE_SECONDS),
        )
    )
    WATCHDOG_REPORT_INTERVAL_SECONDS = float(
        os.environ.get("WATCHDOG_REPORT_INTERVAL_SECONDS", "3.0")
    )
    WATCHDOG_STREAM_INTERVAL_SECONDS = float(
        os.environ.get("WATCHDOG_STREAM_INTERVAL_SECONDS", "1.0")
    )
    WATCHDOG_OPERATION_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_OPERATION_TIMEOUT_SECONDS", "5.0")
    )
    # Secondary fallback for legacy/incomplete telemetry that reports only an
    # unbounded port wait. Current runtimes open an incident when
    # WATCHDOG_MAX_HEARTBEAT_AGE_SECONDS is crossed; this longer limit ensures a
    # missed boundary report cannot leave an unplugged device invisible.
    STREAM_PORT_ABSENT_ESCALATION_SECONDS = float(
        os.environ.get("STREAM_PORT_ABSENT_ESCALATION_SECONDS", "30.0")
    )
    # Grace period for DataFlow workers to stop the device stream, drain pending
    # samples, flush file sinks, and exit. This must exceed the legacy Morelia
    # per-stream five-second default so PVFS catalog publication is not killed
    # midway through shutdown.
    WATCHDOG_DATAFLOW_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_DATAFLOW_STOP_TIMEOUT_SECONDS", "15.0")
    )
    WATCHDOG_PROCESS_LOOP_INTERVAL_SECONDS = float(
        os.environ.get("WATCHDOG_PROCESS_LOOP_INTERVAL_SECONDS", "1.0")
    )
    WATCHDOG_TELEMETRY_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_TELEMETRY_TIMEOUT_SECONDS", "5.0")
    )
    WATCHDOG_CONTROL_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_CONTROL_TIMEOUT_SECONDS", "2.0")
    )
    WATCHDOG_PROCESS_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_PROCESS_STOP_TIMEOUT_SECONDS", "20.0")
    )
    WATCHDOG_PROCESS_STOP_POLL_INTERVAL_SECONDS = float(
        os.environ.get("WATCHDOG_PROCESS_STOP_POLL_INTERVAL_SECONDS", "0.1")
    )
    WATCHDOG_PROCESS_REAP_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_PROCESS_REAP_TIMEOUT_SECONDS", "5.0")
    )
    WATCHDOG_PROCESS_LEGACY_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_PROCESS_LEGACY_STOP_TIMEOUT_SECONDS", "8.0")
    )
    WATCHDOG_CONTROL_SERVER_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_CONTROL_SERVER_STOP_TIMEOUT_SECONDS", "5.0")
    )
    WATCHDOG_OUTBOX_BUSY_TIMEOUT_MILLISECONDS = int(
        os.environ.get("WATCHDOG_OUTBOX_BUSY_TIMEOUT_MILLISECONDS", "5000")
    )
    WATCHDOG_THREAD_JOIN_GRACE_SECONDS = float(
        os.environ.get("WATCHDOG_THREAD_JOIN_GRACE_SECONDS", "1.0")
    )
    RUNTIME_HOST_STOP_DRAIN_TIMEOUT_SECONDS = float(
        os.environ.get("RUNTIME_HOST_STOP_DRAIN_TIMEOUT_SECONDS", "30.0")
    )
    RUNTIME_HOST_STOP_DRAIN_POLL_INTERVAL_SECONDS = float(
        os.environ.get("RUNTIME_HOST_STOP_DRAIN_POLL_INTERVAL_SECONDS", "0.2")
    )
    RUNTIME_HOST_STATUS_PROBE_TIMEOUT_SECONDS = float(
        os.environ.get("RUNTIME_HOST_STATUS_PROBE_TIMEOUT_SECONDS", "2.0")
    )
    RUNTIME_HOST_PROCESS_WAIT_TIMEOUT_SECONDS = float(
        os.environ.get("RUNTIME_HOST_PROCESS_WAIT_TIMEOUT_SECONDS", "5.0")
    )
    RUNTIME_HOST_SERVER_PUSH_TIMEOUT_SECONDS = float(
        os.environ.get("RUNTIME_HOST_SERVER_PUSH_TIMEOUT_SECONDS", "5.0")
    )
    RUNTIME_HOST_SERVER_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("RUNTIME_HOST_SERVER_STOP_TIMEOUT_SECONDS", "5.0")
    )
    RUNTIME_HOST_MAIN_LOOP_INTERVAL_SECONDS = float(
        os.environ.get("RUNTIME_HOST_MAIN_LOOP_INTERVAL_SECONDS", "1.0")
    )
    CONTROL_PLANE_POLL_INTERVAL_SECONDS = float(
        os.environ.get("CONTROL_PLANE_POLL_INTERVAL_SECONDS", "1.0")
    )
    CONTROL_PLANE_DELAYED_AFTER_SECONDS = float(
        os.environ.get("CONTROL_PLANE_DELAYED_AFTER_SECONDS", "5.0")
    )
    CONTROL_PLANE_UNREACHABLE_AFTER_SECONDS = float(
        os.environ.get("CONTROL_PLANE_UNREACHABLE_AFTER_SECONDS", "15.0")
    )
    CONTROL_PLANE_POLLER_STOP_TIMEOUT_SECONDS = float(
        os.environ.get("CONTROL_PLANE_POLLER_STOP_TIMEOUT_SECONDS", "5.0")
    )
    WATCHDOG_TELEMETRY_STALE_AFTER_SECONDS = float(
        os.environ.get("WATCHDOG_TELEMETRY_STALE_AFTER_SECONDS", "10.0")
    )
    WATCHDOG_TELEMETRY_OVERFLOW_AFTER_SECONDS = float(
        os.environ.get("WATCHDOG_TELEMETRY_OVERFLOW_AFTER_SECONDS", "60.0")
    )
    WATCHDOG_STALE_AFTER_SECONDS = float(
        os.environ.get("WATCHDOG_STALE_AFTER_SECONDS", "10.0")
    )
    # How long a latched ``degraded`` source-read status stays authoritative
    # without a fresh re-emit from the DataFlow worker. Morelia re-emits a
    # still-failing source about once per second; once those stop, the source
    # recovered or the stream ended. A latch older than this is cleared even if
    # the one-shot ``recovered`` event was dropped. Default 3.0s (~3x the emit
    # interval) so USB read jitter does not clear a still-failing source early.
    SOURCE_STATUS_STALE_AFTER_SECONDS = float(
        os.environ.get("SOURCE_STATUS_STALE_AFTER_SECONDS", "3.0")
    )
    WATCHDOG_RECOVERY_VERIFY_TIMEOUT_SECONDS = float(
        os.environ.get("WATCHDOG_RECOVERY_VERIFY_TIMEOUT_SECONDS", "5.0")
    )
    WATCHDOG_RECOVERY_RETRY_DELAYS_SECONDS = tuple(
        float(value)
        for value in os.environ.get(
            "WATCHDOG_RECOVERY_RETRY_DELAYS_SECONDS", "1,2,5,10,30,60"
        ).split(",")
        if value.strip()
    )
    # Once a deferred recovery has retried this many times with evidence that
    # never resolves to START_FRESH/ADOPT (e.g. a control-port probe that keeps
    # failing), stop retrying and mark the runtime stopped instead of retrying
    # forever at the last backoff interval.
    WATCHDOG_RECOVERY_MAX_ATTEMPTS = int(
        os.environ.get("WATCHDOG_RECOVERY_MAX_ATTEMPTS", "10")
    )
    STARTUP_RECONCILIATION_ENABLED = (
        os.environ.get("STARTUP_RECONCILIATION_ENABLED", "1").strip().lower()
        not in {"0", "false", "no", "off"}
    )
    STARTUP_RECONCILIATION_STATUS_PROBE = None
    # Restart-only mode: adopt matching live runtime hosts, but never replace
    # one whose identity or liveness cannot be proven.
    STARTUP_RECONCILIATION_ADOPT_ONLY = False
    # Base directory for per-watchdog-process SQLite telemetry outboxes. Each
    # watchdog process instance gets its own file under this directory, named
    # by watchdog_id (see app.watchdog_process.outbox.default_outbox_path), so
    # a respawned watchdog process never reopens the outbox of the one it
    # replaced. By default this lives in the application instance directory so
    # it is stable regardless of the process working directory. Deployments
    # may still override it with WATCHDOG_OUTBOX_DIR (prefer an absolute path).
    WATCHDOG_OUTBOX_DIR = os.environ.get(
        "WATCHDOG_OUTBOX_DIR", str(_INSTANCE_DIR / "watchdog-outbox")
    )
    # ---- Service-sink (Influx/Quest) raw delivery outbox -------------------
    # A SEPARATE durable buffer from the telemetry WatchdogOutbox above: it
    # spools raw, replayable Influx/Quest write payloads when a destination is
    # unavailable after start (see app.watchdog_process.sink_delivery_outbox
    # and gap SINK-27). Each watchdog instance gets its own SQLite file under
    # this directory, named by watchdog_id. Same relative/absolute resolution
    # convention as OUTPUT_DIR / WATCHDOG_OUTBOX_DIR.
    SINK_DELIVERY_OUTBOX_DIR = os.environ.get(
        "SINK_DELIVERY_OUTBOX_DIR", "sink-delivery-outbox"
    )
    # Per-sink age window: buffered samples older than this are permanently
    # dropped as loss. Default 300 seconds (5 minutes) per the sink audit.
    SINK_DELIVERY_OUTBOX_MAX_AGE_SECONDS = float(
        os.environ.get("SINK_DELIVERY_OUTBOX_MAX_AGE_SECONDS", "300.0")
    )
    # Per-sink byte cap: the total buffered bytes for one acquisition/sink key.
    # Default 256 MiB per the sink audit; whichever bound (age or bytes) is
    # reached first evicts the oldest samples.
    SINK_DELIVERY_OUTBOX_MAX_BYTES_PER_SINK = int(
        os.environ.get(
            "SINK_DELIVERY_OUTBOX_MAX_BYTES_PER_SINK", str(256 * 1024 * 1024)
        )
    )
    # Deployment-wide disk cap across the sum of all per-sink outboxes, so a
    # long outage on many sinks cannot exhaust the watchdog filesystem.
    # Default 2 GiB.
    SINK_DELIVERY_OUTBOX_MAX_TOTAL_BYTES = int(
        os.environ.get(
            "SINK_DELIVERY_OUTBOX_MAX_TOTAL_BYTES", str(2 * 1024 * 1024 * 1024)
        )
    )
    SINK_DELIVERY_OUTBOX_BUSY_TIMEOUT_MILLISECONDS = int(
        os.environ.get("SINK_DELIVERY_OUTBOX_BUSY_TIMEOUT_MILLISECONDS", "5000")
    )
    # ---- Influx sink-profile management ------------------------------------
    # Mutating profile routes delegate to a deployment-supplied callable. No
    # callable means deny: the application currently has no general user/role
    # authentication layer that this feature could safely pretend to reuse.
    INFLUX_SINK_PROFILE_AUTHORIZER = None
    INFLUX_SINK_PROFILE_ALLOWED_SCHEMES = tuple(
        value.strip().lower()
        for value in os.environ.get(
            "INFLUX_SINK_PROFILE_ALLOWED_SCHEMES",
            "http,https",
        ).split(",")
        if value.strip()
    )
    # Both an exact hostname and every resolved address must be allowlisted.
    # Empty defaults fail closed. Operators should keep these CIDRs narrow and
    # align deployment egress rules with the same destinations.
    INFLUX_SINK_PROFILE_ALLOWED_HOSTS = tuple(
        value.strip()
        for value in os.environ.get("INFLUX_SINK_PROFILE_ALLOWED_HOSTS", "").split(",")
        if value.strip()
    )
    INFLUX_SINK_PROFILE_ALLOWED_NETWORKS = tuple(
        value.strip()
        for value in os.environ.get("INFLUX_SINK_PROFILE_ALLOWED_NETWORKS", "").split(",")
        if value.strip()
    )
    INFLUX_SINK_PROFILE_PROBE_TIMEOUT_SECONDS = float(
        os.environ.get("INFLUX_SINK_PROFILE_PROBE_TIMEOUT_SECONDS", "3.0")
    )
    INFLUX_SINK_PROFILE_PROBE_MAX_RESPONSE_BYTES = int(
        os.environ.get("INFLUX_SINK_PROFILE_PROBE_MAX_RESPONSE_BYTES", "4096")
    )
    # Trusted resolver seam for controlled deployments and deterministic tests.
    # The probe override is honored only while TESTING; production always uses
    # the built-in address-pinned health transport.
    INFLUX_SINK_PROFILE_RESOLVER = None
    INFLUX_SINK_PROFILE_PROBE = None
    WATCHDOG_HARDWARE_LOCK_DIR = os.environ.get(
        "WATCHDOG_HARDWARE_LOCK_DIR", str(_INSTANCE_DIR / "watchdog-hardware-locks")
    )
    WATCHDOG_RESPAWN_MAX_ATTEMPTS = int(os.environ.get("WATCHDOG_RESPAWN_MAX_ATTEMPTS", "3"))
    WATCHDOG_RESPAWN_RETRY_DELAY_SECONDS = float(
        os.environ.get("WATCHDOG_RESPAWN_RETRY_DELAY_SECONDS", "1.0")
    )
    WATCHDOG_STALE_GRACE_ATTEMPTS = int(
        os.environ.get("WATCHDOG_STALE_GRACE_ATTEMPTS", "20")
    )
    # ---- Output finalization (crash-safe EDF/PVFS merge coordination) -------
    # The control plane owns a durable, fenced finalization JOB; a dedicated
    # finalizer process (app.finalizer_process) runs one merge attempt at a
    # time without ever owning hardware. These knobs tune the claim/lease/retry
    # state machine and component retention. See gap SINK-26 and section 7 of
    # docs/all-sink-support-design-and-gap-audit.md.
    #
    # How long a superseded component (the linked B/B_1/... segments) is kept
    # on disk AFTER a verified publish is durable, before a separate cleanup
    # job (packet 29) may delete it. Default 7 days.
    FINALIZER_COMPONENT_RETENTION_SECONDS = float(
        os.environ.get("FINALIZER_COMPONENT_RETENTION_SECONDS", str(7 * 24 * 3600))
    )
    # A merge attempt whose lease heartbeat is older than this is considered
    # abandoned (crashed finalizer) and may be re-claimed by a fresh attempt,
    # which bumps the fence token and fences out the stale worker. Default 300s.
    FINALIZER_LEASE_TTL_SECONDS = float(
        os.environ.get("FINALIZER_LEASE_TTL_SECONDS", "300.0")
    )
    # How often the active finalizer refreshes its lease heartbeat while a
    # merge is in progress. Must be well below FINALIZER_LEASE_TTL_SECONDS.
    FINALIZER_HEARTBEAT_INTERVAL_SECONDS = float(
        os.environ.get("FINALIZER_HEARTBEAT_INTERVAL_SECONDS", "15.0")
    )
    # Bounded retry budget: after this many failed merge attempts a logical
    # output stays merge_failed and is not re-claimed automatically, so a
    # permanently unreadable segment cannot spin forever.
    FINALIZER_MAX_MERGE_ATTEMPTS = int(
        os.environ.get("FINALIZER_MAX_MERGE_ATTEMPTS", "5")
    )
    # Bounded backoff (seconds) between successive merge attempts for the same
    # logical output; the last value is reused once the list is exhausted.
    FINALIZER_RETRY_BACKOFF_SECONDS = tuple(
        float(value)
        for value in os.environ.get(
            "FINALIZER_RETRY_BACKOFF_SECONDS", "5,15,60,300"
        ).split(",")
        if value.strip()
    )
    # Finalizer-process main-loop poll interval: how often it scans for a
    # claimable logical output when idle.
    FINALIZER_POLL_INTERVAL_SECONDS = float(
        os.environ.get("FINALIZER_POLL_INTERVAL_SECONDS", "5.0")
    )
    FINALIZER_PROCESS_ENABLED = os.environ.get("FINALIZER_PROCESS_ENABLED", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    # Base directory for the finalizer's temporary merged artifacts. A merger
    # (packets 17/18) writes and verifies its temp file on the SAME filesystem
    # as the components before an atomic publish; this is the fallback base
    # when a format cannot derive a same-directory temp path. Same
    # relative/absolute resolution convention as OUTPUT_DIR.
    FINALIZER_TEMP_DIR = os.environ.get("FINALIZER_TEMP_DIR", "finalizer-temp")
    # Northbound ingest — the URL the spawned host POSTs reports to.
    # Empty string disables push (poller-only mode).
    INGEST_BASE_URL = os.environ.get("INGEST_BASE_URL", CONTROL_PLANE_BASE_URL)
    # Optional token the host sends as X-Agent-Token on every POST to this plane.
    INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "") or None
    # SSE live endpoint tuning.
    SSE_POLL_INTERVAL = float(os.environ.get("SSE_POLL_INTERVAL", "2"))
    SSE_HEARTBEAT_INTERVAL = float(os.environ.get("SSE_HEARTBEAT_INTERVAL", "15"))
    # ---- flask-smorest / OpenAPI -------------------------------------------
    # These drive the auto-generated OpenAPI document and the docs UIs.
    API_TITLE = "Guarded Experiment Dashboard API"
    API_VERSION = "v1"
    OPENAPI_VERSION = "3.0.3"  # the OpenAPI *spec* version, not our API version
    # Serve the spec + interactive docs at the app root.
    OPENAPI_URL_PREFIX = "/"
    OPENAPI_JSON_PATH = "openapi.json"  # -> GET /openapi.json (the machine-readable contract)
    OPENAPI_SWAGGER_UI_PATH = "/docs"  # -> GET /docs (human-friendly "try it" UI)
    OPENAPI_SWAGGER_UI_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"


class DevelopmentConfig(Config):
    """Local development: verbose errors, auto-reload-friendly."""

    DEBUG = True
    # The current dashboard is explicitly a localhost-only control plane. This
    # does not trust forwarding headers and is not a replacement for real user
    # authentication if remote binding is ever enabled.
    INFLUX_SINK_PROFILE_AUTHORIZER = staticmethod(
        _authorize_loopback_profile_management
    )
    # Relative SQLite paths are resolved under Flask's instance directory.
    # Source: https://flask-sqlalchemy.palletsprojects.com/en/stable/config/#connection-url-format
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "DATABASE_URL",
        "sqlite:///guarded-experiment.sqlite3",
    )


class TestingConfig(Config):
    """Test runs: TESTING=True makes Flask propagate errors to pytest instead
    of swallowing them into 500 responses, which makes failures legible."""

    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
    STARTUP_RECONCILIATION_ENABLED = False
    SESSION_SCHEDULER_ENABLED = False
    FINALIZER_PROCESS_ENABLED = False
    DIAGNOSTIC_LOG_DIR = None
    INGEST_BASE_URL = ""
    # No sleep between polls so SSE generator tests are instant.
    SSE_POLL_INTERVAL = 0.0
    # Very long heartbeat window so heartbeat frames don't appear mid-test.
    SSE_HEARTBEAT_INTERVAL = 9999.0


class ProductionConfig(Config):
    """Production: never trust a default secret."""

    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    LOG_FORMAT = "json"


_CONFIGS: dict[str, type[Config]] = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
}


def get_config(name: str | None = None) -> type[Config]:
    """Resolve a config class by name.

    Precedence: explicit argument > FLASK_CONFIG env var > "development".
    """
    name = name or os.environ.get("FLASK_CONFIG", "development")
    return _CONFIGS.get(name, DevelopmentConfig)
