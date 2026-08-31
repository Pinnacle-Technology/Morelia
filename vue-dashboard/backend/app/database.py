"""SQLAlchemy extension shared by the application and future model modules."""

from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from importlib import import_module

from flask import Flask
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.orm import DeclarativeBase, scoped_session

DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5_000

# Worker processes intentionally use the small database app factory instead of
# the full HTTP application factory.  Keep model registration explicit at this
# boundary so a worker still has the complete metadata graph when it persists
# an object with foreign keys to another model.
MODEL_MODULES = (
    "app.models.backend_event",
    "app.models.device_config",
    "app.models.device_registration",
    "app.models.device_seen",
    "app.models.device_template",
    "app.models.experiment",
    "app.models.incident",
    "app.models.influx_sink_profile",
    "app.models.operation",
    "app.models.output_file",
    "app.models.recovery_gap",
    "app.models.runtime_manifest",
    "app.models.runtime_ownership",
    "app.models.session",
    "app.models.session_activity_entry",
    "app.models.session_note",
    "app.models.session_template",
)


class Base(DeclarativeBase):
    """Declarative base for all persisted models."""


# Flask-SQLAlchemy's documented factory pattern creates the extension once and
# binds it to each application with db.init_app().
# Source: https://flask-sqlalchemy.palletsprojects.com/en/stable/quickstart/#initialize-the-extension
db = SQLAlchemy(model_class=Base)
# Flask-Migrate exposes Alembic through Flask's CLI and supports init_app().
# Source: https://flask-migrate.readthedocs.io/en/latest/#command-reference
migrate = Migrate(compare_type=True, render_as_batch=True)


def ensure_models_registered() -> None:
    """Import every ORM model into the shared declarative metadata registry.
    """
    for module_name in MODEL_MODULES:
        import_module(module_name)


@contextmanager
def transaction() -> Iterator[scoped_session]:
    """Run a unit of work atomically against ``db.session``.

    Commits when the block exits cleanly; rolls back and re-raises if the block
    raises. This is the boundary services use so a failed multi-record operation
    leaves no partial state.

    The manual commit/rollback (rather than ``db.session.begin()``) is
    deliberate: the scoped session may already have autobegun a transaction by
    the time a service reaches this helper, and ``begin()`` would raise in that
    case. Re-raising preserves the original exception for the caller.
    """
    try:
        yield db.session
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def init_database(app: Flask) -> None:
    """Initialize SQLAlchemy and install SQLite connection safety settings."""
    db.init_app(app)
    migrate.init_app(app, db)
    ensure_models_registered()

    with app.app_context():
        engine = db.engine

    if engine.dialect.name != "sqlite":
        return

    busy_timeout_ms = int(
        app.config.get("SQLITE_BUSY_TIMEOUT_MS", DEFAULT_SQLITE_BUSY_TIMEOUT_MS)
    )
    event.listen(
        engine,
        "connect",
        partial(_configure_sqlite_connection, busy_timeout_ms=busy_timeout_ms),
    )


def create_database_app(
    config_name: str | None = None,
    config_overrides: dict | None = None,
) -> Flask:
    """Create the minimal Flask app needed by worker-side DB access.

    This deliberately does not call ``app.create_app``to avoid startup reconcilation that
    preventing runtime_host to be respawned.
    """
    from app.config import get_config

    app = Flask("app.database")
    app.config.from_object(get_config(config_name))
    if config_overrides:
        app.config.update(config_overrides)
    init_database(app)
    return app


def _configure_sqlite_connection(
    dbapi_connection,
    _connection_record,
    *,
    busy_timeout_ms: int,
) -> None:
    """Apply connection-local SQLite pragmas before SQLAlchemy uses it."""
    previous_autocommit = getattr(dbapi_connection, "autocommit", None)
    if previous_autocommit is not None:
        dbapi_connection.autocommit = True

    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
    finally:
        cursor.close()
        if previous_autocommit is not None:
            dbapi_connection.autocommit = previous_autocommit
