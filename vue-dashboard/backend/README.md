# Guarded Experiment Dashboard — Backend

Flask backend. Validates requests, stores dashboard data in SQLite, and
manages watchdog processes. (See `../frontend/guarded-experiment-dashboard.md`
for the full system design.)

## Setup

```bash
# 1. Create an isolated virtual environment (one per project).
python -m venv .venv

# 2. Activate it.
#    Windows (PowerShell): .venv\Scripts\Activate.ps1
#    Git Bash / macOS / Linux: source .venv/Scripts/activate  (Windows)
#                              source .venv/bin/activate       (Unix)

# 3. Install the app plus dev tools, editable so code changes take effect live.
pip install -e ".[dev]"

# 4. Create / upgrade the SQLite schema (required on first start).
flask --app app db upgrade
```

Schema is managed by Flask-Migrate / Alembic under `migrations/`. The default
database is `guarded-experiment.sqlite3` (override with `DATABASE_URL`). Relative
SQLite paths resolve under Flask's `instance/` directory.

To reset a disposable local database and re-apply migrations:

```bash
# PowerShell
Remove-Item .\instance\guarded-experiment.sqlite3 -ErrorAction SilentlyContinue
flask --app app db upgrade

# Unix
# rm -f instance/guarded-experiment.sqlite3 && flask --app app db upgrade
```

Optional readiness check: `flask --app app pinnacle doctor`.

## Run

```bash
flask --app app run --debug
# Flask auto-discovers the create_app() factory in the `app` package.
# Then: curl http://127.0.0.1:5000/health
```

## Operational rules

For USB presence, connection, or recovery errors, inspect the automatic
[hardware diagnostic events](tools/hardware_diagnostics.md) in the session log.
They include the inventory used by the backend, exact target identity, failure
stage, and exception chain.

### Single owner of hardware

Exactly **one** control-plane process may own a given piece of hardware at a
time. The backend never drives Morelia devices directly — it talks to a Watchdog
over the versioned localhost HTTP contract (`docs/watchdog-http-v1.md`), and the
Watchdog process is the sole owner of the serial ports / DataFlow workers. Two
processes contending for the same device is a corruption/lockup hazard, so any
future code that acquires a hardware-owning resource must ensure it is the only
one doing so.

### Flask debug-reloader guard

`flask run --debug` enables Werkzeug's auto-reloader, which runs the app in
**two** processes: a supervisor that watches files and a child that actually
serves requests. That means `create_app()` runs twice. Today this is harmless
(the backend owns no hardware), but any code that starts a hardware-owning
resource at boot must run in the serving child only — never the supervisor —
or it would become a second owner and violate the rule above. Guard such paths
on the child-process marker:

```python
import os

# Werkzeug sets this to "true" only in the reloader's serving child.
if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_hardware_owning_thing()
```

For non-dev entrypoints (production WSGI, CLI), start the resource directly with
the reloader disabled (`use_reloader=False`) so there is only ever one process.

## Test & lint

```bash
pytest          # run the test suite
ruff check      # lint
ruff format     # auto-format (optional)
```

## Layout

| Path                  | Responsibility                                  |
| --------------------- | ----------------------------------------------- |
| `app/__init__.py`     | `create_app()` application factory              |
| `app/config.py`       | Per-environment configuration profiles          |
| `app/health/`         | Liveness (`/health`) & readiness (`/ready`)     |
| `app/watchdog/`       | Versioned Watchdog contract and adapters         |
| `migrations/`         | Alembic schema revisions (`flask db upgrade`)   |
| `docs/watchdog-http-v1.md` | Local HTTP/JSON wire contract             |
| `tests/`              | pytest suite + shared fixtures (`conftest.py`)  |
| `pyproject.toml`      | Dependencies, ruff, and pytest configuration    |
