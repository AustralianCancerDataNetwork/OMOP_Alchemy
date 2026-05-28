# Quickstart (Experimental Docker Stack)

**This Docker stack is intended for local experimentation, development, and exploration only.**  
It is **not** hardened, secured, or tuned for production use.

The goal is to provide a fast, reproducible environment for:
- Exploring schemas and data
- Prototyping ETL / ORM logic
- Testing materialized views, loaders, and queries
- Running notebooks against a local PostgreSQL instance

---

## What this stack provides

When started with the appropriate profile, this stack runs:

- **PostgreSQL** (`postgres`)
  - Official `postgres:18` image with bulk-load-oriented runtime tuning in compose
  - Persistent storage via Docker volumes
- **Python workspace** (`python`)
  - Local OMOP Alchemy source installed into a reusable container image
  - PostgreSQL client tools included for direct `psql` / `pg_dump` access
- **pgAdmin** (`pgadmin`)
  - Web UI for inspecting and querying PostgreSQL (optional)
- **JupyterLab** (`cava-jupyter-notebook`, optional)
  - Notebook environment built from the local repo and wired to the same database

All services communicate on a dedicated Docker bridge network (`cava-network`).

---

## Prerequisites

You’ll need:

- Docker Desktop (or Docker Engine + Compose v2)
- `docker compose` available on your PATH
- A `.env` file in the `docker/` directory

---

## Environment configuration

Create a `.env` file alongside `docker-compose.yml`, for example:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=cava

HOST=localhost
HTTP_TYPE=http
```

These credentials are not secure and are intentionally simple for local use.

### Starting the stack

From the `docker/` directory.

#### Database + Python workspace

```
docker compose up -d
```

#### Database + Python workspace + pgAdmin

```
docker compose --profile pgadmin up -d
```

#### Database + Python workspace + Jupyter

```
docker compose --profile jupyter up -d
```

---

## Running PostgreSQL tests locally

The test suite includes PostgreSQL-specific tests that skip by default unless a test database is configured.

> **This test database is destructive.** The test suite drops and recreates the entire `public`
> schema on every run. `test_cdm_db` must point to a **dedicated, empty test database** — never
> to a database that contains real data. `configure-test-db` will refuse to save if the connection
> details match any existing resource in your config.

**Step 1 — Register a test database connection:**

```bash
omop-alchemy configure-test-db
```

Prompts for connection details (defaults: `localhost:55432`, `postgresql+psycopg`, `test/test/test_db`).
Press Enter to accept or supply your own values.

If the test user and database don't exist yet on your Postgres instance, add `--provision`:

```bash
omop-alchemy configure-test-db --provision
```

`--provision` uses your existing `cdm_db` admin connection to run `CREATE USER ... SUPERUSER` and
`CREATE DATABASE`. Both operations are idempotent — safe to re-run. You need a local PostgreSQL
instance with a superuser admin account.

> **Note on SUPERUSER**: the test user is created as a PostgreSQL superuser. This is required
> because the test suite disables FK constraint triggers during bulk vocabulary loads —
> an operation PostgreSQL restricts to superusers. This matches CI behaviour exactly.
> On a shared or production Postgres instance, provision the user manually instead.

**Step 2 — Run the tests:**

```bash
pytest -v tests/test_load_vocab_postgres.py
```
