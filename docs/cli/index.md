# CLI Overview

The `omop-alchemy` command-line interface provides a suite of maintenance utilities for OMOP CDM databases. It is installed as part of the `omop-alchemy` package and is available on `PATH` after installation.

```bash
pip install omop-alchemy
omop-alchemy --help
```

## Command groups and flat commands

| Group / Command | What it covers |
|---|---|
| `info` | Environment inspection: package version, dependency status, connection state, per-command readiness |
| `doctor` | Health check: connection, schema drift, FK trigger state, FK violations, backup tooling |
| `reconcile-schema` | Compare ORM metadata against live column types, indexes, FK constraints, and cluster state |
| `create-missing-tables` | Detect and create ORM-managed OMOP tables that are absent from the database |
| `data-summary` | Row counts and existence state for ORM-managed tables |
| `load-vocab-source` | Load Athena CDM vocabulary CSV files |
| `analyze-tables` | ANALYZE or VACUUM ANALYZE selected tables to refresh planner statistics |
| `reset-sequences` | Reset owned PostgreSQL sequences to MAX(pk) + 1 |
| `truncate-tables` | Truncate selected ORM-managed tables |
| `indexes disable` / `enable` | Drop or recreate ORM-defined secondary indexes |
| `foreign-keys disable` / `enable` / `status` / `validate` | Manage PostgreSQL RI trigger enforcement |
| `fulltext install` / `populate` / `drop` | Manage tsvector sidecar columns on vocabulary tables |
| `backup-database` | Create a pg_dump backup artifact |
| `restore-database` | Restore a pg_dump or psql backup artifact |

See the [Command Reference](reference.md) for full parameter details.

---

## The `@omop_command` decorator

Most commands are decorated with `@omop_command`. This decorator handles all connection boilerplate so the command function body only needs to work with `conn` and `engine`.

### What it injects

Every decorated command receives:

- `conn` — a `_ConnContext` dataclass (see below)
- `engine` — a SQLAlchemy `Engine` ready to use
- `--dry-run` — injected on commands that support preview mode

No connection flags are injected; all configuration comes from oa_configurator.

### What it does behind the scenes

When a decorated command is invoked:

1. Loads `~/.config/omop/config.toml` via `load_stack_config()`.
2. Calls `OmopAlchemyConfig.from_stack(config)` to read package-specific settings and validate that the required `cdm_db` resource (or the `[tools.omop_alchemy] default_resource` override) is present. Raises `ConfigurationError` with a helpful message if it is missing.
3. Resolves the resource: `Resolver(config).resolve_resource("cdm_db")`.
4. Calls `.create_engine()` to build a SQLAlchemy engine with `schema_translate_map` applied.
5. Prints a command header showing the resource name, CDM schema, and run mode.
6. Calls the original function body with `(conn, engine, ...)`.
7. Catches `RuntimeError`, `SQLAlchemyError`, and `BackendNotSupportedError`; renders them as formatted errors and exits with code 1.

### Before and after

Without the decorator, every command would need this boilerplate:

```python
from omop_alchemy.config import TOOL_NAME
def my_command() -> None:
    stack = load_stack_config()
    tool = stack.tools.get(TOOL_NAME)
    resource_name = (tool.default_resource if tool else None) or "cdm_db"
    resolved = Resolver(stack).resolve_resource(resource_name)
    engine = resolved.create_engine()
    try:
        # actual work here
        results = do_work(engine, db_schema=resolved.cdm_schema)
        console.print(render_results(results))
    except Exception as exc:
        handle_error(exc)
```

With the decorator, the function body is all that matters:

```python
@app.command("my-command")
@omop_command("my-command")
def my_command(conn, engine) -> None:
    results = do_work(engine, db_schema=conn.db_schema)
    console.print(render_results(results))
```

---

## The `conn` object

`conn` is a `_ConnContext` dataclass. It exposes:

| Attribute | Description |
|---|---|
| `conn.db_schema` | CDM schema name from the resolved resource (e.g. `"omop"`) |
| `conn.athena_source` | Athena vocabulary CSV directory from `[tools.omop_alchemy.extra]`; `None` if not configured |
