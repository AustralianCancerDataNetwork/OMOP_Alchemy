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
| `config show` / `override` | View and persist saved connection defaults |

See the [Command Reference](reference.md) for full parameter details.

---

## The `@omop_command` decorator

Most commands are decorated with `@omop_command`. This decorator handles all connection boilerplate so the command function body only needs to work with `conn` and `engine`.

### What it injects

Every decorated command receives three additional CLI flags, wired to identical Typer `Option` definitions across all commands:

| Flag | Type | Description |
|---|---|---|
| `--dotenv` | `str` (optional) | Path to a `.env` file loaded before connection resolution. Overrides the saved `DOTENV` default. |
| `--engine-schema` | `str` (optional) | Named engine configuration (e.g. `cdm`, `results`). Resolves to the `ENGINE_<SCHEMA>` environment variable group. |
| `--db-schema` | `str` (optional) | Database schema to target (e.g. `cdm5`, `vocab`). Sets `search_path` on PostgreSQL. Not supported on SQLite. |

Commands that support preview mode also receive `--dry-run` via the decorator.

### What it does behind the scenes

When a decorated command is invoked:

1. The decorator pops `dotenv`, `engine_schema`, and `db_schema` from the Typer kwargs.
2. It calls `resolve_connection(...)` to produce a `conn` object carrying those values merged with any saved defaults.
3. It prints a header showing the command name, engine schema, database schema, and mode label (apply / dry-run / inspect).
4. It calls `build_engine(...)` to create a SQLAlchemy `Engine`.
5. It calls the original function body with `(conn, engine, **remaining_kwargs)`.
6. Any `RuntimeError`, `SQLAlchemyError`, or `BackendNotSupportedError` raised by the body is caught and rendered as a formatted error, then exits with code 1.

### Before and after

Without the decorator, every command would need this boilerplate:

```python
def my_command(
    dotenv: str | None = typer.Option(None, help="..."),
    engine_schema: str | None = typer.Option(None, help="..."),
    db_schema: str | None = typer.Option(None, help="..."),
) -> None:
    conn = resolve_connection(dotenv=dotenv, engine_schema=engine_schema, db_schema=db_schema)
    console.print(render_command_header(...))
    try:
        engine = build_engine(dotenv=conn.dotenv, engine_schema=conn.engine_schema)
        # actual work here
    except Exception as exc:
        handle_error(exc)
```

With the decorator, the function body is all that matters:

```python
@app.command("my-command")
@omop_command("my-command")
def my_command(conn, engine) -> None:
    # conn and engine are ready to use
    results = do_work(engine, db_schema=conn.db_schema)
    console.print(render_results(results))
```

---

## The `conn` object

`conn` is a `ConnectionDefaults` instance. It exposes:

| Attribute | Description |
|---|---|
| `conn.dotenv` | Resolved dotenv path (from CLI flag or saved default) |
| `conn.engine_schema` | Resolved engine schema name |
| `conn.db_schema` | Resolved database schema name |
| `conn.athena_source` | Resolved Athena vocabulary CSV directory path |

---

## Connection resolution order

When the CLI resolves a connection parameter, it uses this precedence (highest to lowest):

1. Explicit CLI flag (e.g. `--db-schema cdm5`)
2. Saved default in the nearest `.omop-maint.toml` file
3. Command default (e.g. `vocabulary_included` defaults to `False` on most commands)

Use `omop-alchemy config override` to persist defaults so you do not need to repeat connection flags on every invocation.
