# Maintenance CLI

OMOP Alchemy includes a maintenance CLI for common operational tasks on an OMOP CDM
database.

> **Alpha status**
> Treat this CLI as alpha operational tooling. Interfaces and behavior may still change.

---

## Entrypoint

```bash
omop-maint --help
python -m omop_alchemy.maintenance.cli --help
```

If you need PostgreSQL driver support:

```bash
uv sync --extra postgres
```

---

## Connection and defaults

Common flags used by many commands:

- `--dotenv`
- `--engine-schema`
- `--db-schema`

!!! info "Defaults file discovery"

  Project-local defaults are stored in `.omop-maint.toml`.
  
  - the CLI looks for the nearest ancestor directory containing `pyproject.toml`
    and uses `<that-directory>/.omop-maint.toml`
  - if no ancestor project marker is found, it falls back to `./.omop-maint.toml`
    in the current working directory
  - to force a fixed path, set `OMOP_MAINT_DEFAULTS_FILE`
  - running `omop-maint` from outside your intended project tree may use a different
    defaults file than expected

```bash
omop-maint config show
omop-maint config set-overrides --dotenv .env --engine-schema cdm --db-schema public --athena-source ./athena_source
omop-maint config clear-overrides
omop-maint config clear-overrides --db-schema
```

Resolution order:

1. explicit CLI flag
2. saved `.omop-maint.toml` default
3. command fallback

`engine_schema` selects the configured engine URL (`ENGINE_<SCHEMA>` or `ENGINE`).
`db_schema` selects the schema inside that database.

Schema-aware ORM tables are configured from environment variables before the model
package is imported. See [Configuration](../api/configuration.md) for the schema
env vars and defaults.

---

## Backend support at a glance

| Area | Commands |
| --- | --- |
| Backend-agnostic | `info`, `doctor`, `data-summary`, `reconcile-schema`, `create-missing-tables`, `indexes`, `load-vocab-source`, `analyze-tables` (PostgreSQL/SQLite) |
| PostgreSQL-only | `backup-database`, `restore-database`, `fulltext`, `reset-sequences`, `truncate-tables`, `foreign-keys` |

If a PostgreSQL-only command runs on an unsupported backend, the CLI returns a short
user-facing error.

---

## Command quick reference

| Command | Purpose | Key options | Backend |
| --- | --- | --- | --- |
| `info` | Show readiness and compatibility | `--engine-schema` | All |
| `doctor` | Read-only operational health check | `--deep`, `--vocab` | All (`--deep` is PostgreSQL-focused) |
| `data-summary` | Show managed tables and row counts | `--vocab`, `--include-missing` | All |
| `reconcile-schema` | Inspect drift between ORM and live schema | `--vocab` | All |
| `create-missing-tables` | Create absent ORM-managed tables | `--dry-run`, `--no-vocab` | All |
| `load-vocab-source` | Load Athena vocab CSVs | `--athena-source`, `--dry-run`, `--merge-strategy` | PostgreSQL, SQLite |
| `truncate-tables` | Truncate selected tables | `--scope` or `--table`, `--yes`, `--cascade`, `--restart-identities` | PostgreSQL |
| `reset-sequences` | Reset owned PK sequences to `MAX(pk) + 1` | `--dry-run`, `--vocab` | PostgreSQL |
| `foreign-keys` | Disable/enable/validate FK trigger enforcement | subcommands: `disable`, `enable`, `validate`, `status`; `--strict` | PostgreSQL |
| `analyze-tables` | Refresh planner statistics | `--scope`, `--table`, `--vacuum` | PostgreSQL, SQLite (`--vacuum` PostgreSQL-only) |
| `indexes` | Disable/enable ORM-defined secondary indexes | subcommands: `disable`, `enable`; `--dry-run`, `--vocab` | All (cluster apply on PostgreSQL) |
| `fulltext` | Manage sidecar `tsvector` columns and indexes | subcommands: `install`, `populate`, `drop`; `--regconfig`, `--no-create-indexes` | PostgreSQL |
| `backup-database` | Create PostgreSQL backup artifact | `--output-path`, `--format` | PostgreSQL |
| `restore-database` | Restore PostgreSQL backup artifact | backup path, `--format`, `--dry-run` | PostgreSQL |

---

## Minimal examples by area

### Inspect

```bash
omop-maint info
omop-maint doctor
omop-maint doctor --deep
```

### Schema

```bash
omop-maint reconcile-schema
omop-maint create-missing-tables --dry-run
omop-maint create-missing-tables
```

### Vocabulary

```bash
omop-maint load-vocab-source
omop-maint load-vocab-source --athena-source ./athena_source --dry-run
```

### Bulk reload helpers

```bash
omop-maint foreign-keys disable
omop-maint indexes disable
omop-maint truncate-tables --scope clinical --restart-identities --yes
```

After ETL:

```bash
omop-maint reset-sequences
omop-maint indexes enable
omop-maint foreign-keys enable --strict
omop-maint analyze-tables --scope clinical
```

### Full-text sidecars

```bash
omop-maint fulltext install
omop-maint fulltext populate
omop-maint fulltext drop
```

For query-side usage and optional ORM metadata registration, see
[PostgreSQL Full-Text Search](../advanced/fulltext.md).

### Backup and restore

```bash
omop-maint backup-database --engine-schema source --output-path ./cdm.dump
omop-maint restore-database ./cdm.dump --format custom --engine-schema target
```

---

## High-impact gotchas

- Run destructive commands with `--dry-run` first.
- `truncate-tables`, `foreign-keys`, `fulltext`, `backup-database`, and
  `restore-database` are PostgreSQL-only.
- `foreign-keys disable` and `enable` toggle PostgreSQL RI triggers; they do not drop
  FK definitions.
- `fulltext populate` must be rerun after bulk vocabulary changes because sidecar
  vectors do not auto-refresh.
- `indexes enable` may also apply PostgreSQL clustering, which can be heavy.
- `restore-database` restores into the configured target DB; it does not create or
  clean that DB for you.
- `restore-database` now requires an explicit `--format` (`custom` or `plain`);
  there is no automatic format detection.

---

## Help

```bash
omop-maint --help
omop-maint doctor --help
omop-maint fulltext --help
omop-maint config --help
```
