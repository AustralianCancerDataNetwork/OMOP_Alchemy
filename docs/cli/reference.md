# Command Reference

Connection and schema configuration comes from `~/.config/omop/config.toml` — no per-command connection flags are needed. See the [CLI Overview](index.md) for how the `@omop_command` decorator resolves the connection, and [Configuration](../getting-started/configuration.md) for setup.

---

## Schema inspection

### `info`

Inspect maintenance CLI readiness, backend compatibility, and current installation state.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the managed-table count. |

---

### `doctor`

Run a read-only maintenance health check across connection readiness, schema drift, and FK state.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--deep` | bool | `False` | Include heavier checks: FK validation scans every constraint for referential integrity violations. |

---

### `reconcile-schema`

Compare ORM-managed SQLAlchemy metadata against the current target database schema.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the reconciliation. |

---

### `create-missing-tables`

Create missing ORM-managed OMOP tables from metadata.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `True` | Include OMOP vocabulary tables in the selection. Enabled by default. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `data-summary`

Summarise ORM-managed OMOP tables present in the target database.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the summary. |
| `--include-missing` | bool | `False` | Also list ORM-managed tables that are absent from the target database. |

---

## Vocabulary

### `load-vocab-source`

Load all Athena vocabulary CSVs from the configured source path, optionally toggling indexes and FK triggers for speed.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--athena-source` | str (optional) | (saved default) | Path to the unzipped Athena vocabulary CSV directory. Falls back to the saved athena-source default. |
| `--merge-strategy` | `replace` / `upsert` / `insert_if_empty` | `replace` | CSV merge strategy. `replace` keeps the DB in sync with the source. `upsert` is incremental and non-destructive. `insert_if_empty` is the fast path for a fresh empty target. |
| `--chunksize` | int (optional) | `100000` | Chunk size for fallback ORM CSV loading. Defaults to 100 000 rows. Pass `0` to disable chunking. |
| `--bulk-mode` / `--no-bulk-mode` | bool | `True` | Disable FK triggers and drop indexes globally before loading, then rebuild after. Much faster for a full vocabulary reload. Ignored on backends that do not support it. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

## Tables

### `analyze-tables`

Analyse selected ORM-managed tables to update planner statistics.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--scope` | `clinical` / `vocabulary` / `derived` / ... (optional) | all tables | CDM category scope to analyze. Defaults to all ORM-managed tables when omitted. |
| `--table` | str (repeatable, optional) | (none) | Specific ORM-managed table name to analyze. Repeat to target multiple tables. |
| `--vacuum` | bool | `False` | Use VACUUM ANALYZE instead of plain ANALYZE to also reclaim dead tuples. Not available on all backends. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `reset-sequences`

Reset each owned sequence to MAX(pk) + 1 to prevent insert conflicts after bulk loads.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `truncate-tables`

Truncate selected ORM-managed OMOP tables. Aborts if external FK references would block unless `--cascade` is set.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--scope` | str (optional) | (none) | CDM category scope to truncate (e.g. `clinical`, `vocabulary`). Must specify scope or `--table`. |
| `--table` | str (repeatable, optional) | (none) | Specific ORM-managed table name to truncate. Repeat to target multiple tables. |
| `--restart-identities` | bool | `False` | Reset owned sequences to 1 after truncation (`TRUNCATE ... RESTART IDENTITY`). |
| `--cascade` | bool | `False` | Automatically truncate dependent tables via PostgreSQL CASCADE. Use with care. |
| `--yes` | bool | `False` | Confirm the destructive operation. Required when not using `--dry-run`. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

## Indexes

### `indexes disable`

Drop all ORM-defined secondary indexes from the target database. Useful before bulk data loads.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `indexes enable`

Recreate all ORM-defined secondary indexes. Also CLUSTERs tables on PostgreSQL where metadata specifies it.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

## Foreign keys

### `foreign-keys disable`

Disable PostgreSQL RI trigger enforcement for all participating OMOP tables.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--strict` | bool | `False` | Validate all FK relationships and report violations before disabling trigger enforcement. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `foreign-keys enable`

Re-enable PostgreSQL RI trigger enforcement. Use `--strict` to abort if any violations exist first.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |
| `--strict` | bool | `False` | Validate all FK relationships before enabling trigger enforcement. Aborts if any violations are found. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `foreign-keys status`

Show the current enabled/disabled state of RI triggers for each participating OMOP table.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |

---

### `foreign-keys validate`

Validate FK constraints on selected tables and report any rows that violate referential integrity.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--vocab` / `--no-vocab` | bool | `False` | Include OMOP vocabulary tables in the selection. |

---

## Full-text search

### `fulltext install`

Add tsvector sidecar columns to vocabulary tables and optionally create GIN indexes for fast full-text search.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--create-indexes` / `--no-create-indexes` | bool | `True` | Create GIN indexes alongside the tsvector columns for fast full-text search. |
| `--fastupdate` / `--no-fastupdate` | bool | `False` | Enable PostgreSQL GIN fastupdate on newly created indexes (trades write speed for query latency). |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `fulltext populate`

Fill tsvector sidecar columns with pre-computed search vectors using the specified PostgreSQL text search configuration.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--regconfig` | str | `english` | PostgreSQL text search configuration to use when building tsvector values (e.g. `english`, `simple`). |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `fulltext drop`

Remove tsvector sidecar columns and their associated GIN indexes from vocabulary tables.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--drop-indexes` / `--no-drop-indexes` | bool | `True` | Drop managed GIN indexes before dropping the tsvector columns. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

## Backup and restore

### `backup-database`

Create a database backup that can be restored with `restore-database`.

| Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `--output-path` | str (optional) | timestamped file in cwd | Output path for the backup artifact. Defaults to a timestamped file in the current directory. |
| `--format` | `custom` / `plain` | `custom` | pg_dump output format. `custom` produces a binary `.dump` file. `plain` produces a plain SQL `.sql` file. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

### `restore-database`

Restore a database backup that was created with `backup-database`.

| Argument / Flag | Type / Choices | Default | Description |
|---|---|---|---|
| `PATH` (argument) | str | (required) | Path to the backup artifact (`.dump` or `.sql`) to restore. |
| `--format` | `custom` / `plain` | (required) | Format of the artifact to restore. Must match the format used when the backup was created. |
| `--dry-run` | bool | `False` | Preview planned actions without applying any changes to the database. |

---

## Configuration

Connection and schema settings are managed via `omop-config` (oa_configurator). Use `omop-config init` to create `~/.config/omop/config.toml` and `omop-config configure omop_alchemy` to set package-specific options such as `athena_source_path`. See [Configuration](../getting-started/configuration.md).
