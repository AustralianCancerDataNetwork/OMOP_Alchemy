# Maintenance CLI

OMOP Alchemy includes a dedicated maintenance CLI for common operational tasks against
an OMOP CDM database.

> **Alpha status:**
> The maintenance CLI should currently be treated as alpha-state operational tooling.
> It was vibe-coded and although it has been put together with care and in collaboration
> with the coding assistant, no guarantees are currently made and it is expected to 
> undergo some pretty serious refactoring before being declared stable.

The CLI is designed for:

- running a read-only operational doctor check
- inspecting maintenance readiness and compatibility
- inspecting which OMOP Alchemy-managed tables exist
- reconciling live database schema against ORM metadata
- creating missing tables from ORM metadata
- loading Athena vocabulary CSV files through the ORM staged CSV loader
- truncating selected PostgreSQL OMOP tables before reloads
- refreshing planner statistics on supported backends
- resetting PostgreSQL sequences after truncation and ETL reruns
- temporarily disabling and re-enabling PostgreSQL foreign key trigger enforcement
- validating PostgreSQL foreign key relationships before re-enabling enforcement
- disabling and re-enabling ORM-defined secondary indexes
- applying ORM-defined PostgreSQL clustering metadata
- creating restore-ready PostgreSQL backup artifacts
- restoring PostgreSQL backup artifacts into a target environment

---

## Command entrypoint

If the package is installed with its console script, use:

```bash
omop-maint --help
```

You can also invoke the module directly:

```bash
python -m omop_alchemy.maintenance.cli --help
```

If you need PostgreSQL driver support:

```bash
uv sync --extra postgres
```

---

## Connection defaults

Most maintenance commands accept:

- `--dotenv`
- `--engine-schema`
- `--db-schema`

The maintenance defaults file can also persist:

- `athena_source`: a path to unzipped Athena vocabulary files for future vocabulary
  loading workflows

To avoid repeating these every time, the CLI can persist project-local defaults in
`.omop-maint.toml`.

Show the currently saved overrides:

```bash
omop-maint config show
```

Set overrides:

```bash
omop-maint config set-overrides --dotenv .env --engine-schema cdm --db-schema public --athena-source ./athena_source
```

Clear all saved overrides:

```bash
omop-maint config clear-overrides
```

Clear only one override field:

```bash
omop-maint config clear-overrides --db-schema
```

Resolution order is:

1. explicit CLI flag
2. saved `.omop-maint.toml` default
3. command fallback

`engine_schema` selects which configured engine URL is used via `ENGINE_<SCHEMA>` or
`ENGINE`, while `db_schema` is the schema inside that connected database.

Important note:

- `.omop-maint.toml` is resolved from the project root by default, where project root
  means the nearest ancestor directory containing `pyproject.toml`
- this makes defaults discovery deterministic anywhere inside the same project tree
- if no project root is found, the CLI falls back to `./.omop-maint.toml`
- running `omop-maint` outside the intended project tree can still pick up a different
  overrides file or no overrides file at all
- if you need a fixed location, set `OMOP_MAINT_DEFAULTS_FILE`

---

## Backend support

Some commands are backend-agnostic, while others are PostgreSQL-specific.

Backend-agnostic:

- `info`
- `doctor`
- `data-summary`
- `analyze-tables` on PostgreSQL and SQLite
- `reconcile-schema`
- `create-missing-tables`
- `indexes`
- `load-vocab-source` on SQLite and PostgreSQL

PostgreSQL-only:

- `backup-database`
- `restore-database`
- `reset-sequences`
- `truncate-tables`
- `foreign-keys`

If a PostgreSQL-only command is run against an unsupported backend, the CLI fails with
a short user-facing error instead of a raw DBAPI traceback.

---

## Environment inspection

### `info`

Shows current installation and compatibility state for the maintenance CLI, including:

- configured defaults
- backend and connection readiness
- PostgreSQL client tooling availability
- command compatibility for the current environment

Example:

```bash
omop-maint info
omop-maint info --engine-schema cdm
```

### `doctor`

Runs a read-only operational health check over:

- connection readiness
- missing ORM-managed tables
- schema drift against ORM metadata
- PostgreSQL foreign key trigger state
- PostgreSQL backup-tool availability

On PostgreSQL, `--deep` also validates selected foreign key relationships.

Examples:

```bash
omop-maint doctor
omop-maint doctor --deep
omop-maint doctor --vocabulary-included
```

Notes:

- `doctor` is intended to be a quick operational checkpoint before and after ETL work
- `doctor --deep` is more expensive because it runs live FK validation queries on PostgreSQL

---

## Table inspection

### `data-summary`

Shows OMOP Alchemy-managed tables in the connected database, including category,
primary key columns, and row counts.

Examples:

```bash
omop-maint data-summary
omop-maint data-summary --vocabulary-included
omop-maint data-summary --include-missing
```

Useful options:

- `--vocabulary-included`: include vocabulary tables
- `--include-missing`: also show ORM-managed tables that do not currently exist

Gotcha:

- this command performs `COUNT(*)` against every selected existing table, so it can be
  slow on large datasets and should not be treated as a lightweight health check

### `reconcile-schema`

Performs a full inspect-based reconciliation between ORM metadata and the live database.

It reports drift across:

- table existence
- columns
- primary keys
- foreign keys
- indexes
- PostgreSQL clustering metadata

Examples:

```bash
omop-maint reconcile-schema
omop-maint reconcile-schema --vocabulary-included
```

This command is inspect-only. It does not modify the target database.

Gotchas:

- database-only objects are reported as drift, so hand-managed indexes, constraints,
  or columns added outside ORM metadata will appear as unexpected differences
- PostgreSQL clustering drift is only inspected on PostgreSQL backends

---

## Schema creation

### `create-missing-tables`

Creates ORM-managed tables that do not yet exist in the target database.

Examples:

```bash
omop-maint create-missing-tables --dry-run
omop-maint create-missing-tables
omop-maint create-missing-tables --no-vocabulary-included
```

Notes:

- defaults to including vocabulary tables
- respects `--db-schema` if you want to create tables in a specific schema
- uses ORM metadata as the source of truth

Important gotchas:

- the target schema must already exist; this command does not create schemas
- missing tables are now created from a schema-adjusted metadata graph instead of one
  table at a time, so dependency ordering is handled much more safely than before
- this still assumes the referenced external schema objects already exist; unresolved
  dependencies outside the selected ORM-managed set are reported as blocked
- `--no-vocabulary-included` is not suitable for bootstrapping a fresh clinical schema
  from nothing, because many clinical tables depend on vocabulary tables such as
  `concept`
- a brand new PostgreSQL database is still an alpha workflow and should be verified
  afterwards with `omop-maint reconcile-schema`

---

## Vocabulary loading

### `load-vocab-source`

Loads Athena vocabulary CSV files from a configured `athena_source` directory using the
existing ORM staged CSV loader (`load_csv(...)` on the vocabulary models).

Examples:

```bash
omop-maint load-vocab-source
omop-maint load-vocab-source --athena-source ./athena_source
omop-maint load-vocab-source --dry-run
```

Notes:

- the command expects an unzipped Athena vocabulary directory containing CSV files such
  as `CONCEPT.csv`, `VOCABULARY.csv`, and `RELATIONSHIP.csv`
- it creates missing vocabulary tables automatically before loading
- Athena vocabulary imports are treated as tab-delimited input for both the staged
  PostgreSQL COPY path and the fallback CSV loader path
- optional files such as `DRUG_STRENGTH.csv` and `SOURCE_TO_CONCEPT_MAP.csv` are skipped
  when absent
- the default merge strategy is non-destructive `upsert`, which inserts new primary keys
  without overwriting existing vocabulary rows
- on PostgreSQL, concept sequences are reset after load so explicit Athena concept IDs
  do not leave the sequence behind

Important gotchas:

- `athena_source` must be configured in `.omop-maint.toml` or passed explicitly
- `--db-schema` is only supported for this command on PostgreSQL, where the loader uses
  `search_path` to target the selected schema
- use `--merge-strategy replace` only when you explicitly want incoming Athena rows to
  replace existing rows with matching primary keys
- on large vocabulary loads, the staged CSV loader can still be expensive and may take
  significant time even though it uses staging tables

---

## Sequence management

### `truncate-tables`

Truncates selected PostgreSQL ORM-managed tables.

Examples:

```bash
omop-maint truncate-tables --scope clinical --dry-run
omop-maint truncate-tables --scope clinical --restart-identities --yes
omop-maint truncate-tables --table person --table visit_occurrence --dry-run
```

Notes:

- you must choose either `--scope` or one or more `--table` values
- the command is intentionally destructive only with explicit confirmation
- use `--dry-run` first to confirm table selection and row counts

Important gotchas:

- `truncate-tables` is PostgreSQL-only
- a non-dry-run execution requires `--yes`
- scope-limited truncation can still be blocked by foreign key references from tables
  outside the current selection; in that case use `--cascade`, widen the selection,
  or disable FK trigger enforcement first
- `--cascade` can affect more tables than the explicit selection if PostgreSQL needs
  to include dependent relations
- if your workflow loads explicit IDs, pair truncation with `reset-sequences` after the load

### `reset-sequences`

Resets PostgreSQL-owned sequences for OMOP tables with a single integer primary key.

This is intended for ETL workflows where tables have been truncated and need their
sequences moved back to `MAX(pk) + 1`.

Examples:

```bash
omop-maint reset-sequences --dry-run
omop-maint reset-sequences
omop-maint reset-sequences --vocabulary-included
```

Notes:

- vocabulary tables are excluded by default
- tables without an owned PostgreSQL sequence are skipped safely
- this command is PostgreSQL-only

Important gotcha:

- if your ETL loads explicit primary key values, the safest time to run
  `reset-sequences` is usually after the load, not before it
- running it before a load on empty tables only sets sequences back to `1`; it does
  not advance them to the IDs imported later by the ETL

---

## Foreign key trigger management

### `foreign-keys`

Manages PostgreSQL internal RI trigger enforcement for OMOP tables participating in
foreign key relationships.

Examples:

```bash
omop-maint foreign-keys disable --dry-run
omop-maint foreign-keys disable
omop-maint foreign-keys enable
omop-maint foreign-keys validate
omop-maint foreign-keys status
```

Important note:

These commands do **not** remove foreign key definitions from the schema. They toggle
the PostgreSQL internal triggers that enforce those constraints. This makes them useful
for controlled bulk loads, but they are PostgreSQL-only and should be used carefully.

Use strict validation when enabling if you want all relationships checked before
triggers are restored:

If any violations are found, no triggers are re-enabled.

Example:

```bash
omop-maint foreign-keys enable --strict
omop-maint foreign-keys enable --strict --dry-run
```

Validate relationships without enabling anything:

```bash
omop-maint foreign-keys validate
omop-maint foreign-keys validate --vocabulary-included
```

Important gotchas:

- `foreign-keys disable` / `enable` operate via PostgreSQL trigger management, not by
  dropping or recreating foreign key definitions
- these commands may require elevated PostgreSQL privileges depending on how the
  database is owned and administered, because they operate on internal RI trigger
  enforcement rather than ORM metadata alone
- `foreign-keys validate` and `foreign-keys enable --strict` can be expensive on large
  datasets because they run validation queries across every selected FK relationship

---

## Statistics maintenance

### `analyze-tables`

Refreshes planner statistics for selected ORM-managed tables.

Examples:

```bash
omop-maint analyze-tables
omop-maint analyze-tables --scope clinical
omop-maint analyze-tables --table person --table visit_occurrence
omop-maint analyze-tables --scope clinical --vacuum
```

Notes:

- if no selection is provided, the command defaults to all ORM-managed tables
- on PostgreSQL, `--vacuum` runs `VACUUM ANALYZE`
- on SQLite, plain `ANALYZE` is supported but `--vacuum` is not

Important gotchas:

- `VACUUM ANALYZE` is PostgreSQL-only
- on large tables, this command can still take a meaningful amount of time and hold
  maintenance-related locks

## Index management

### `indexes`

Manages ORM-defined secondary indexes.

- `indexes disable` drops metadata-defined secondary indexes that currently exist
- `indexes enable` recreates missing metadata-defined secondary indexes

On PostgreSQL, `indexes enable` also applies ORM-defined clustering metadata.

Examples:

```bash
omop-maint indexes disable --dry-run
omop-maint indexes disable
omop-maint indexes enable
omop-maint indexes enable --vocabulary-included
```

Notes:

- primary keys and constraints are not removed
- only indexes defined in ORM metadata are managed
- PostgreSQL clustering metadata is applied on `indexes enable`

Important gotchas:

- `indexes disable` will not touch database-only indexes that are not represented in
  ORM metadata
- `indexes enable` on PostgreSQL may also run `CLUSTER`, which is significantly heavier
  than plain index creation and can rewrite and lock tables
- on non-PostgreSQL backends, index creation/drop still works, but clustering metadata
  is skipped

---

## Backup and restore

### `backup-database`

Creates a restore-ready PostgreSQL backup artifact using local PostgreSQL client tools.

Examples:

```bash
omop-maint backup-database --engine-schema cdm
omop-maint backup-database --engine-schema cdm --output-path ./cdm-backup.dump
omop-maint backup-database --engine-schema cdm --format plain --output-path ./cdm-backup.sql
```

### `restore-database`

Restores a PostgreSQL backup artifact into the configured target database.

Examples:

```bash
omop-maint restore-database ./cdm-backup.dump --engine-schema cdm
omop-maint restore-database ./cdm-backup.sql --engine-schema cdm
omop-maint restore-database ./cdm-backup.dump --engine-schema cdm --dry-run
```

Important notes:

- these commands run on the machine where you execute `omop-maint`, not on the database server
- the backup artifact is written to and read from the local filesystem of that machine
- local PostgreSQL client tools must be installed:
  - `pg_dump` for backups
  - `pg_restore` for custom dump restores
  - `psql` for plain SQL restores
- the machine running the CLI must be able to reach the database over the network
- connection parameters from the configured SQLAlchemy URL are passed through to the PostgreSQL client tools, including query-string options such as `sslmode`
- passwords are provided via `PGPASSWORD`, not embedded into the generated command line
- `restore-database` restores into the configured target database; it does not create,
  drop, or clean that database for you first
- `--db-schema` filtering during restore is only meaningful for custom-format dumps
  restored via `pg_restore`; plain SQL restores are replayed as-is

This makes the commands suitable for use from:

- a developer laptop
- a bastion or admin host
- a CI runner
- any remote machine with the required PostgreSQL client tools and network access to the target database

Avoid:

- restoring into a populated target database unless you explicitly want the SQL in the
  dump to merge with or overwrite what is already there
- assuming a restore will provision a fresh database automatically

---

## Things To Avoid

These are the main sharp edges worth keeping in mind in the current alpha state:

- do not treat `create-missing-tables` as a guaranteed full bootstrap mechanism for a
  brand new PostgreSQL database until dependency-aware create ordering is added
- do not assume `data-summary`, `foreign-keys validate`, or `foreign-keys enable --strict`
  are lightweight on large datasets
- do not run `truncate-tables` without a `--dry-run` pass first unless you are already
  confident in the exact selection
- do not assume `indexes enable` only creates indexes; on PostgreSQL it may also apply
  clustering
- do not assume `restore-database` prepares an empty target for you
- do not assume saved overrides are global; by default they are rooted to the working
  directory where you run the CLI
- do not treat `reconcile-schema` drift as automatically wrong when the environment
  intentionally contains DBA-managed objects outside ORM metadata

---

## Typical workflows

### Prepare for a bulk clinical reload

```bash
omop-maint doctor
omop-maint foreign-keys disable
omop-maint indexes disable
omop-maint truncate-tables --scope clinical --restart-identities --yes
```

Then run the ETL. If the ETL loads explicit primary key values, reset sequences after
the load and before handing the database back to normal application writes:

```bash
omop-maint reset-sequences
omop-maint indexes enable
omop-maint foreign-keys validate
omop-maint foreign-keys enable --strict
omop-maint analyze-tables --scope clinical
omop-maint doctor --deep
```

### Inspect a partially built schema

```bash
omop-maint data-summary --include-missing
omop-maint create-missing-tables --dry-run
```

### Move a database into another environment

From a machine with PostgreSQL client tools and network access to both environments:

```bash
omop-maint backup-database --engine-schema source --output-path ./cdm.dump
omop-maint restore-database ./cdm.dump --engine-schema target
```

### Bootstrap a schema from ORM metadata

```bash
omop-maint create-missing-tables
omop-maint indexes enable
```

For a brand new PostgreSQL database, treat this as an alpha workflow rather than a
guaranteed one-shot bootstrap path. If dependency ordering matters in your target
environment, verify the result with `omop-maint reconcile-schema`.

---

## Help

Top-level help:

```bash
omop-maint --help
```

Command-specific help:

```bash
omop-maint data-summary --help
omop-maint reset-sequences --help
omop-maint foreign-keys --help
omop-maint indexes --help
omop-maint config --help
```
