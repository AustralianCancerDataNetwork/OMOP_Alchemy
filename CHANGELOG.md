## 0.2.0
- Initial public release
- SQLAlchemy 2.0 typed OMOP CDM models
- Domain validation helpers
- Episode + event scaffolding

## 0.2.1
- minor modification to support chunked csv dedupe queries for larger load files 

## 0.2.2
- changed load_environment function to accept a string parameter for path to a .env file holding parameters for ENGINE and SOURCE_PATH to accommodate easier downstream usage

## 0.2.3
- added bulk_load_context for trusted bulk loads (e.g. Athena vocabulary)
- Disables FK enforcement where supported
- Suppresses autoflush during bulk load operations

## 0.2.4
- fstrings in logging throughout
- changed type handling for string columns that may contain numeric data to prevent errors during load

## 0.2.5
- optional commit on chunk load to reduce transaction size for large files

## 0.2.6
- extra episode demo and modify joined load strategy for conceptview objects

## 0.5.0
- significant refactor
- extract generic ORM loading, validation, and metadata into base library (orm-loader)
- elements now reusable in other downstream data models

## 0.5.1
- change get_engine_name to take optional parameter

## 0.5.2
- mostly just upversioning orm_loader base
- added table_has_rows convenience function

## 0.5.3
- upversioning orm_loader dependency to handle date parse updates for non-onco-branch vocab files

## 0.5.4
- modification of PK for concept_synonym table

## 0.5.5
- created some tests
- upversion orm_loader
- episode event convenience function

## 0.5.6
- added ConceptValidationMixin for both view and table mapped classes

## 0.5.7
- nullability in concept table

## 0.5.8
- moved complex mappers out of model module to stand alone

## 0.5.9
- refactoring vocabulary mapping lookups
- added typers for cdm type hinting

## 0.5.10
- added concept resolution registry

## 0.5.11
- updated concept resolution builder pattern

## 0.5.12
- created visit_occurrence view object to handle 'visits where a provider of specialty [x] was seen' (whether through visit provider, procedure provider, obs provider relationships)

## 0.6.0
- added PostgreSQL sequence reset utility for OMOP tables
- default reset scope excludes vocabulary-backed tables unless explicitly requested
- centralised maintenance table categories and reusable table selection helpers for future admin tasks
- added optional postgres dependency group and clearer missing-driver error for postgres engine creation
- added data-summary maintenance command for live OMOP table presence and row-count reporting
- added postgres foreign-key trigger management commands for bulk-load workflows
- added create-missing-tables maintenance command for schema bootstrap from ORM metadata
- added metadata-driven index disable/enable maintenance commands for bulk-load workflows
- optional tsvector handling
- upversion `orm-loader` dependency floor to `0.3.23` for generated-column-safe staged merges
- keep Athena vocabulary loading on tab-delimited input and improve load failure reporting
- refine PostgreSQL full-text sidecar support under `cdm.handlers.fulltext`
- add `omop-maint fulltext install`, `populate`, and `drop` commands
- document manual refresh expectations for sidecar `tsvector` columns

## 0.6.1
- upversion orm-loader
- set minimum versions per dependabot (dev and required deps)

## 0.6.2
- capped maximum `orm-loader` version to avoid pulling in future breaking changes

## 0.6.3
- fix CSV quote mode for Athena vocabulary loading: switch from `literal` to `auto` to prevent quoted concept names from overflowing `VARCHAR(255)` database columns
- make `chunksize=100_000` the default for `load-vocab-source` (was `None`/disabled); pass `--chunksize 0` to disable chunking explicitly
- **breaking:** `load-vocab-source` CLI now defaults `--merge-strategy` to `replace` (was `upsert`) to match the Python API default and ensure retired concepts are purged on vocabulary refresh; pass `--merge-strategy upsert` to restore the previous behaviour
- **breaking:** CLI entry point renamed from `omop-maint` to `omop-alchemy`; update any scripts or aliases accordingly (saved `.omop-maint.toml` defaults files are unaffected)
- remove stale notebooks from repository

## 0.7.0
- major CLI overhaul: new `backends/` package (`Backend` ABC, `PostgresBackend`, `SQLiteBackend`) centralises all dialect-specific SQL, replacing logic scattered across CLI files
- new `@omop_command` decorator removes repeated connection/error-handling boilerplate from all CLI commands
- split the monolithic `cli_schema.py` into focused modules (`cli_schema_info`, `cli_schema_doctor`, `cli_schema_reconcile`, `cli_schema_tables`, `cli_schema_summary`); old import path kept as a re-export shim
- **breaking:** DB/engine configuration now goes entirely through `oa-configurator`'s config file; CLI commands no longer accept `--dotenv` or env-var based connection config
- accelerated vocabulary ingestion: pooled/reloaded connections, FK-trigger and index management around bulk loads, schema-qualified staging tables
- added PostgreSQL full-text sidecar (`tsvector`) install/populate/drop commands
- expanded CLI and API documentation: architecture overview and full command reference
