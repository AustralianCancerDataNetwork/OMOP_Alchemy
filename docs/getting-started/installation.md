# Basic setup

## Connecting directly via SQLAlchemy

OMOP_Alchemy does not require any special database wrapper. 

You can always connect to your database using plain SQLAlchemy and work with engines, connections, and sessions exactly as you would in any other project.

```python
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.cdm.model.vocabulary import Concept

engine = sa.create_engine(
    "postgresql+psycopg://user:password@localhost:5432/omop",
    future=True,
    echo=False,
)

with so.Session(engine) as sess:
    concepts = (
        session.query(Concept)
        .filter(Concept.domain_id == "Drug")
        .limit(10)
        .all()
    )
```


```python
with so.Session(engine) as session:
    session.add(obj)
    session.commit()
```

## Connecting with OMOP_Alchemy-specific helpers

### Environment-based config

```python
def load_environment(dotenv: str = '') -> None:
```

Loads environment variables from a .env file into the process environment.

* If a specific .env path is provided, it is loaded first
* Otherwise, a default .env file is searched for

```python
load_environment()

load_environment("/etc/myapp/.env")
```

### Database engine resolution


```python
def get_engine_name(schema: str | None = None) -> str:
```
Resolves a SQLAlchemy database engine URI from environment variables.

If a `schema` is provided, resolution proceeds as follows:

1. `ENGINE_<SCHEMA>`
2. `ENGINE` as fallback (if only one)

Single DB .env example:

```
ENGINE=postgresql+psycopg://user:password@localhost:5432/omop

engine_url = get_engine_name()
```

Multi-schema routing

```
ENGINE_CDM=postgresql+psycopg://user:password@localhost:5432/cdm
ENGINE_SOURCE=postgresql+psycopg://user:password@localhost:5432/source
ENGINE=postgresql+psycopg://user:password@localhost:5432/default

cdm_engine = get_engine_name("cdm")
source_engine = get_engine_name("source")
default_engine = get_engine_name()
```

### Recommended patterns

```python
from orm_loader.helpers import configure_logging, bootstrap
from omop_alchemy import get_engine_name, load_environment
import sqlalchemy as sa

configure_logging()
load_environment()

engine_string = get_engine_name('cdm')
engine = sa.create_engine(engine_string, future=True, echo=False)

bootstrap(engine, create=True)
```

### Session & Engine Management for Bulk Operations

ORM-loader module provides context managers for safely relaxing database constraints during high-volume operations such as CSV loads, staging-table merges, and backfills.

These utilities temporarily change database behaviour and guarantee restoration even on failure.

`bulk_load_context` temporarily adjusts session-level behaviour to make bulk inserts faster:

* Optionally disables foreign key enforcement
* Optionally disables SQLAlchemy autoflush
* Ensures all settings are restored on exit
* Rolls back the session if an exception occurs

This context manager is session-scoped and safe to use alongside ORM loaders, and is supported for sqlite and postgres backends.

```python
@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
):
```

Usage:

```python
from sqlalchemy.orm import Session
from orm_loader.helpers import bulk_load_context

with Session(engine) as session:
    with bulk_load_context(session):
        MyTable.load_csv(
            session,
            path="MY_TABLE.csv",
            dedupe=True,
            merge_strategy="upsert",
        )

    session.commit()
```

`engine_with_replica_role` enforces replica mode at the engine level, meaning:

* All new connections opened during the context run with `session_replication_role = replica`
* The role is restored to DEFAULT afterward

This is supported for postgres only.

Use engine_with_replica_role when:

* Creating / refreshing materialized views
* Running schema-level operations that might trigger independent sessions
* Using tooling that opens its own connections

## Optional PostgreSQL full-text search

OMOP Alchemy can optionally integrate with PostgreSQL full-text search for selected
vocabulary text fields.

This feature is **not required** to use the library and is intentionally treated as an
optional enhancement rather than part of the core OMOP schema.

At the library level:

- query helpers can fall back to inline `to_tsvector(...)` expressions
- optional sidecar `tsvector` columns can be registered into SQLAlchemy metadata when
  they exist in the database

At the database level:

- PostgreSQL-only sidecar columns and optional GIN indexes can be managed through the
  maintenance CLI
- those sidecar columns are populated explicitly rather than auto-generated

Typical maintenance workflow:

```bash
omop-maint fulltext install
omop-maint fulltext populate
```

If you later reload vocabulary data, rerun:

```bash
omop-maint fulltext populate
```

For the full design and query patterns, see:

- [PostgreSQL Full-Text Search](../advanced/fulltext.md)
- [Maintenance CLI](maintenance.md)
