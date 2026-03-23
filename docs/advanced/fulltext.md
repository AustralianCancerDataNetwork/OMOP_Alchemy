# PostgreSQL Full-Text Search

OMOP Alchemy includes an **optional** PostgreSQL full-text search integration for
selected vocabulary text fields.

This feature is deliberately bolt-on:

- the library works without it
- the ORM models do not require the extra database columns
- the query helpers can fall back to inline `to_tsvector(...)` expressions when the
  sidecar columns are not installed

That makes it useful when you want faster repeated text-search workloads on PostgreSQL,
without forcing every environment to carry extra full-text infrastructure.

---

## What It Covers

The current full-text support targets:

- `concept.concept_name`
- `concept_synonym.concept_synonym_name`

The handler module lives under:

```python
from omop_alchemy.cdm.handlers import (
    concept_name_tsvector_expression,
    concept_synonym_name_tsvector_expression,
)
```

These helpers return the best available expression for the configured environment:

- if the optional sidecar `tsvector` columns are registered in metadata, they return the
  stored column
- otherwise they fall back to an inline computed PostgreSQL expression using
  `to_tsvector(...)`

---

## Why It Is Optional

Full-text search is useful, but it also introduces operational tradeoffs:

- extra columns in the database
- optional GIN indexes
- explicit backfill / refresh work
- PostgreSQL-specific behavior

Many users only need occasional text matching and are perfectly fine with inline search
expressions. Others want fast repeated full-text lookups across large vocabularies and
are happy to manage the extra schema objects.

OMOP Alchemy therefore treats full-text sidecars as an **optional PostgreSQL
enhancement**, not as part of the core required OMOP schema.

---

## Two Modes

### 1. Inline Expression Mode

This requires no schema changes.

```python
import sqlalchemy as sa
from sqlalchemy.orm import Session

from omop_alchemy.cdm.handlers import concept_name_tsvector_expression
from omop_alchemy.cdm.model.vocabulary import Concept

query = sa.func.plainto_tsquery("english", "edoxaban")

with Session(engine) as session:
    rows = (
        session.query(Concept)
        .filter(concept_name_tsvector_expression() == query)
        .all()
    )
```

In practice you will often want the PostgreSQL full-text match operator rather than
equality:

```python
vector = concept_name_tsvector_expression()
query = sa.func.plainto_tsquery("english", "edoxaban")

stmt = sa.select(Concept).where(vector.op("@@")(query))
```

This mode is simple and portable at the library level, but PostgreSQL must compute the
vector expression at query time unless the planner can otherwise optimize it.

### 2. Stored Sidecar Mode

This mode adds real `tsvector` columns to the database and optionally GIN indexes.

Once installed and registered, the helper functions point at the stored columns instead
of recomputing vectors inline.

This is the mode you want when:

- you run frequent vocabulary search queries
- you care about PostgreSQL full-text query performance
- you are comfortable managing the sidecar lifecycle

---

## Lifecycle

The maintenance CLI manages the full-text sidecars through:

```bash
omop-maint fulltext install
omop-maint fulltext populate
omop-maint fulltext drop
```

Typical workflow:

```bash
omop-maint fulltext install
omop-maint fulltext populate
```

If you later reload or update vocabulary data, refresh the stored vectors with:

```bash
omop-maint fulltext populate
```

If you want to remove the feature completely:

```bash
omop-maint fulltext drop
```

---

## Important Behavior

The current implementation uses **ordinary nullable sidecar `tsvector` columns**, not
generated columns and not trigger-managed columns.

That means:

- `install` creates the columns and optional GIN indexes
- `populate` backfills or refreshes the values
- future data changes are **not** reflected automatically until you repopulate

This is a deliberate choice because it keeps the feature explicit and easier to manage
alongside bulk vocabulary loads.

---

## Querying Pattern

A typical PostgreSQL query looks like this:

```python
import sqlalchemy as sa
from sqlalchemy.orm import Session

from omop_alchemy.cdm.handlers import concept_name_tsvector_expression
from omop_alchemy.cdm.model.vocabulary import Concept

vector = concept_name_tsvector_expression()
query = sa.func.plainto_tsquery("english", "direct oral anticoagulant")

stmt = (
    sa.select(
        Concept.concept_id,
        Concept.concept_name,
        sa.func.ts_rank_cd(vector, query).label("rank"),
    )
    .where(vector.op("@@")(query))
    .order_by(sa.text("rank DESC"))
    .limit(20)
)

with Session(engine) as session:
    rows = session.execute(stmt).all()
```

The same idea applies to `concept_synonym_name_tsvector_expression()`.

---

## Metadata Registration

If your process will use the stored sidecar columns directly, register them into the ORM
metadata:

```python
from omop_alchemy.cdm.handlers import register_optional_fulltext_columns

register_optional_fulltext_columns()
```

If you later remove the columns from the database in the same process and want query
helpers to fall back cleanly again:

```python
from omop_alchemy.cdm.handlers import unregister_optional_fulltext_columns

unregister_optional_fulltext_columns()
```

This only affects SQLAlchemy metadata in the current Python process. It does not alter
the database by itself.

---

## PostgreSQL Scope

This feature is PostgreSQL-specific in its database form because it relies on:

- `tsvector`
- PostgreSQL full-text query functions such as `to_tsvector` and `plainto_tsquery`
- optional GIN indexes

The helper expressions can still be imported safely, but the sidecar install / populate /
drop lifecycle is only meaningful on PostgreSQL.

---

## Operational Gotchas

- treat the sidecar columns as **derived search state**, not source-of-truth data
- if you bulk-load new vocabulary rows, rerun `omop-maint fulltext populate`
- if you use `reconcile-schema`, the sidecar columns and indexes are intentional
  database additions outside the core OMOP schema
- GIN indexes can be expensive to build on large vocabularies, so plan that as a real
  maintenance operation rather than a trivial toggle

---

## When To Use It

Use the optional full-text feature when:

- you are on PostgreSQL
- you run repeated vocabulary-name or synonym-name search workloads
- inline `to_tsvector(...)` search is becoming a bottleneck

Skip it when:

- you only do occasional text search
- you want to keep the database as close as possible to the base OMOP schema
- you do not want to own the refresh cycle for derived search columns
