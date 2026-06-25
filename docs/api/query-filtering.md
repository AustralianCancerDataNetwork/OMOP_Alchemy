# Query Filtering Framework

## Overview

A composable, reusable filtering system for common OMOP query patterns. This framework provides type-safe, extensible filters that can be applied to SQLAlchemy Select statements, enabling consistent query construction across different use cases.

## Implementation

### New Module Structure
```
omop_alchemy/cdm/query/
├── __init__.py           # Public exports
└── filters.py            # BaseConceptFilter, ConceptFilter
```

### Key Components

#### `BaseConceptFilter` (Abstract Base)
- Protocol for all future concept-based filters
- Forces subclasses to implement `apply(query: Select) -> Select`
- Allows for extensibility

#### `ConceptFilter`
- Frozen dataclass for immutability
- Supports filtering by: concept_ids, domains, vocabularies, require_standard

### Export Paths
```python
# From query submodule
from omop_alchemy.cdm.query import ConceptFilter, BaseConceptFilter

# From CDM package (convenience)
from omop_alchemy.cdm import ConceptFilter, BaseConceptFilter
```

## Usage

### Basic Filtering

```python
from sqlalchemy import select
from omop_alchemy.cdm.query import ConceptFilter
from omop_alchemy.cdm.model.vocabulary import Concept

# Single filter
filter = ConceptFilter(
    domains=("Condition", "Drug"),
    require_standard=True
)

query = select(Concept)
filtered_query = filter.apply(query)
results = session.execute(filtered_query).all()
```

## Benefits

**Composable**: Chain multiple filters or apply individually  
**Type-Safe**: Frozen dataclasses with optional typing  
**Reusable**: Single implementation across different contexts  
**Extensible**: `BaseConceptFilter` protocol enables new filter types  

## Extensibility

The framework is designed to support additional filter types by implementing `BaseConceptFilter`:

```python
from dataclasses import dataclass
from typing import Optional, Tuple
from sqlalchemy.sql import Select
from omop_alchemy.cdm.query.filters import BaseConceptFilter

@dataclass(frozen=True)
class DomainSpecificFilter(BaseConceptFilter):
    """Custom filter for specialized concept querying."""
    custom_constraint: Optional[Tuple[str, ...]] = None
    
    def apply(self, query: Select) -> Select:
        # Implement domain-specific filtering logic
        if self.custom_constraint is not None:
            query = query.where(...)  # Your constraint logic
        return query
```

### Potential Filter Types

While `ConceptFilter` covers the core concept-filtering patterns, the protocol supports domain-specific extensions:

- **Measurement filters** — Unit types, operator constraints (>, <, =)
- **Relationship filters** — Hierarchical traversal, predicate types, depth bounds
- **Temporal filters** — Valid date ranges, versioning constraints
- **Vocabulary-specific filters** — Code patterns, classification hierarchies
- **Domain composition filters** — Multi-table filtering across clinical events

