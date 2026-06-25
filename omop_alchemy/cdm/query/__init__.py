"""
Query filtering framework for OMOP Alchemy.

Provides composable, reusable filters for common query patterns across
OMOP concepts and vocabularies. Filters can be combined and applied to
SQLAlchemy Select statements, enabling consistent filtering logic across
different projects (omop-emb, omop-graph, etc.) without circular imports.
"""

from .filters import (
    ConceptFilter,
    BaseConceptFilter,
)


__all__ = [
    "ConceptFilter",
    "BaseConceptFilter",
]
