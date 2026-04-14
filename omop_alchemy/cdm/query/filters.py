"""
Concept filtering for OMOP Concept table queries.

Provides a composable filter that applies constraints to SQLAlchemy queries
targeting the OMOP Concept table. This unified implementation is used by both
omop-emb and omop-graph to avoid code duplication and circular imports.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Tuple

from sqlalchemy.sql import Select

from omop_alchemy.cdm.model.vocabulary import Concept


class BaseConceptFilter(ABC):
    """
    Abstract base for filters that can be applied to Concept queries.
    
    Subclasses implement the ``apply`` method to modify a SQLAlchemy Select
    statement with domain-specific constraints.
    """

    @abstractmethod
    def apply(self, query: Select) -> Select:
        """
        Apply filter constraints to a SQLAlchemy Select statement.

        Parameters
        ----------
        query : Select
            A SQLAlchemy Select statement, typically targeting the Concept table.

        Returns
        -------
        Select
            The modified Select statement with filter constraints appended.
        """
        pass


@dataclass(frozen=True)
class ConceptFilter(BaseConceptFilter):
    """
    Unified filter for OMOP Concept table queries.

    Consolidates filtering logic previously duplicated in omop-emb
    (EmbeddingConceptFilter) and omop-graph (SearchConstraintConcept).
    This filter can be used by any project that needs to constrain
    Concept queries by domain, vocabulary, concept IDs, or standardization status.

    Parameters
    ----------
    concept_ids : tuple[int, ...], optional
        A tuple of OMOP Concept IDs to filter by.
        If None, no concept ID filtering is applied.
    domains : tuple[str, ...], optional
        A tuple of OMOP Domain IDs to filter by (e.g., ('Condition', 'Drug')).
        If None, no domain filtering is applied.
    vocabularies : tuple[str, ...], optional
        A tuple of OMOP Vocabulary IDs to filter by (e.g., ('SNOMED', 'RxNorm')).
        If None, no vocabulary filtering is applied.
    require_standard : bool, optional
        If True, restricts results to standard ('S') or classification ('C') concepts.
        Default is False.

    Examples
    --------
    >>> from omop_alchemy.cdm.query import ConceptFilter
    >>> from sqlalchemy import select
    >>> from omop_alchemy.cdm.model.vocabulary import Concept
    >>>
    >>> # Filter for conditions and drugs in SNOMED and RxNorm
    >>> filter = ConceptFilter(
    ...     domains=("Condition", "Drug"),
    ...     vocabularies=("SNOMED", "RxNorm"),
    ...     require_standard=True
    ... )
    >>>
    >>> query = select(Concept)
    >>> filtered_query = filter.apply(query)

    Notes
    -----
    - All parameters are optional; filters are only applied if set (not None or default).
    - The `require_standard` flag filters for both 'S' (Standard) and 'C' (Classification)
      concepts to allow curated, non-standard-but-approved concepts.
    - Filters are composable with SQLAlchemy's native query building.
    """

    concept_ids: Optional[Tuple[int, ...]] = field(default=None)
    domains: Optional[Tuple[str, ...]] = field(default=None)
    vocabularies: Optional[Tuple[str, ...]] = field(default=None)
    require_standard: bool = False

    def apply(self, query: Select) -> Select:
        """
        Apply the filter constraints to a SQLAlchemy Select statement.

        Parameters
        ----------
        query : Select
            The SQLAlchemy Select statement targeting the Concept table.

        Returns
        -------
        Select
            The modified Select statement with where clauses appended.
        """
        if self.concept_ids is not None:
            query = query.where(Concept.concept_id.in_(self.concept_ids))

        if self.domains is not None:
            query = query.where(Concept.domain_id.in_(self.domains))

        if self.vocabularies is not None:
            query = query.where(Concept.vocabulary_id.in_(self.vocabularies))

        if self.require_standard:
            # Filters for 'S' (Standard) or 'C' (Classification)
            query = query.where(Concept.standard_concept.in_(["S", "C"]))

        return query
