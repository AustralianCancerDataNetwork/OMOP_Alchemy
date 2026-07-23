"""Shared CDM concept-table query filtering.

Consolidates filtering logic previously duplicated across downstream packages
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import sqlalchemy as sa

from omop_alchemy.cdm.model.vocabulary import (
    Concept,
    StandardConceptFlag,
    normalised_flag_expr,
)


@dataclass(frozen=True)
class ConceptFilter:
    """Search constraints for plain CDM ``concept``-table queries.

    All fields are optional. Unset fields impose no constraint.

    Attributes
    ----------
    concept_ids : tuple[int, ...], optional
        Restrict results to this set of concept IDs.
    domains : tuple[str, ...], optional
        Restrict results to concepts in these OMOP domains.
    vocabularies : tuple[str, ...], optional
        Restrict results to concepts from these vocabularies.
    require_standard : bool
        When ``True``, only standard concepts (``standard_concept`` in
        ``('S', 'C')``) are returned. Default ``False``. Blank or
        whitespace-only ``standard_concept`` values are treated as unset (not
        standard). Any other non-canonical value (e.g. corrupt data) is also
        treated as not matching, since it is neither ``'S'`` nor ``'C'``.
    require_active : bool
        When ``True``, only active concepts (``invalid_reason`` is ``NULL``,
        i.e. not ``'D'`` or ``'U'``) are returned. Default ``False``. Blank or
        whitespace-only ``invalid_reason`` values are treated as unset (i.e.
        active). Any other non-canonical value (e.g. corrupt data) is treated
        as not matching (i.e. not active), since only a ``NULL``/blank value
        is recognised as active.
    limit : int, optional
        Maximum number of rows to return. If not set, all matching rows are
        returned.
    """

    concept_ids: Optional[tuple[int, ...]] = None
    domains: Optional[tuple[str, ...]] = None
    vocabularies: Optional[tuple[str, ...]] = None
    require_standard: bool = False
    require_active: bool = False
    limit: Optional[int] = None

    def __post_init__(self) -> None:
        if self.limit is not None and self.limit <= 0:
            raise ValueError(
                f"ConceptFilter.limit must be a positive integer, got {self.limit}."
            )

    def apply(self, query: sa.Select) -> sa.Select:
        """Apply filter constraints to a Select already targeting Concept."""
        if self.concept_ids is not None:
            query = query.where(Concept.concept_id.in_(self.concept_ids))

        if self.domains is not None:
            query = query.where(Concept.domain_id.in_(self.domains))

        if self.vocabularies is not None:
            query = query.where(Concept.vocabulary_id.in_(self.vocabularies))

        if self.require_standard:
            query = query.where(
                normalised_flag_expr(Concept.standard_concept).in_(
                    [flag.value for flag in StandardConceptFlag]
                )
            )

        if self.require_active:
            query = query.where(normalised_flag_expr(Concept.invalid_reason).is_(None))

        if self.limit is not None:
            query = query.limit(self.limit)

        return query

    def is_empty(self) -> bool:
        """Return ``True`` if no constraints are set."""
        return (
            self.concept_ids is None
            and self.domains is None
            and self.vocabularies is None
            and not self.require_standard
            and not self.require_active
            and self.limit is None
        )
