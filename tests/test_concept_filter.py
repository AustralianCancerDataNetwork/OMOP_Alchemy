"""Tests for ConceptFilter.apply(), the shared CDM concept-table WHERE/LIMIT builder."""

import pytest
import sqlalchemy as sa

from omop_alchemy.cdm.model.vocabulary import Concept
from omop_alchemy.cdm.query import ConceptFilter


class TestConceptFilterApply:
    def test_empty_filter_adds_no_clauses(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter().apply(query)

        assert str(result) == str(query)

    def test_concept_ids_adds_in_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(concept_ids=(1, 2, 3)).apply(query)

        compiled = str(result)
        assert "WHERE" in compiled
        assert "concept_id IN" in compiled

    def test_domains_adds_in_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(domains=("Condition", "Drug")).apply(query)

        assert "domain_id IN" in str(result)

    def test_vocabularies_adds_in_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(vocabularies=("SNOMED",)).apply(query)

        assert "vocabulary_id IN" in str(result)

    def test_require_standard_adds_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_standard=True).apply(query)

        assert "standard_concept IN" in str(result)

    def test_require_active_adds_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_active=True).apply(query)

        assert "invalid_reason NOT IN" in str(result)

    def test_limit_is_applied(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(limit=5).apply(query)

        assert "LIMIT" in str(result)

    def test_negative_limit_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            ConceptFilter(limit=0)

    def test_is_empty(self):
        assert ConceptFilter().is_empty()
        assert not ConceptFilter(limit=5).is_empty()
        assert not ConceptFilter(domains=("Drug",)).is_empty()
        assert not ConceptFilter(require_standard=True).is_empty()
        assert not ConceptFilter(require_active=True).is_empty()
