"""Tests for ConceptFilter.apply(), the shared CDM concept-table WHERE/LIMIT builder."""

import pytest
import sqlalchemy as sa

from omop_alchemy.cdm.model.vocabulary import (
    Concept,
    ConceptView,
    InvalidReasonFlag,
    StandardConceptFlag,
    normalised_flag_expr,
)
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

        compiled = str(result).lower()
        assert "nullif" in compiled
        assert "trim" in compiled
        assert " in " in compiled

    def test_require_active_adds_clause(self):
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_active=True).apply(query)

        compiled = str(result).lower()
        assert "nullif" in compiled
        assert "trim" in compiled
        assert "is null" in compiled

    def test_require_active_does_not_exclude_null_invalid_reason(self, session):
        """Regression test: NULL invalid_reason (the normal, active case) must
        not be dropped by a SQL NOT IN three-valued-logic bug."""
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_active=True).apply(query)

        returned_ids = set(session.scalars(result).all())
        all_ids = set(session.scalars(sa.select(Concept.concept_id)).all())
        assert returned_ids == all_ids
        assert returned_ids  # sanity: fixtures aren't empty

    def test_require_standard_executes_and_matches_fixtures(self, session):
        """Regression test: prove require_standard actually executes and
        matches real 'S' rows, not just that the compiled clause looks right."""
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_standard=True).apply(query)

        returned_ids = set(session.scalars(result).all())
        all_ids = set(session.scalars(sa.select(Concept.concept_id)).all())
        assert returned_ids == all_ids
        assert returned_ids  # sanity: fixtures aren't empty

    def test_require_standard_compiles_with_literal_binds(self):
        """Regression test: StrEnum members passed to .in_() must render as
        plain string literals, not enum reprs, so query.compile(literal_binds=
        True) (used for debug logging) doesn't raise a CompileError."""
        query = sa.select(Concept.concept_id)
        result = ConceptFilter(require_standard=True).apply(query)

        compiled = str(result.compile(compile_kwargs={"literal_binds": True}))
        assert "'S'" in compiled
        assert "'C'" in compiled

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


class TestNormalisedFlagExpr:
    """normalised_flag_expr must trim whitespace and turn blank strings into
    NULL, while leaving NULL and non-blank values (canonical or not) alone."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            (None, None),
            ("", None),
            ("   ", None),
            ("S", "S"),
            (" S ", "S"),
            ("X", "X"),  # non-canonical, non-blank values pass through unchanged
        ],
    )
    def test_normalises_value(self, session, raw, expected):
        result = session.scalar(
            sa.select(normalised_flag_expr(sa.literal(raw, type_=sa.String)))
        )
        assert result == expected


class TestConceptViewFlags:
    """ConceptView.is_standard/is_valid must agree with StandardConceptFlag /
    the OMOP definition of "standard" (S or C), not just a single literal."""

    @pytest.mark.parametrize(
        "standard_concept, expected",
        [
            (StandardConceptFlag.STANDARD, True),
            (StandardConceptFlag.CLASSIFICATION, True),
            (f" {StandardConceptFlag.STANDARD} ", True),
            (None, False),
            ("", False),
            ("   ", False),
        ],
    )
    def test_is_standard(self, standard_concept, expected):
        cv = ConceptView(standard_concept=standard_concept)
        assert cv.is_standard is expected

    @pytest.mark.parametrize(
        "invalid_reason, expected",
        [
            (None, True),
            ("", True),
            ("   ", True),
            (InvalidReasonFlag.DELETED, False),
            (InvalidReasonFlag.UPDATED, False),
        ],
    )
    def test_is_valid(self, invalid_reason, expected):
        cv = ConceptView(invalid_reason=invalid_reason)
        assert cv.is_valid is expected

    @pytest.mark.parametrize(
        "invalid_reason", [None, "", "   ", "D", "U", "X", " X "]
    )
    def test_is_valid_agrees_with_require_active_filter(self, session, invalid_reason):
        """PR feedback regression test: a 'dirty' row must not be judged
        active by ConceptFilter(require_active=True) but invalid by
        ConceptView.is_valid, or vice versa."""
        included_by_filter = session.scalar(
            sa.select(normalised_flag_expr(sa.literal(invalid_reason, type_=sa.String)).is_(None))
        )
        cv = ConceptView(invalid_reason=invalid_reason)
        assert cv.is_valid == included_by_filter

    @pytest.mark.parametrize(
        "standard_concept", [None, "", "   ", "S", " S ", "C", " C ", "X", " X "]
    )
    def test_is_standard_agrees_with_require_standard_filter(self, session, standard_concept):
        """Same consistency check as above, for is_standard/require_standard."""
        # bool(...): a bare SELECT surfaces SQL's three-valued IN() as NULL
        # rather than False, but a WHERE clause treats NULL the same as
        # False (row excluded) - so this coercion mirrors WHERE semantics.
        included_by_filter = bool(
            session.scalar(
                sa.select(
                    normalised_flag_expr(sa.literal(standard_concept, type_=sa.String)).in_(
                        [flag.value for flag in StandardConceptFlag]
                    )
                )
            )
        )
        cv = ConceptView(standard_concept=standard_concept)
        assert cv.is_standard == included_by_filter
