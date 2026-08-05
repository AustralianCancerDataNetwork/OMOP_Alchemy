from __future__ import annotations

from collections.abc import Callable, Iterable

import sqlalchemy.orm as so
from sqlalchemy import select

from omop_alchemy.cdm.handlers._semantics import default_semantics_runtime
from omop_alchemy.cdm.model import Concept_Ancestor


def _runtime_group(value_set_name: str, semantic_unit_name: str, group_name: str):
    runtime = default_semantics_runtime()
    value_set = getattr(runtime, value_set_name)
    semantic_unit = getattr(value_set, semantic_unit_name)
    return semantic_unit.groups[group_name]


def rt_procedure_parent_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "cancer_procedures",
        "cancer_procedure_types",
        "cancer_procedure_types",
    )
    mapper = group.mapper()
    return (
        mapper["rt_externalbeam"],
        mapper["rt_procedure"],
        mapper["rt_brachytherapy"],
    )


def cancer_indicating_surgery_parent_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "cancer_procedures",
        "cancer_indicating_surgery_parent_concepts",
        "cancer_indicating_surgery_parent_concepts",
    )
    return tuple(group.mapper().values())


def cancer_indicating_surgery_point_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "cancer_procedures",
        "cancer_indicating_surgery_point_concepts",
        "cancer_indicating_surgery_point_concepts",
    )
    return tuple(group.mapper().values())


def diagnostic_staging_procedure_parent_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "cancer_procedures",
        "diagnostic_staging_procedure_parent_concepts",
        "diagnostic_staging_procedure_parent_concepts",
    )
    return tuple(group.mapper().values())


def diagnostic_staging_procedure_point_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "cancer_procedures",
        "diagnostic_staging_procedure_point_concepts",
        "diagnostic_staging_procedure_point_concepts",
    )
    return tuple(group.mapper().values())


def sact_drug_parent_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "sact",
        "sact_drug_classification",
        "sact_drug_classification",
    )
    return tuple(group.ids)


def sact_drug_excluded_parent_concept_ids() -> tuple[int, ...]:
    group = _runtime_group(
        "sact",
        "sact_drug_classification",
        "sact_drug_classification",
    )
    return tuple(group.excluded_ids)


def disease_episode_type_concept_ids() -> tuple[int, ...]:
    return tuple(default_semantics_runtime().types.disease_episode_types.ids)


def overarching_episode_type_concept_id() -> int:
    return default_semantics_runtime().types.disease_episode_types.episode_of_care


def treatment_episode_type_concept_ids() -> tuple[int, ...]:
    return tuple(default_semantics_runtime().types.treatment_episode_types.ids)


def treatment_regimen_episode_concept_id() -> int:
    return default_semantics_runtime().types.treatment_episode_types.treatment_regimen


def treatment_cycle_episode_concept_id() -> int:
    return default_semantics_runtime().types.treatment_episode_types.treatment_cycle


def _descendant_concept_ids(
    session: so.Session,
    ancestor_concept_ids: Iterable[int],
) -> frozenset[int]:
    stmt = select(Concept_Ancestor.descendant_concept_id).where(
        Concept_Ancestor.ancestor_concept_id.in_(tuple(ancestor_concept_ids))
    )
    return frozenset(session.execute(stmt).scalars().all())


def procedure_concept_membership_expression(
    concept_id_column,
    parent_ids: Iterable[int],
    point_ids: Iterable[int] = (),
):
    expr = concept_id_column.in_(
        select(Concept_Ancestor.descendant_concept_id).where(
            Concept_Ancestor.ancestor_concept_id.in_(tuple(parent_ids))
        )
    )
    point_ids = tuple(point_ids)
    if point_ids:
        expr = expr | concept_id_column.in_(point_ids)
    return expr


def drug_concept_membership_expression(
    concept_id_column,
    include_parent_ids: Iterable[int],
    exclude_parent_ids: Iterable[int] = (),
):
    expr = concept_id_column.in_(
        select(Concept_Ancestor.descendant_concept_id).where(
            Concept_Ancestor.ancestor_concept_id.in_(tuple(include_parent_ids))
        )
    )
    exclude_parent_ids = tuple(exclude_parent_ids)
    if exclude_parent_ids:
        expr = expr & concept_id_column.not_in(
            select(Concept_Ancestor.descendant_concept_id).where(
                Concept_Ancestor.ancestor_concept_id.in_(exclude_parent_ids)
            )
        )
    return expr


class _ConceptSetCache:
    def __init__(self) -> None:
        self._cache: dict[tuple[int, str], frozenset[int]] = {}

    def get(
        self,
        name: str,
        session: so.Session,
        builder: Callable[[so.Session], frozenset[int]],
    ) -> frozenset[int]:
        bind = session.get_bind()
        key = (id(bind), name)
        if key not in self._cache:
            self._cache[key] = builder(session)
        return self._cache[key]

    def clear(self) -> None:
        self._cache.clear()


_cache = _ConceptSetCache()


def resolve_rt_procedure_concept_ids(session: so.Session) -> frozenset[int]:
    return _cache.get(
        "rt_procedure_concept_ids",
        session,
        lambda s: _descendant_concept_ids(s, rt_procedure_parent_concept_ids()),
    )


def resolve_cancer_indicating_surgery_procedure_concept_ids(
    session: so.Session,
) -> frozenset[int]:
    def _build(s: so.Session) -> frozenset[int]:
        closure = _descendant_concept_ids(
            s,
            cancer_indicating_surgery_parent_concept_ids(),
        )
        return closure | frozenset(cancer_indicating_surgery_point_concept_ids())

    return _cache.get("cancer_indicating_surgery_procedure_concept_ids", session, _build)


def resolve_diagnostic_staging_procedure_concept_ids(
    session: so.Session,
) -> frozenset[int]:
    def _build(s: so.Session) -> frozenset[int]:
        closure = _descendant_concept_ids(
            s,
            diagnostic_staging_procedure_parent_concept_ids(),
        )
        return closure | frozenset(diagnostic_staging_procedure_point_concept_ids())

    return _cache.get("diagnostic_staging_procedure_concept_ids", session, _build)


def resolve_sact_drug_concept_ids(session: so.Session) -> frozenset[int]:
    def _build(s: so.Session) -> frozenset[int]:
        included = _descendant_concept_ids(s, sact_drug_parent_concept_ids())
        excluded = _descendant_concept_ids(s, sact_drug_excluded_parent_concept_ids())
        return included - excluded

    return _cache.get("sact_drug_concept_ids", session, _build)


def clear_concept_set_cache() -> None:
    _cache.clear()
