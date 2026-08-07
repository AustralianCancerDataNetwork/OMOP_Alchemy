from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Callable, Hashable, Optional, Sequence

from omop_alchemy.cdm.model import Drug_Exposure


@dataclass(frozen=True)
class DrugExposureSummary:
    """
    Small generic summary of drug exposures grouped by a caller-chosen key.

    The grouping key can be a drug concept, ingredient concept, regimen member,
    or any downstream classifier. This type deliberately does not encode SACT or
    any other clinical programme-specific policy.
    """

    group_key: object
    n_exposures: int
    first_start_date: Optional[date]
    last_start_date: Optional[date]
    total_quantity: Optional[float]
    dose_unit_source_values: frozenset[str]
    drug_concept_ids: frozenset[int]


def summarize_drug_exposures(
    exposures: Sequence[Drug_Exposure],
    *,
    group_key: object,
) -> DrugExposureSummary:
    quantities = [e.quantity for e in exposures if e.quantity is not None]
    dates = [
        e.drug_exposure_start_date
        for e in exposures
        if e.drug_exposure_start_date is not None
    ]
    dose_units = frozenset(
        e.dose_unit_source_value
        for e in exposures
        if e.dose_unit_source_value
    )
    drug_concept_ids = frozenset(e.drug_concept_id for e in exposures)

    return DrugExposureSummary(
        group_key=group_key,
        n_exposures=len(exposures),
        first_start_date=min(dates) if dates else None,
        last_start_date=max(dates) if dates else None,
        total_quantity=sum(quantities) if quantities else None,
        dose_unit_source_values=dose_units,
        drug_concept_ids=drug_concept_ids,
    )


def summarize_drug_exposures_by(
    exposures: Sequence[Drug_Exposure],
    key: Callable[[Drug_Exposure], Hashable],
) -> list[DrugExposureSummary]:
    grouped: dict[Hashable, list[Drug_Exposure]] = defaultdict(list)
    for exposure in exposures:
        grouped[key(exposure)].append(exposure)
    return [
        summarize_drug_exposures(rows, group_key=group_key)
        for group_key, rows in grouped.items()
    ]
