from __future__ import annotations

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session

from omop_alchemy.cdm.model.clinical.drug_exposure import Drug_ExposureView

from .concept_sets import (
    drug_concept_membership_expression,
    resolve_sact_drug_concept_ids,
    sact_drug_excluded_parent_concept_ids,
    sact_drug_parent_concept_ids,
)


class OncologyDrugExposure(Drug_ExposureView):
    """
    Oncology-aware drug exposure view.

    SACT classification is governed by omop-semantics and intentionally kept
    separate from generic drug episode summaries.
    """

    @hybrid_property
    def is_sact(self) -> bool:
        session = object_session(self)
        if session is None:
            return False
        return self.drug_concept_id in resolve_sact_drug_concept_ids(session)

    @is_sact.inplace.expression
    @classmethod
    def _is_sact_expression(cls):
        return drug_concept_membership_expression(
            cls.drug_concept_id,
            sact_drug_parent_concept_ids(),
            sact_drug_excluded_parent_concept_ids(),
        )
