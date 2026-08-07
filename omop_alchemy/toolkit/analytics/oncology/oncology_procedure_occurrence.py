from __future__ import annotations

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session

from omop_alchemy.cdm.model.clinical.procedure_occurrence import (
    Procedure_OccurrenceView,
)

from .concept_sets import (
    cancer_indicating_surgery_parent_concept_ids,
    cancer_indicating_surgery_point_concept_ids,
    diagnostic_staging_procedure_parent_concept_ids,
    diagnostic_staging_procedure_point_concept_ids,
    procedure_concept_membership_expression,
    resolve_cancer_indicating_surgery_procedure_concept_ids,
    resolve_diagnostic_staging_procedure_concept_ids,
    resolve_rt_procedure_concept_ids,
    rt_procedure_parent_concept_ids,
)


class OncologyProcedure(Procedure_OccurrenceView):
    """
    Oncology-aware procedure occurrence view.

    This maps the standard ``procedure_occurrence`` table and adds governed
    concept-set membership checks used by oncology episode classification.
    """

    @hybrid_property
    def is_radiotherapy(self) -> bool:
        session = object_session(self)
        if session is None:
            return False
        return self.procedure_concept_id in resolve_rt_procedure_concept_ids(session)

    @is_radiotherapy.inplace.expression
    @classmethod
    def _is_radiotherapy_expression(cls):
        return procedure_concept_membership_expression(
            cls.procedure_concept_id,
            rt_procedure_parent_concept_ids(),
        )

    @hybrid_property
    def is_surgery(self) -> bool:
        session = object_session(self)
        if session is None:
            return False
        return (
            self.procedure_concept_id
            in resolve_cancer_indicating_surgery_procedure_concept_ids(session)
        )

    @is_surgery.inplace.expression
    @classmethod
    def _is_surgery_expression(cls):
        return procedure_concept_membership_expression(
            cls.procedure_concept_id,
            cancer_indicating_surgery_parent_concept_ids(),
            cancer_indicating_surgery_point_concept_ids(),
        )

    @hybrid_property
    def is_diagnostic_staging(self) -> bool:
        session = object_session(self)
        if session is None:
            return False
        return (
            self.procedure_concept_id
            in resolve_diagnostic_staging_procedure_concept_ids(session)
        )

    @is_diagnostic_staging.inplace.expression
    @classmethod
    def _is_diagnostic_staging_expression(cls):
        return procedure_concept_membership_expression(
            cls.procedure_concept_id,
            diagnostic_staging_procedure_parent_concept_ids(),
            diagnostic_staging_procedure_point_concept_ids(),
        )
