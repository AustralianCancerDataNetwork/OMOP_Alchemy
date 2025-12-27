import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....conventions.concept_enumerators import CancerProcedureTypes
from ....model.clinical import Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from .MaterializedViewMixin import MaterializedViewMixin
from .ConditionTreatmentEpisode import OverarchingCondition

"""
Note that there are some concepts that have ancestry defined under surgical procedure
and radiotherapy procedure.
"""
rth_ca = sa.orm.aliased(Concept_Ancestor, name='rth_ca')
srg_ca = sa.orm.aliased(Concept_Ancestor, name='srg_ca')

surgical = (
    sa.select(
        Concept.concept_name,
        Concept.concept_code,
        Concept.concept_id,
    )
    .join(srg_ca, Concept.concept_id == srg_ca.descendant_concept_id)
    .filter(srg_ca.ancestor_concept_id==CancerProcedureTypes.surgical_procedure.value)
)

radiotherapy = (
    sa.select(
        Concept.concept_name,
        Concept.concept_code,
        Concept.concept_id,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .filter(
        sa.or_(
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rt_procedure.value,
        )
    )
)

radioisotopes = (
    sa.select(
        Concept.concept_name,
        Concept.concept_code,
        Concept.concept_id,
    )
    .join(rth_ca, Concept.concept_id == rth_ca.descendant_concept_id)
    .filter(
        sa.or_(
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rn_procedure.value
        )
    )
)

surg_only = surgical.except_all(radiotherapy.union_all(radioisotopes)).subquery()
radioisotopes_only = radioisotopes.subquery()

surgical_procedure = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id.label('surgery_occurrence_id'),
        Procedure_Occurrence.procedure_concept_id.label('surgery_concept_id'),
        Procedure_Occurrence.procedure_datetime.label('surgery_datetime'),
        surg_only.c.concept_name.label('surgery_name'),
        surg_only.c.concept_code.label('surgery_concept_code'),
        sa.sql.expression.literal('procedure').label('surgery_source')
    )
    .join(surg_only, surg_only.c.concept_id == Procedure_Occurrence.procedure_concept_id)
)

surg_obs_concept = so.aliased(Concept, name='surg_obs_concept')

historical_procedure = (
    sa.select(
        Observation.person_id,
        Observation.observation_id.label('surgery_occurrence_id'),
        Observation.value_as_concept_id.label('surgery_concept_id'),
        Observation.observation_datetime.label('surgery_datetime'),
        surg_obs_concept.concept_name.label('surgery_name'),
        surg_obs_concept.concept_code.label('surgery_concept_code'),
        sa.sql.expression.literal('observation').label('surgery_source')
    )
    .join(surg_obs_concept, surg_obs_concept.concept_id == Observation.observation_concept_id)
    .filter(Observation.observation_concept_id == CancerProcedureTypes.historical_procedure.value)
)

all_cancer_relevant_surg = (
    sa.union_all(
        historical_procedure, 
        surgical_procedure
    )
    .subquery()
)

cancer_relevant_surg = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        OverarchingCondition.person_id, 
        OverarchingCondition.condition_concept, 
        OverarchingCondition.condition_start_date,
        OverarchingCondition.overarching_episode_id,
        all_cancer_relevant_surg.c.surgery_occurrence_id,
        all_cancer_relevant_surg.c.surgery_concept_id,
        all_cancer_relevant_surg.c.surgery_datetime,
        all_cancer_relevant_surg.c.surgery_concept_code,
        all_cancer_relevant_surg.c.surgery_name,
        all_cancer_relevant_surg.c.surgery_source
    )
    # todo: surgical procedures are not currently mapped to episodes because we have not 
    # done any date windowing for historical procedures, so joining on person_id
    # at the moment this is safe because we are only pulling in manually-entered surgical 
    # procedures, which can be safely assumed to be cancer relevant
    .join(all_cancer_relevant_surg, all_cancer_relevant_surg.c.person_id == OverarchingCondition.person_id, isouter=True)
)

radioisotope_procedure = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        OverarchingCondition.person_id, 
        OverarchingCondition.condition_concept, 
        OverarchingCondition.condition_start_date,
        OverarchingCondition.overarching_episode_id,
        Procedure_Occurrence.procedure_occurrence_id.label('ri_occurrence_id'),
        Procedure_Occurrence.procedure_concept_id.label('ri_concept_id'),
        Procedure_Occurrence.procedure_datetime.label('ri_datetime'),
        radioisotopes_only.c.concept_name.label('ri_name'),
        radioisotopes_only.c.concept_code.label('ri_concept_code'),
        sa.sql.expression.literal('radioisotope_procedure').label('ri_source')
    )
    .join(Procedure_Occurrence, Procedure_Occurrence.person_id == OverarchingCondition.person_id, isouter=True)
    .join(radioisotopes_only, radioisotopes_only.c.concept_id == Procedure_Occurrence.procedure_concept_id, isouter=True)
)


class SurgicalProcedure(MaterializedViewMixin, Base):

    __mv_name__ = 'surgical_procedure_mv'
    __mv_select__ = cancer_relevant_surg.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_start_date = sa.Column(sa.Date)
    overarching_episode_id = sa.Column(sa.Integer)
    surgery_occurrence_id = sa.Column(sa.Integer)
    surgery_concept_id = sa.Column(sa.Integer)
    surgery_datetime = sa.Column(sa.DateTime)
    surgery_name = sa.Column(sa.String)
    surgery_concept_code = sa.Column(sa.String)
    surgery_source = sa.Column(sa.String)


class Radioisotope(MaterializedViewMixin, Base):

    __mv_name__ = 'radioisotope_mv'
    __mv_select__ = radioisotope_procedure.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    condition_concept = sa.Column(sa.String)
    condition_start_date = sa.Column(sa.Date)
    overarching_episode_id = sa.Column(sa.Integer)
    ri_occurrence_id = sa.Column(sa.Integer)
    ri_concept_id = sa.Column(sa.Integer)
    ri_datetime = sa.Column(sa.DateTime)
    ri_name = sa.Column(sa.String)
    ri_concept_code = sa.Column(sa.String)
    ri_source = sa.Column(sa.String)