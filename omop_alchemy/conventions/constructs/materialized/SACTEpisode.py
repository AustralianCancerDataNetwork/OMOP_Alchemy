import sqlalchemy as sa
import sqlalchemy.orm as so

from .MaterializedViewMixin import MaterializedViewMixin
from .ModifiedProcedure import ModifiedProcedure
from ...concept_enumerators import ModifierFields, TreatmentEpisode
from ....db import Base
from ....model.vocabulary import Concept, Concept_Ancestor
from ....model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from ....model.onco_ext import Episode, Episode_Event
# note: this will only pull in drug exposure events that have been explicitly 
# linked to episodes via care plan - it will miss incidental events 
# (mostly blood products & non SACT)

drug_concept = so.aliased(Concept, name='drug_concept')
route_concept = so.aliased(Concept, name='route_concept')
cycle_concept = so.aliased(Concept, name='cycle_concept')

cycle_join = (
    sa.select(
        sa.func.row_number().over().label('mv_id'),
        Drug_Exposure.person_id,
        Drug_Exposure.drug_exposure_start_date,
        Drug_Exposure.drug_exposure_end_date,
        Drug_Exposure.drug_exposure_id,
        Drug_Exposure.quantity,
        Drug_Exposure.drug_concept_id,
        Drug_Exposure.dose_unit_source_value,
        drug_concept.concept_name.label('drug_name'),
        #unit_concept.concept_name.label('unit'),
        route_concept.concept_name.label('route'),
        Episode.episode_id.label('cycle_id'),
        Episode.episode_number.label('cycle_number'),
        Episode.episode_parent_id.label('regimen_id'),
        cycle_concept.concept_name.label('cycle_concept')
    )
    .join(drug_concept, drug_concept.concept_id==Drug_Exposure.drug_concept_id)
    .join(route_concept, route_concept.concept_id==Drug_Exposure.route_concept_id)
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.event_id==Drug_Exposure.drug_exposure_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.drug_exposure_id.value
        )
    )
    .join(Episode, Episode.episode_id==Episode_Event.episode_id)
    .join(cycle_concept, cycle_concept.concept_id==Episode.episode_concept_id)
)

class Cycle(MaterializedViewMixin, Base):
    __mv_name__ = 'cycle_mv'
    __mv_select__ = cycle_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    drug_exposure_id = sa.Column(sa.Integer)
    drug_exposure_start_date = sa.Column(sa.Date)
    drug_exposure_end_date = sa.Column(sa.Date)
    quantity = sa.Column(sa.Numeric)
    drug_concept_id = sa.Column(sa.Integer)
    dose_unit_source_value = sa.Column(sa.String)
    drug_name = sa.Column(sa.String)
    route = sa.Column(sa.String)
    cycle_id = sa.Column(sa.Integer)
    cycle_number = sa.Column(sa.Integer)
    cycle_concept = sa.Column(sa.Integer)
    regimen_id = sa.Column(sa.Integer)


# note: this will only pull in drug exposure events that have been explicitly 
# linked to episodes via care plan - it will miss incidental events 
# (mostly blood products & non SACT)

cycle_summary_join = (
    sa.select(
        Cycle.person_id,
        Cycle.cycle_id,
        Cycle.cycle_number,
        Cycle.regimen_id,
        Cycle.cycle_concept,
        sa.func.min(Cycle.drug_exposure_start_date).label('first_exposure_date'),
        sa.func.max(Cycle.drug_exposure_end_date).label('last_exposure_date'),
        sa.func.count(Cycle.drug_exposure_id).label('exposure_count'),
    )
    .group_by(Cycle.person_id, Cycle.cycle_id, Cycle.cycle_number, Cycle.regimen_id, Cycle.cycle_concept)
    .subquery()
)

regimen_join = (
    sa.select(
        *cycle_summary_join.c,
        sa.func.row_number().over().label('mv_id'),
        Episode.episode_number.label('regimen_number'),
        Episode.episode_parent_id.label('condition_episode_id'),
        ModifiedProcedure.procedure_occurrence_id.label('regimen_prescription_id'),
        ModifiedProcedure.procedure_concept_id.label('regimen_concept_id'),
        ModifiedProcedure.procedure_concept.label('regimen_concept'),
        ModifiedProcedure.intent_concept,
        ModifiedProcedure.intent_concept_id,
        ModifiedProcedure.intent_datetime,
    )
    .join(
        Episode, 
        sa.and_(
            Episode.episode_id == cycle_summary_join.c.regimen_id, 
            Episode.episode_concept_id==TreatmentEpisode.treatment_regimen.value
        ),
        isouter=True
    )
    .join(
        Episode_Event, 
        sa.and_(
            Episode_Event.episode_id==Episode.episode_id,
            Episode_Event.episode_event_field_concept_id==ModifierFields.procedure_occurrence_id.value
        ),
        isouter=True
    )
    .join(ModifiedProcedure, ModifiedProcedure.procedure_occurrence_id==Episode_Event.event_id, isouter=True)
)

class SACTRegimen(MaterializedViewMixin, Base):
    __mv_name__ = 'sact_reg_mv'
    __mv_select__ = regimen_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    cycle_id = sa.Column(sa.Integer)
    cycle_number = sa.Column(sa.Integer)
    cycle_concept = sa.Column(sa.String)
    exposure_count = sa.Column(sa.Integer)
    first_exposure_date = sa.Column(sa.Date)
    last_exposure_date = sa.Column(sa.Date)
    regimen_id = sa.Column(sa.Integer)
    regimen_number = sa.Column(sa.Integer)
    condition_episode_id = sa.Column(sa.Integer)
    regimen_prescription_id = sa.Column(sa.Integer)
    regimen_concept_id = sa.Column(sa.Integer)
    regimen_concept = sa.Column(sa.String)
    intent_datetime = sa.Column(sa.DateTime)
    intent_concept_id = sa.Column(sa.Integer)
    intent_concept = sa.Column(sa.String)

    
regimen_summary_join = (
    sa.select(
        SACTRegimen.person_id,
        SACTRegimen.regimen_concept,
        SACTRegimen.regimen_id,
        SACTRegimen.regimen_number,
        SACTRegimen.condition_episode_id,
        SACTRegimen.intent_concept_id.label('sact_intent_concept_id'),
        SACTRegimen.intent_concept.label('sact_intent_concept'),
        sa.func.min(SACTRegimen.first_exposure_date).label('regimen_start_date'),
        sa.func.max(SACTRegimen.last_exposure_date).label('regimen_end_date'),
        sa.func.sum(SACTRegimen.exposure_count).label('exposure_count'),
        sa.func.count(SACTRegimen.regimen_id).label('regimen_count'),
    )
    .group_by(SACTRegimen.person_id, SACTRegimen.regimen_concept, SACTRegimen.regimen_id, SACTRegimen.regimen_number, SACTRegimen.condition_episode_id, SACTRegimen.intent_concept_id, SACTRegimen.intent_concept)
    .subquery()
)
