import sqlalchemy as sa
import sqlalchemy.orm as so

from .MaterializedViewMixin import MaterializedViewMixin
#from .SpecialistVisits import VisitsBySpecialty
from ...concept_enumerators import ModifierFields, TreatmentEpisode, EAVMeasurements, GenomicMeasurements, CancerConsultTypes, CancerObservations, DiseaseEpisodeConcepts
from ....db import Base
from ....model.vocabulary import Concept, Concept_Ancestor
from ....model.clinical import Condition_Occurrence, Person, Measurement, Procedure_Occurrence, Observation
from ....model.onco_ext import Episode, Episode_Event

modifier_concept = so.aliased(Concept, name='modifier_concept')
value_concept = so.aliased(Concept, name='value_concept')
condition_concept = so.aliased(Concept, name='condition_concept')
procedure_concept = so.aliased(Concept, name='procedure_concept')

def unlinked_eav_modifier_query(modifier_concept_id, target_cols=[Measurement.measurement_concept_id, Measurement.value_as_number], join_col=Measurement.measurement_concept_id):
    return (
        sa.select(
            Measurement.person_id,
            Measurement.measurement_id, 
            Measurement.measurement_date, 
            modifier_concept.concept_name,
            modifier_concept.concept_id,
            *target_cols,
            Episode.episode_id,
            Episode.episode_start_datetime,
            (sa.func.extract('epoch', Measurement.measurement_date - Episode.episode_start_datetime)/86400).label('condition_delta_days')
        )
        .join(modifier_concept, modifier_concept.concept_id==join_col, isouter=True)
        .join(Episode, Episode.person_id==Measurement.person_id)
        .filter(Episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value)
        .filter(Measurement.measurement_concept_id==modifier_concept_id)
        .subquery()
    )

def dx_relevant_measures(starting_query, max_days_post=90, max_days_prior=30):
    return (
        sa.select(
            sa.func.row_number().over().label('mv_id'),
            *starting_query.c
        )
        .where(
            sa.and_(
                starting_query.c.condition_delta_days < max_days_post,
                starting_query.c.condition_delta_days >= -1*max_days_prior
            )
        )
        .subquery()
    )

weight_query = dx_relevant_measures(
    unlinked_eav_modifier_query(EAVMeasurements.weight.value, 
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

creatinine_clearance_query = dx_relevant_measures(
    unlinked_eav_modifier_query(
        EAVMeasurements.creatinine_clearance.value, 
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

est_gfr_query = dx_relevant_measures(
    unlinked_eav_modifier_query(
        EAVMeasurements.egfr.value, 
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

weight_change_query = dx_relevant_measures(
    unlinked_eav_modifier_query(
        EAVMeasurements.weight_change.value,
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

body_height_query = dx_relevant_measures(
    unlinked_eav_modifier_query(
        EAVMeasurements.body_height.value,
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

bsa_query = dx_relevant_measures(
    unlinked_eav_modifier_query(
        EAVMeasurements.bsa.value,
        [Measurement.value_as_number, Measurement.measurement_concept_id], 
        Measurement.unit_concept_id)
)

distress_thermometer_query = dx_relevant_measures(unlinked_eav_modifier_query(EAVMeasurements.distress_thermometer.value))
smoking_pyh_query = dx_relevant_measures(unlinked_eav_modifier_query(EAVMeasurements.smoking_pyh.value))
fev1_query = dx_relevant_measures(unlinked_eav_modifier_query(EAVMeasurements.fev1.value))
ecog_query = dx_relevant_measures(unlinked_eav_modifier_query(EAVMeasurements.ecog.value))


class DXRelevantMeasCols:
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    measurement_concept_id = sa.Column(sa.Integer)
    measurement_date = sa.Column(sa.Date)
    concept_id = sa.Column(sa.Integer)
    concept_name = sa.Column(sa.String)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.Date)
    condition_delta_days = sa.Column(sa.Integer)

# this could use a refactor to be the baseline for other MV obj.
all_dx_meas_select = (
        sa.select(
            sa.func.row_number().over().label('mv_id'),
            Measurement.person_id,
            Measurement.measurement_id, 
            Measurement.measurement_date, 
            modifier_concept.concept_name,
            modifier_concept.concept_id,
            Measurement.measurement_concept_id,
            Measurement.value_as_concept_id,
            # value_concept.concept_name.label('value_concept'),
            # value_concept.concept_id.label('value_concept_id'),
            Measurement.value_as_number,
            Episode.episode_id,
            Episode.episode_start_datetime,
            (sa.func.extract('epoch', Measurement.measurement_date - Episode.episode_start_datetime)/86400).label('condition_delta_days')
        )
        .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
        # .join(value_concept, value_concept.concept_id==Measurement.value_as_concept_id, isouter=True)
        .join(Episode, Episode.person_id==Measurement.person_id)
        .filter(Episode.episode_concept_id==DiseaseEpisodeConcepts.episode_of_care.value)
        .filter(Measurement.modifier_of_event_id==None)
)

class DXRelevantMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'dx_meas_mv'
    __mv_select__ = all_dx_meas_select.select()
    __tablename__ = __mv_name__
    # value_concept = sa.Column(sa.String)
    value_as_concept_id = sa.Column(sa.Integer)
    value_as_number = sa.Column(sa.Integer)

class ECOGMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'ecog_mv'
    __mv_select__ = ecog_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)

class FEVMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'fev_mv'
    __mv_select__ = fev1_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)

class PYHMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'pyh_mv'
    __mv_select__ = smoking_pyh_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)

class DistressThermMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'distress_mv'
    __mv_select__ = distress_thermometer_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class CreatClearance(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'creatinine_clearance_mv'
    __mv_select__ = creatinine_clearance_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class EstGFRate(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'est_gfr_mv'
    __mv_select__ = est_gfr_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class Weight(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'weight_mv'
    __mv_select__ = weight_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class WeightChange(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'weight_change_mv'
    __mv_select__ = weight_change_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class Height(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'height_mv'
    __mv_select__ = body_height_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)
    
class BSAMeas(DXRelevantMeasCols, MaterializedViewMixin, Base):
    __mv_name__ = 'bsa_mv'
    __mv_select__ = bsa_query.select()
    __tablename__ = __mv_name__
    value_as_number = sa.Column(sa.Integer)

# todo: factory pattern, more flexible windowing to handle local practice, other?



value_concept = so.aliased(Concept, name='value_concept')

genomic_query = (
    sa.select(
        Measurement.person_id,
        Measurement.measurement_id, 
        Measurement.measurement_date, 
        modifier_concept.concept_name.label('variant'),
        modifier_concept.concept_id.label('variant_concept_id'),
        value_concept.concept_name.label('value'),
        value_concept.concept_id.label('value_concept_id'),
        Episode.episode_id,
        Episode.episode_start_datetime,
    )
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
    .join(value_concept, value_concept.concept_id==Measurement.value_as_concept_id, isouter=True)
    .join(Episode, Episode.person_id==Measurement.person_id)
    .filter(Measurement.measurement_concept_id.in_(GenomicMeasurements.member_values()))
)

class GenomicVariants(MaterializedViewMixin, Base):
    __mv_pk__ = ["measurement_id"]
    __table_args__ = {"extend_existing": True}
    __mv_name__ = 'genomic_mv'
    __mv_select__ = genomic_query.select()
    __tablename__ = __mv_name__

    measurement_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    measurement_date = sa.Column(sa.Date)
    variant_concept_id = sa.Column(sa.Integer)
    variant = sa.Column(sa.String)
    value_concept_id = sa.Column(sa.Integer)
    value = sa.Column(sa.String)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.Integer)


unlinked_procedures_query = (
        sa.select(
            Procedure_Occurrence.person_id,
            Procedure_Occurrence.procedure_occurrence_id, 
            Procedure_Occurrence.procedure_date, 
            procedure_concept.concept_name,
            procedure_concept.concept_id,
            Episode.episode_id,
            Episode.episode_start_datetime,
            (sa.func.extract('epoch', Procedure_Occurrence.procedure_date - Episode.episode_start_datetime)/86400).label('condition_delta_days')
        )
        .join(procedure_concept, procedure_concept.concept_id==Procedure_Occurrence.procedure_concept_id, isouter=True)
        .join(Episode, Episode.person_id==Procedure_Occurrence.person_id)
        .filter(Procedure_Occurrence.procedure_concept_id.in_(CancerConsultTypes.member_values()))
        .subquery()
)

dx_relevant_procedures = dx_relevant_measures(unlinked_procedures_query, max_days_post=365)

class DXRelevantProc(MaterializedViewMixin, Base):
    __mv_pk__ = ["mv_id"]
    __mv_name__ = 'dx_proc_mv'
    __mv_select__ = dx_relevant_procedures.select()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    procedure_occurrence_id = sa.Column(sa.Integer)
    procedure_date = sa.Column(sa.Date)
    concept_id = sa.Column(sa.Integer)
    concept_name = sa.Column(sa.String)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.Integer)
    condition_delta_days = sa.Column(sa.Integer)


observation_concept = so.aliased(Concept, name='observation_concept')
observation_value_concept = so.aliased(Concept, name='observation_value_concept')
qualifier_concept = so.aliased(Concept, name='qualifier_concept')

unlinked_obs_query = (
        sa.select(
            Observation.person_id,
            Observation.observation_id, 
            Observation.observation_date, 
            observation_concept.concept_name,
            observation_concept.concept_id,
            observation_value_concept.concept_name.label('value_concept'),
            observation_value_concept.concept_id.label('value_concept_id'),
            qualifier_concept.concept_name.label('qualifier_concept'),
            qualifier_concept.concept_id.label('qualifier_concept_id'),
            Episode.episode_id,
            Episode.episode_start_datetime,
            (sa.func.extract('epoch', Observation.observation_date - Episode.episode_start_datetime)/86400).label('condition_delta_days')
        )
        .join(observation_concept, observation_concept.concept_id==Observation.observation_concept_id, isouter=True)
        .join(qualifier_concept, qualifier_concept.concept_id==Observation.qualifier_concept_id, isouter=True)
        .join(observation_value_concept, observation_value_concept.concept_id==Observation.value_as_concept_id, isouter=True)
        .join(Episode, Episode.person_id==Observation.person_id)
        .filter(Observation.observation_event_id==None)
        .filter(Observation.observation_concept_id.in_(CancerObservations.member_values()))
)

dx_relevant_obs = dx_relevant_measures(unlinked_obs_query, max_days_post=365)

class DXRelevantObs(MaterializedViewMixin, Base):
    __mv_pk__ = ["mv_id"]
    __mv_name__ = 'dx_obs_mv'
    __mv_select__ = dx_relevant_obs.select()
    __tablename__ = __mv_name__
    __table_args__ = {"extend_existing": True}
    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column(sa.Integer)
    observation_id = sa.Column(sa.Integer)
    observation_date = sa.Column(sa.Date)
    concept_id = sa.Column(sa.Integer)
    concept_name = sa.Column(sa.String)
    value_concept_id = sa.Column(sa.Integer)
    value_concept = sa.Column(sa.String)
    qualifier_concept_id = sa.Column(sa.Integer)
    qualifier_concept = sa.Column(sa.String)
    episode_id = sa.Column(sa.Integer)
    episode_start_datetime = sa.Column(sa.Integer)
    condition_delta_days = sa.Column(sa.Integer)
