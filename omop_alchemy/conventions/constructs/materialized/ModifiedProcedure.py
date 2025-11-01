import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement, Modifiable_Table, Drug_Exposure, Procedure_Occurrence
from .MaterializedViewMixin import MaterializedViewMixin
from ...concept_enumerators import ModifierFields, TreatmentIntent
from ....db import Base

modifier_concept = so.aliased(Concept, name='modifier_concept')
procedure_concept = so.aliased(Concept, name='procedure_concept')

modified_procedure_join = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_datetime, 
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_source_value,
        Procedure_Occurrence.procedure_concept_id,
        procedure_concept.concept_name.label('procedure_concept'),
        Measurement.measurement_id.label('intent_id'),
        Measurement.measurement_datetime.label('intent_datetime'),
        Measurement.measurement_concept_id.label('intent_concept_id'),
        modifier_concept.concept_name.label('intent_concept'),
        sa.func.row_number().over().label('mv_id')
    )
    .join(
        Measurement, 
        sa.and_(
            Measurement.modifier_of_field_concept_id==ModifierFields.procedure_occurrence_id.value,
            Procedure_Occurrence.procedure_occurrence_id==Measurement.modifier_of_event_id,
            Measurement.measurement_concept_id.in_(TreatmentIntent.member_values())
        ),
        isouter=True
    )
    .join(procedure_concept, procedure_concept.concept_id==Procedure_Occurrence.procedure_concept_id)
    .join(modifier_concept, modifier_concept.concept_id==Measurement.measurement_concept_id, isouter=True)
)


class ModifiedProcedure(MaterializedViewMixin, Base):
    __mv_name__ = 'modified_procedure_mv'
    __mv_select__ = modified_procedure_join.select()
    __mv_pk__ = ["mv_id"]
    __table_args__ = {"extend_existing": True}
    __tablename__ = __mv_name__

    mv_id = sa.Column(primary_key=True)
    person_id = sa.Column()
    procedure_datetime = sa.Column()
    procedure_occurrence_id = sa.Column()
    procedure_source_value = sa.Column()
    procedure_concept_id = sa.Column()
    procedure_concept = sa.Column()
    intent_id = sa.Column()
    intent_datetime = sa.Column()
    intent_concept_id = sa.Column()
    intent_concept = sa.Column()