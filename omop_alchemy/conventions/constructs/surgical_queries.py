import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ...conventions.concept_enumerators import CancerProcedureTypes
from ..clinical.procedure_occurrence import Procedure_Occurrence
from ..clinical.observation import Observation
from ..vocabulary.concept import Concept
from ..vocabulary.concept_ancestor import Concept_Ancestor


rth_ca = so.aliased(Concept_Ancestor, name='rth_ca')
srg_ca = so.aliased(Concept_Ancestor, name='srg_ca')

"""
Note that there are some concepts that have ancestry defined under surgical procedure
and radiotherapy procedure.
"""

surgical = (
    sa.select(
        Concept.concept_name,
        Concept.concept_code,
        Concept.concept_id,
        srg_ca.descendant_concept_id
    )
    .join(srg_ca, Concept.concept_id == srg_ca.descendant_concept_id)
    .filter(srg_ca.ancestor_concept_id==CancerProcedureTypes.surgical_procedure.value)
    .subquery()
)

radiotherapy = (
    sa.select(
        rth_ca.descendant_concept_id.label('rt_id')
    )
    .select_from(rth_ca)
    .filter(
        sa.or_(
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rt_procedure.value,
            rth_ca.ancestor_concept_id==CancerProcedureTypes.rn_procedure.value
        )
    )
    .subquery()
)
    
surg_only = (
    sa.join(
        surgical,
        radiotherapy,
        radiotherapy.c.rt_id == surgical.c.descendant_concept_id,
        isouter=True
    )
)

surgical_procedure = (
    sa.select(
        Procedure_Occurrence.person_id,
        Procedure_Occurrence.procedure_occurrence_id,
        Procedure_Occurrence.procedure_concept_id,
        Procedure_Occurrence.procedure_datetime,
        surgical.c.concept_name,
        surgical.c.concept_code
    )
    .join(surg_only, surgical.c.concept_id == Procedure_Occurrence.procedure_concept_id)
    .filter(radiotherapy.c.rt_id == None)
    .subquery()
)

class Dated_Surgical_Procedure(Base):
    """
    Denoted as dated - distinct from historical, because we know at least some
    details of when this event occurred. 
    """
    __table__ = surgical_procedure
    procedure_occurrence_id = surgical_procedure.c.procedure_occurrence_id
    person_id = surgical_procedure.c.person_id
    procedure_concept_id = surgical_procedure.c.procedure_concept_id
    concept_name = surgical_procedure.c.concept_name
    concept_code = surgical_procedure.c.concept_code
    procedure_datetime = so.column_property(surgical_procedure.c.procedure_datetime)


historical_procedure = so.aliased(
    Observation,
    sa.select(Observation).where(
        Observation.observation_concept_id == CancerProcedureTypes.historical_procedure.value
    ).subquery(), 
    'historical_procedure'
)


historical_surgery = (
    sa.select(
        historical_procedure.person_id,
        historical_procedure.observation_id,
        historical_procedure.value_as_concept_id,
        historical_procedure.observation_datetime
    ).join(
        surgical, surgical.c.concept_id == historical_procedure.value_as_concept_id
    ).subquery()
)

class Historical_Surgical_Procedure(Base):
    """
    Some procedures may be noted as an observation as part of clinical history - these 
    procedures will not have an available date.

    They can be used to filter cohorts in certain situations ('no prior surgery' etc.),
    but won't typically be used directly in terms of measured outcomes or review of guideline
    adherence. 

    They should be noted as an observation, with 
    observation_concept_id = CancerProcedureTypes.historical_procedure.value
    """
    __table__ = historical_surgery
    observation_id = historical_surgery.c.observation_id
    person_id = historical_surgery.c.person_id
    procedure_concept_id = historical_surgery.c.value_as_concept_id
    history_datettime = so.column_property(historical_surgery.c.observation_datetime)

