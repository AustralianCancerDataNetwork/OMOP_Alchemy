import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ....conventions.concept_enumerators import CancerProcedureTypes
from ....model.clinical import Procedure_Occurrence, Observation
from ....model.vocabulary import Concept, Concept_Ancestor
from .alias_definitions import rth_ca, srg_ca, diagnosis

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