import sqlalchemy as sa
import sqlalchemy.orm as so

from ....db import Base
from ..definitions.surgical_subqueries import surgical_procedure, historical_surgery


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

