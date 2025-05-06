import sqlalchemy.orm as so

from ...model.clinical import Drug_Exposure, Procedure_Occurrence, Observation
from ...model.vocabulary import Concept, Concept_Ancestor


systemic_therapy = so.aliased(Drug_Exposure, flat=True)
radiation_therapy = so.aliased(Procedure_Occurrence, flat=True)
