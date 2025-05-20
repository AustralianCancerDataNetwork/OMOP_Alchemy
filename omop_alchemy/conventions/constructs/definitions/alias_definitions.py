import sqlalchemy.orm as so

from ....model.clinical import Drug_Exposure, Procedure_Occurrence, Observation, Condition_Occurrence
from ....model.vocabulary import Concept, Concept_Ancestor

systemic_therapy = so.aliased(Drug_Exposure, flat=True)
radiation_therapy = so.aliased(Procedure_Occurrence, flat=True)
diagnosis = so.aliased(Condition_Occurrence, flat=True)
rth_ca = so.aliased(Concept_Ancestor, name='rth_ca')
srg_ca = so.aliased(Concept_Ancestor, name='srg_ca')

oncologist_consults = so.aliased(Observation, flat=True)
