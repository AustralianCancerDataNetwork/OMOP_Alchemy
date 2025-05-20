import sqlalchemy as sa
import sqlalchemy.orm as so

from ....model.clinical import Measurement
from ....model.vocabulary import Concept
from ...concept_enumerators import WeightConcepts

weights = (
    sa.select(
        Measurement.person_id, 
        Measurement.measurement_id,
        Measurement.measurement_datetime,
        Measurement.measurement_concept_id,
        Measurement.unit_concept_id,
        Measurement.value_as_number,
        Concept.concept_name.label('measurement_concept')
    )
    .join(Concept, Concept.concept_id==Measurement.measurement_concept_id)
    .filter(Measurement.measurement_concept_id.in_([v for v in WeightConcepts.member_values()])) # only measurements that are relevant to weight 
    .subquery("weights")
)