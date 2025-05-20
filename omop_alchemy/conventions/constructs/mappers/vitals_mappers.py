from ..definitions.vitals_subqueries import weights
from ....db import Base

class Weights(Base):
    __table__ = weights

    person_id = weights.c.person_id
    measurement_id = weights.c.measurement_id
    measurement_date = weights.c.measurement_datetime
    measurement_concept = weights.c.measurement_concept
    value_as_number = weights.c.value_as_number
    unit_concept_id = weights.c.unit_concept_id

    @property
    def weight_base_unit(self):
        ...
        # if unit == x...

        # return numeric value that has been converted to a selected base unit
