from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ...conventions.concept_enumerators import ModifierFields

class Modifiable_Table(Base):
    __tablename__ = 'modifiable_table'
    modifier_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    modifier_of_field_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='mt_fk_1'))

    modifiers: so.Mapped[List['Measurement']] = so.relationship(
        backref="modifying_object", lazy="selectin", viewonly=True
    )

    related_obs: so.Mapped[List['Observation']] = so.relationship(
        backref="observing_object", lazy="selectin", viewonly=True
    )

    related_events: so.Mapped[List['Episode_Event']] = so.relationship(
        backref="event_object", lazy="selectin", viewonly=True
    )
    
    # TODO - need to play with this so that the same base class can be polymorphic with both measurement table
    # modifiers and linked observations. Not possible to have two base classes both representing tables. 
    # only measurement-based modifiers currently work, although the observation class looks like it has been 
    # partially updated already 
    # EDIT - think this has been achieved, but needs to be validated

    __mapper_args__ = {
            "polymorphic_on":sa.case(
                (modifier_of_field_concept_id == ModifierFields.condition_occurrence_id.value, "condition"),
                (modifier_of_field_concept_id == ModifierFields.procedure_occurrence_id.value, "procedure"),
                (modifier_of_field_concept_id == ModifierFields.drug_exposure_id.value, "drug_exposure"),
                else_="episode"),
            "polymorphic_identity":"measurement"
        }
    