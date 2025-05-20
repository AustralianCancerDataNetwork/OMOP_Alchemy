from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy

from ...db import Base
from ...conventions.concept_enumerators import ModifierFields

class Modifiable_Table(Base):
    __tablename__ = 'modifiable_table'
    modifier_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    modifier_of_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='mt_fk_1'))

    modifiers: so.Mapped[List['Measurement']] = so.relationship(
        backref="modifying_object", lazy="selectin", viewonly=True
    )

    related_obs: so.Mapped[List['Observation']] = so.relationship(
        backref="observing_object", lazy="selectin", viewonly=True
    )

    related_events: so.Mapped[List['Episode_Event']] = so.relationship(
        backref="event_object", lazy="selectin", viewonly=True
    )

    modifier_concepts: AssociationProxy[List[int]] = association_proxy("modifiers", "measurement_concept_id")
    modifier_value_concepts: AssociationProxy[List[int]] = association_proxy("modifiers", "value_as_concept_id")

    @property
    def primary_event(self):
        return [event.primary_ep for event in self.related_events]
    
    @property
    def event_date(self):
        return None


    __mapper_args__ = {
            "polymorphic_on":sa.case(
                (modifier_of_field_concept_id == ModifierFields.condition_occurrence_id.value, "condition"),
                (modifier_of_field_concept_id == ModifierFields.procedure_occurrence_id.value, "procedure"),
                (modifier_of_field_concept_id == ModifierFields.drug_exposure_id.value, "drug_exposure"),
                else_="episode"),
            "polymorphic_identity":"measurement"
        }
    
    
    @property
    def polymorphic_label(self):
        return self.__mapper_args__['polymorphic_identity']