from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ..conventions import ModifierFields


class Modifiable_Table(Base):
    __tablename__ = 'modifiable_table'
    modifier_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    modifier_of_field_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))


    modifiers: so.Mapped[List['Measurement']] = so.relationship(
        backref="modifying_object", lazy="selectin", viewonly=True
    )

    __mapper_args__ = {
            "polymorphic_on":sa.case(
                (modifier_of_field_concept_id == ModifierFields.condition_occurrence_id.value, "condition"),
                (modifier_of_field_concept_id == ModifierFields.procedure_occurrence_id.value, "procedure"),
                else_="episode"),
            "polymorphic_identity":"measurement"
        }