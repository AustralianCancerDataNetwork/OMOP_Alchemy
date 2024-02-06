from datetime import date, datetime, time
from typing import Optional
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base
from ..conventions import ModifierFields

class Measurement(Base):
    __tablename__ = 'measurement'
    # identifier
    measurement_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    measurement_date: so.Mapped[date] = so.mapped_column(sa.Date)
    measurement_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    measurement_time: so.Mapped[Optional[time]] = so.mapped_column(sa.Time)
    # strings
    measurement_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    value_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    value_as_number: so.Mapped[Decimal] = so.mapped_column(sa.Numeric)
    range_low: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)
    range_high: so.Mapped[Optional[Decimal]] = so.mapped_column(sa.Numeric)

    modifier_of_event_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'))
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id'))
    # concept fks
    value_as_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    measurement_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    measurement_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    measurement_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    operator_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    modifier_of_field_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    person_object: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider_object: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence_object: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail_object: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    modified_object: so.Mapped[Optional['Modifiable_Table']] = so.relationship(foreign_keys=[modifier_of_event_id])
    # concept_relationships
    unit_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[unit_concept_id])
    measurement_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[measurement_concept_id])
    measurement_source_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[measurement_source_concept_id])
    measurement_type_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[measurement_type_concept_id])
    operator_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[operator_concept_id])
    modifier_of_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[modifier_of_field_concept_id])
    value_as_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[value_as_concept_id])



# class Condition_Modifier(Measurement):
#     __tablename__ = 'condition_modifier'    
#     measurement_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("measurement.measurement_id"), primary_key=True)
#     condition_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('condition_occurrence.condition_occurrence_id'))
#     # condition_occurrence_object: so.Mapped[Optional['Condition_Occurrence']] = so.relationship(foreign_keys=[condition_occurrence_id])
#     __mapper_args__ = {
#         "polymorphic_load": "inline",
#         "polymorphic_identity": "condition_modifier",
#     }


# class Procedure_Modifier(Measurement):
#     __tablename__ = 'procedure_modifier'
#     measurement_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("measurement.measurement_id"), primary_key=True)
#     __mapper_args__ = {
#         "polymorphic_load": "inline",
#         "polymorphic_identity": "procedure_modifier",
#     }


# class Episode_Modifier(Measurement):
#     __tablename__ = 'episode_modifier'
#     measurement_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("measurement.measurement_id"), primary_key=True)
#     __mapper_args__ = {
#         "polymorphic_load": "inline",
#         "polymorphic_identity": "episode_modifier",
#     }