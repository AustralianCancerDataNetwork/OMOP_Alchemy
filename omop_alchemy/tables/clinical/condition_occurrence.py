from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property

from .modifiable_table import Modifiable_Table
from ...db import Base

class Condition_Occurrence(Modifiable_Table):
    __tablename__ = 'condition_occurrence'
    # identifier
    condition_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'), primary_key=True, autoincrement=True)
    # temporal
    condition_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    condition_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    condition_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    condition_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    stop_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    condition_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    condition_status_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='co_fk_1'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='co_fk_2'))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='co_fk_3'))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='co_fk_4'))
    # concept fks
    condition_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_5'))
    condition_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_6'))
    condition_status_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_7'))
    condition_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_8'))
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept relationships
    condition_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_concept_id])
    condition_type_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_type_concept_id])
    condition_status_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_status_concept_id])
    condition_source_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_source_concept_id])

    __mapper_args__ = {
        "polymorphic_identity": "condition",
        'inherit_condition': (condition_occurrence_id == Modifiable_Table.modifier_id)
    }

    @hybrid_property
    def condition_label(self):
        if self.condition_concept:
            return self.condition_concept.concept_name
        
    @condition_label.expression
    def _condition_label_expression(cls) -> sa.ColumnElement[Optional[str]]:
        return sa.cast("SQLColumnExpression[Optional[str]]", cls.condition_concept.concept_name)