from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so

from ...db import Base

class Survey_Conduct(Base):
    __tablename__ = 'survey_conduct'
    # identifier
    survey_conduct_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True) 
    # temporal
    survey_start_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    survey_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    survey_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date)
    survey_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    assisted_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    respondent_type_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    timing_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    collection_method_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    survey_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    survey_source_identifier: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    validated_survey_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(100))
    survey_version_number: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20))
    # numeric
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='sc_fk_1'))
    provider_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='sc_fk_2'))
    visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='sc_fk_3'))
    visit_detail_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='sc_fk_4'))    
    response_visit_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='sc_fk_5'))
    # concept fks
    survey_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_6'))
    assisted_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_7'))
    respondent_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_8'))
    timing_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_9'))
    collection_method_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_10'))
    validated_survey_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_11'))
    survey_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='sc_fk_12'))

    # relationships
    person: so.Mapped[Optional['Person']] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    response_visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[response_visit_occurrence_id])

    # concept_relationships
    survey_concept_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[survey_concept_id])
    assisted_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[assisted_concept_id])
    respondent_type_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[respondent_type_concept_id])
    timing_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[timing_concept_id])
    collection_method_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[collection_method_concept_id])
    validated_survey_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[validated_survey_concept_id])
    survey_source_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[survey_source_concept_id])
