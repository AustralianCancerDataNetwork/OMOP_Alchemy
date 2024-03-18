from datetime import datetime, date
from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from .modifiable_table import Modifiable_Table
from ...db import Base

class Drug_Exposure(Modifiable_Table):
    __tablename__ = 'drug_exposure'    
    # identifier
    drug_exposure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'), primary_key=True, autoincrement=True)

    # temporal
    drug_exposure_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    drug_exposure_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    drug_exposure_end_date: so.Mapped[date] = so.mapped_column(sa.Date)
    drug_exposure_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)    
    verbatim_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    # strings
    stop_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20), nullable=True)
    lot_number: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    drug_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    route_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    dose_unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    sig: so.Mapped[Optional[str]] = so.mapped_column(sa.Text, nullable=True)
    # numeric
    refills: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    days_supply: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    quantity: so.Mapped[Optional[int]] = so.mapped_column(sa.Numeric, nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='dr_fk_1'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='dr_fk_2'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='dr_fk_3'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='dr_fk_4'), nullable=True)
    # concept fks
    drug_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dr_fk_5'))
    drug_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dr_fk_6'))
    route_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dr_fk_7'), nullable=True)
    drug_source_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='dr_fk_8'), nullable=True)
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept_relationships
    drug_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_concept_id])
    drug_type_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_type_concept_id])
    route_concept: so.Mapped[Optional['Concept']] = so.relationship(foreign_keys=[route_concept_id])
    drug_source_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[drug_source_concept_id])


    __mapper_args__ = {
        "polymorphic_identity": "drug_exposure",
        'inherit_condition': (drug_exposure_id == Modifiable_Table.modifier_id)
    }


    @hybrid_property
    def event_date(self):
        return  self.drug_exposure_start_datetime.date() if self.drug_exposure_start_datetime is not None else self.drug_exposure_start_date
