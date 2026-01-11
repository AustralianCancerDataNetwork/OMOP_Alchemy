from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from typing import Optional
from datetime import date, datetime
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    CDMTableBase,
    cdm_table,
    ValueMixin
)

@cdm_table
class Observation(Base, CDMTableBase, ValueMixin):
    __tablename__ = "observation"

    observation_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"), nullable=False, index=True)
    observation_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=False, index=True)
    observation_date: so.Mapped[date] = so.mapped_column(nullable=False)
    observation_datetime: so.Mapped[Optional[datetime]]
    observation_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"), nullable=False)
    #value_as_number: so.Mapped[Optional[float]]
    value_as_string: so.Mapped[Optional[str]]
    #value_as_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    qualifier_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    unit_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("provider.provider_id"))
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_occurrence.visit_occurrence_id"))
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("visit_detail.visit_detail_id"))
    observation_source_value: so.Mapped[Optional[str]]
    observation_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"))
    unit_source_value: so.Mapped[Optional[str]]
    qualifier_source_value: so.Mapped[Optional[str]]
    value_source_value: so.Mapped[Optional[str]]
    observation_event_id: so.Mapped[Optional[int]]
    obs_event_field_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("concept.concept_id"), index=True)

    @hybrid_property
    def modifier_of_event_id(self) -> Optional[int]:
        return self.observation_event_id

    @hybrid_property
    def modifier_of_field_concept_id(self) -> Optional[int]:
        return self.obs_event_field_concept_id