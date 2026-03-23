import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional, TYPE_CHECKING
from datetime import date, datetime

from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    PersonScoped, 
    HealthSystemContext, 
    FactTable, 
    CDMTableBase,
    cdm_table, 
    required_concept_fk,
    optional_concept_fk,
    optional_int,
    ModifierTargetMixin,
    merge_table_args,
    omop_index,
)

if TYPE_CHECKING:
    from ..vocabulary import Concept
    from ..health_system import Visit_Occurrence

@cdm_table
class Device_Exposure(
    PersonScoped,
    CDMTableBase,
    FactTable,
    ModifierTargetMixin,
    HealthSystemContext,
    Base,
):
    __tablename__ = "device_exposure"
    __table_args__ = merge_table_args(
        omop_index("idx_device_person_id_1", "person_id", cluster=True),
        omop_index("idx_device_concept_id_1", "device_concept_id"),
        omop_index("idx_device_visit_id_1", "visit_occurrence_id"),
        omop_index("ix_device_exposure_device_type_concept_id", "device_type_concept_id"),
        omop_index("ix_device_exposure_provider_id", "provider_id"),
        omop_index("ix_device_exposure_visit_detail_id", "visit_detail_id"),
    )

    device_exposure_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    
    device_exposure_start_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    device_exposure_end_date: so.Mapped[date] = so.mapped_column(sa.Date, nullable=False)
    device_exposure_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    device_exposure_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    
    device_concept_id: so.Mapped[int] = required_concept_fk()
    device_type_concept_id: so.Mapped[int] = required_concept_fk()
    device_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    unit_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()
    unit_source_concept_id: so.Mapped[Optional[int]] = optional_concept_fk()

    unique_device_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255),nullable=True)
    production_id: so.Mapped[Optional[str]] = so.mapped_column(sa.String(255),nullable=True)
    quantity: so.Mapped[Optional[int]] = optional_int()
    device_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    unit_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
