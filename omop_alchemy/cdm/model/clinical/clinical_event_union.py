from sqlalchemy import select, union_all, literal
from .condition_occurrence import Condition_Occurrence
#from .drug_exposure import Drug_Exposure

from orm_loader.helpers import Base

clinical_event_union = union_all(
    select(
        literal("condition").label("domain"),
        Condition_Occurrence.condition_occurrence_id.label("event_id"),
        Condition_Occurrence.person_id,
        Condition_Occurrence.condition_concept_id.label("concept_id"),
        Condition_Occurrence.condition_start_date.label("start_date"),
        Condition_Occurrence.condition_end_date.label("end_date"),
        Condition_Occurrence.condition_type_concept_id.label("type_concept_id"),
        Condition_Occurrence.visit_occurrence_id,
        Condition_Occurrence.visit_detail_id,
    ),
    # select(
    #     literal("drug").label("domain"),
    #     Drug_Exposure.drug_exposure_id,
    #     Drug_Exposure.person_id,
    #     Drug_Exposure.drug_concept_id,
    #     Drug_Exposure.drug_exposure_start_date,
    #     Drug_Exposure.drug_exposure_end_date,
    #     Drug_Exposure.drug_type_concept_id,
    #     Drug_Exposure.visit_occurrence_id,
    #     Drug_Exposure.visit_detail_id,
    # ),
    # procedure...
).subquery("clinical_event")

class ClinicalEventView(Base):
    __table__ = clinical_event_union
    __mapper_args__ = {
        "primary_key": [
            clinical_event_union.c.domain,
            clinical_event_union.c.event_id,
        ]
    }