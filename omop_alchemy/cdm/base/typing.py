
from typing import Protocol, ClassVar, runtime_checkable, TYPE_CHECKING, Optional
from sqlalchemy.orm import DeclarativeMeta
from datetime import date

if TYPE_CHECKING:
    from omop_alchemy.cdm.base import ExpectedDomain
    from omop_alchemy.cdm.registry import DomainRule


@runtime_checkable
class HasTableName(Protocol):
    __tablename__: ClassVar[str]

@runtime_checkable
class HasConceptId(Protocol):
    concept_id: int

@runtime_checkable
class HasPersonId(Protocol):
    person_id: int

@runtime_checkable
class ORMTable(Protocol):
    __tablename__: ClassVar[str]
    __mapper__: ClassVar[DeclarativeMeta]

@runtime_checkable
class DomainSemanticTable(Protocol):
    __tablename__: ClassVar[str]
    __mapper__: ClassVar[DeclarativeMeta]
    __expected_domains__: ClassVar[dict[str, "ExpectedDomain"]]

    @classmethod
    def collect_domain_rules(cls) -> list["DomainRule"]: ...

class ClinicalEvent(Protocol):
    __tablename__: str

    event_id: int
    person_id: int
    concept_id: int

    start_date: date
    end_date: Optional[date]

    type_concept_id: int

    visit_occurrence_id: Optional[int]
    visit_detail_id: Optional[int]