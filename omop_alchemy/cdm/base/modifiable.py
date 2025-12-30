from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from typing import List, ClassVar, Dict, TYPE_CHECKING, Optional, Type
from datetime import date
from .cdm_constants import ModifierFieldConcepts
from .declarative import Base
from .typing import ClinicalEvent

if TYPE_CHECKING:
    from omop_alchemy.model.clinical import Measurement, Observation
    from omop_alchemy.model.structural import Episode_Event


class ModifierTargetMixin:
    """
    Marker + helpers for OMOP tables that can be modified
    by Measurements / Observations / Episode Events.
    """

    __abstract__ = True
    __event_id_col__: ClassVar[str]
    __concept_id_col__: ClassVar[str]
    __start_date_col__: ClassVar[str]
    __end_date_col__: ClassVar[str]
    __type_concept_id_col__: ClassVar[str]

    @classmethod
    def modifier_field_concept_id(cls) -> int:
        raise NotImplementedError

    @classmethod
    def modifier_target_table(cls) -> str:
        return cls.__tablename__ # type: ignore[attr-defined]

    @hybrid_property
    def event_id(self) -> int: # type: ignore
        return getattr(self, self.__event_id_col__)
    
    @event_id.expression
    def event_id(cls):
        return getattr(cls, cls.__event_id_col__)

    @property
    def concept_id(self) -> int:
        return getattr(self, self.__concept_id_col__)

    @property
    def start_date(self) -> date:
        return getattr(self, self.__start_date_col__)

    @property
    def end_date(self) -> Optional[date]:
        return getattr(self, self.__end_date_col__)

    @property
    def type_concept_id(self) -> int:
        return getattr(self, self.__type_concept_id_col__)


class ModifiableRelationshipsMixin:
    """
    Adds viewonly relationships for Measurement / Observation / Episode_Event
    that target this event table.

    Requires the class to implement:
      - modifier_field_concept_id() -> int
      - event_id column property (or __event_id_col__)
    """

    __abstract__ = True

    @classmethod
    def modifier_field_concept_id(cls) -> int:
        raise NotImplementedError

    @declared_attr
    def modifiers(cls) -> so.Mapped[List["Measurement"]]:
        from omop_alchemy.model.clinical import Measurement  # adjust import

        return so.relationship(
            Measurement,
            primaryjoin=lambda: sa.and_(
                Measurement.modifier_of_event_id == cls.event_id,  # type: ignore
                Measurement.modifier_of_field_concept_id == cls.modifier_field_concept_id(),
            ),
            viewonly=True,
            lazy="selectin",
        )

    @declared_attr
    def related_obs(cls) -> so.Mapped[List["Observation"]]:
        from omop_alchemy.model.clinical import Observation

        return so.relationship(
            Observation,
            primaryjoin=lambda: sa.and_(
                Observation.observation_event_id == cls.event_id, # type: ignore
                Observation.obs_event_field_concept_id == cls.modifier_field_concept_id(),
            ),
            viewonly=True,
            lazy="selectin",
        )

    # @declared_attr
    # def related_events(cls) -> so.Mapped[List["Episode_Event"]]:
    #     from omop_alchemy.model.clinical import Episode_Event

    #     return so.relationship(
    #         Episode_Event,
    #         primaryjoin=lambda: sa.and_(
    #             Episode_Event.event_id == cls.event_id,
    #             Episode_Event.episode_event_field_concept_id == cls.modifier_field_concept_id(),
    #         ),
    #         viewonly=True,
    #         lazy="selectin",
    #     )

    modifier_concepts = association_proxy("modifiers", "measurement_concept_id")
    modifier_value_concepts = association_proxy("modifiers", "value_as_concept_id")




class ModifierTargetRegistry:
    _targets: dict[int, type] = {}

    @classmethod
    def register(cls, model: type[ModifierTargetMixin]) -> None:
        cls._targets[model.modifier_field_concept_id()] = model

    @classmethod
    def resolve(cls, field_concept_id: int):
        return cls._targets.get(field_concept_id)
    
def resolve_modified_event(
    session,
    modifier,
) -> Optional[ClinicalEvent]:

    target_cls = ModifierTargetRegistry.resolve(
        modifier.modifier_of_field_concept_id
    )
    if not target_cls:
        return None

    return session.get(
        target_cls,
        modifier.modifier_of_event_id,
    )


# class ModifiableTable(Base):
#     """
#     Structural base for OMOP tables that can be modified by
#     Measurements / Observations / Episode_Events.

#     Semantic interpretation of modifier_of_field_concept_id
#     is intentionally deferred.
#     """

#     __abstract__ = True

#     modifier_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
#     modifier_of_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),index=True,nullable=False)
#     modifiers: so.Mapped[List["Measurement"]] = so.relationship(backref="modifying_object",lazy="selectin",viewonly=True)
#     related_obs: so.Mapped[List["Observation"]] = so.relationship(backref="observing_object",lazy="selectin",viewonly=True)
#     related_events: so.Mapped[List["Episode_Event"]] = so.relationship(backref="event_object",lazy="selectin",viewonly=True)
#     modifier_concepts = association_proxy("modifiers", "measurement_concept_id")
#     modifier_value_concepts = association_proxy("modifiers", "value_as_concept_id")

#     _polymorphic_map_: ClassVar[Dict[int, str]] = {}

#     __mapper_args__ = {
#         "polymorphic_on": sa.case(
#             (modifier_of_field_concept_id == ModifierFieldConcepts.CONDITION_OCCURRENCE, "condition"),
#             (modifier_of_field_concept_id == ModifierFieldConcepts.PROCEDURE_OCCURRENCE, "procedure"),
#             (modifier_of_field_concept_id == ModifierFieldConcepts.DRUG_EXPOSURE, "drug_exposure"),
#             else_="episode"
#         ),
#         "polymorphic_identity": "measurement"
#     }

#     @property
#     def polymorphic_label(self) -> str:
#         return self.__mapper_args__.get("polymorphic_identity", "unknown")
