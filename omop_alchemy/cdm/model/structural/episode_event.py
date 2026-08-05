import sqlalchemy as sa
import sqlalchemy.orm as so
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Type
from functools import cached_property
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import (
    cdm_table,
    CDMTableBase, 
    ReferenceContext,
    DomainValidationMixin,
    ExpectedDomain,
    ModifierFieldConcepts,
    ModifierTargetMixin,
    merge_table_args,
    omop_index,
)

if TYPE_CHECKING:
    from ..vocabulary import Concept
    from .episode import Episode

ResolutionDiagnosticKind = Literal[
    "unrecognized_field_concept",
    "unmapped_field_concept",
    "dangling_event",
]


@dataclass(frozen=True)
class EpisodeEventResolutionDiagnostic:
    """
    Advisory detail for an episode_event row whose target cannot be resolved.

    Resolution remains best-effort: unresolved rows still return ``None`` from
    ``resolved_event``. These diagnostics give maintenance and QA code enough
    information to distinguish miscoded field concepts from harmless ORM
    coverage gaps and genuine dangling event references.
    """

    kind: ResolutionDiagnosticKind
    episode_id: int
    event_id: int
    episode_event_field_concept_id: int
    message: str


def _known_modifier_field_concept_ids() -> set[int]:
    return {
        value
        for name, value in vars(ModifierFieldConcepts).items()
        if name.isupper() and isinstance(value, int)
    }


def _modifier_target_classes_by_field_concept_id() -> dict[int, Type[Any]]:
    classes: dict[int, Type[Any]] = {}
    for mapper in Base.registry.mappers:
        cls = mapper.class_
        if not issubclass(cls, ModifierTargetMixin):
            continue
        try:
            field_concept_id = cls.modifier_field_concept_id()
        except NotImplementedError:
            continue
        if field_concept_id not in classes:
            classes[field_concept_id] = cls
    return classes


class _EpisodeEventTargetClassCache:
    def __init__(self) -> None:
        self._cache: dict[int, Type[Any]] | None = None

    def get(self) -> dict[int, Type[Any]]:
        if self._cache is None:
            self._cache = _modifier_target_classes_by_field_concept_id()
        return self._cache

    def clear(self) -> None:
        self._cache = None


_target_class_cache = _EpisodeEventTargetClassCache()


def clear_episode_event_target_class_cache() -> None:
    """
    Clear cached episode_event field-concept target mappings.

    Most applications import all mapped classes before resolving episode events.
    Long-running processes that register additional modifier-target classes
    later can call this before resolving through the new class surface.
    """
    _target_class_cache.clear()


@cdm_table
class Episode_Event(CDMTableBase, Base):
    __tablename__ = "episode_event"
    __table_args__ = merge_table_args(
        omop_index(__tablename__, "episode_id", cluster=True),
        omop_index(__tablename__, "episode_event_field_concept_id"),
    )

    episode_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("episode.episode_id"),nullable=False,primary_key=True)
    event_id: so.Mapped[int] = so.mapped_column(nullable=False,primary_key=True)
    episode_event_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,primary_key=True)

    def __repr__(self) -> str:
        return f"<EpisodeEvent ep={self.episode_id} event={self.event_id}>"
    
class Episode_EventContext(ReferenceContext):
    episode: so.Mapped["Episode"] = ReferenceContext._reference_relationship(target="Episode",local_fk="episode_id",remote_pk="episode_id",)  # type: ignore[assignment]
    event_field: so.Mapped["Concept"] = ReferenceContext._reference_relationship(target="Concept",local_fk="episode_event_field_concept_id",remote_pk="concept_id",)  # type: ignore[assignment]

class Episode_EventView(Episode_Event, Episode_EventContext, DomainValidationMixin):
    """
    Episode ↔ Event linkage view.

    Identifies which CDM table the EVENT_ID comes from via
    episode_event_field_concept_id.
    """

    __tablename__ = "episode_event"
    __mapper_args__ = {"concrete": False}

    __expected_domains__ = {
        "episode_event_field_concept_id": ExpectedDomain("Metadata"),
    }

    @classmethod
    def resolved_event_target_classes(cls) -> dict[int, Type[Any]]:
        """
        Map episode_event field concepts to ORM classes that can receive them.

        Subclasses may override this to prefer domain-specific mapped views
        over the base CDM views. The default map is built from registered
        ``ModifierTargetMixin`` classes and keyed by the field concept id
        itself, avoiding the fragile concept-name table parsing that the CDM
        metadata labels happen to support today.
        """
        return _target_class_cache.get()

    @classmethod
    def recognized_field_concept_ids(cls) -> set[int]:
        return _known_modifier_field_concept_ids()

    @property
    def event_table(self) -> str | None:
        if self.event_field and "." in self.event_field.concept_name:
            return self.event_field.concept_name.split(".", 1)[0]
        return None

    @property
    def resolved_event_class(self) -> Type[Any] | None:
        return self.resolved_event_target_classes().get(
            self.episode_event_field_concept_id
        )

    @property
    def event_resolution_diagnostics(self) -> list[EpisodeEventResolutionDiagnostic]:
        session = so.object_session(self)
        field_concept_id = self.episode_event_field_concept_id

        if field_concept_id not in self.recognized_field_concept_ids():
            return [
                EpisodeEventResolutionDiagnostic(
                    kind="unrecognized_field_concept",
                    episode_id=self.episode_id,
                    event_id=self.event_id,
                    episode_event_field_concept_id=field_concept_id,
                    message=(
                        "episode_event_field_concept_id is not a known "
                        "ModifierFieldConcepts value"
                    ),
                )
            ]

        cls = self.resolved_event_class
        if cls is None:
            return [
                EpisodeEventResolutionDiagnostic(
                    kind="unmapped_field_concept",
                    episode_id=self.episode_id,
                    event_id=self.event_id,
                    episode_event_field_concept_id=field_concept_id,
                    message=(
                        "episode_event_field_concept_id is recognized, but no "
                        "registered ORM target class declares it"
                    ),
                )
            ]

        if session is None:
            return []

        if session.get(cls, self.event_id) is None:
            return [
                EpisodeEventResolutionDiagnostic(
                    kind="dangling_event",
                    episode_id=self.episode_id,
                    event_id=self.event_id,
                    episode_event_field_concept_id=field_concept_id,
                    message=(
                        f"episode_event points to {cls.__name__}#{self.event_id}, "
                        "but no matching row exists"
                    ),
                )
            ]

        return []

    @cached_property
    def resolved_event(self) -> Any | None:
        """
        Resolve EVENT_ID to concrete OMOP row.
        Cached per-instance.
        """
        session = so.object_session(self)
        cls = self.resolved_event_class
        if session is None or cls is None:
            return None

        return session.get(cls, self.event_id)

    def __repr__(self):
        target = self.resolved_event
        if target is not None:
            return (
                f"<EpisodeEvent ep={self.episode_id} "
                f"{target.__class__.__name__}#{self.event_id}>"
            )
        return f"<EpisodeEvent ep={self.episode_id} event={self.event_id}>"
    
    @property
    def episode_start_datetime(self):
        return (
            self.episode.episode_start_datetime
            if self.episode else None
        )
    
    @property
    def resolved_event_id_column(self) -> str | None:
        """
        Name of the ID column on the resolved event table.

        Derived from episode_event_field_concept_id metadata.
        
        Example:
            'condition_occurrence.condition_occurrence_id' resolves to 'condition_occurrence_id'
        """
        if self.event_field and "." in self.event_field.concept_name:
            return self.event_field.concept_name.split(".", 1)[1]
        return None
