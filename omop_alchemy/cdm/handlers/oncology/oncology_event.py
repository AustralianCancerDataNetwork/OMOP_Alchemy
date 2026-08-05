from __future__ import annotations

from typing import Any, cast

import sqlalchemy.orm as so
from sqlalchemy.ext.declarative import declared_attr

from omop_alchemy.cdm.base import ModifierFieldConcepts
from omop_alchemy.cdm.model.structural.episode_event import Episode_EventView

from .oncology_drug_exposure import OncologyDrugExposure
from .oncology_procedure_occurrence import OncologyProcedure


class OncologyEpisodeEvent(Episode_EventView):
    """
    Episode-event view that prefers oncology-aware fact table views.

    Non-oncology targets continue to use the base resolver map from
    ``Episode_EventView``. Procedure and drug targets resolve to oncology
    subclasses so episode traversal can ask domain-specific questions such as
    ``is_radiotherapy`` and ``is_sact``.
    """

    @classmethod
    def resolved_event_target_classes(cls) -> dict[int, type[Any]]:
        base_resolver = getattr(Episode_EventView, "resolved_event_target_classes")
        targets = dict(base_resolver())
        targets.update(
            {
                ModifierFieldConcepts.PROCEDURE_OCCURRENCE: OncologyProcedure,
                ModifierFieldConcepts.DRUG_EXPOSURE: OncologyDrugExposure,
            }
        )
        return targets


class OncologyEpisodeEventMixin:
    """
    Relationship override for episode classes that need oncology event targets.
    """

    @declared_attr
    @classmethod
    def episode_events(cls) -> so.Mapped[list["OncologyEpisodeEvent"]]:
        owner_episode_id = cast(Any, cls).__table__.c.episode_id
        event_episode_id = OncologyEpisodeEvent.__table__.c.episode_id
        return so.relationship(
            "OncologyEpisodeEvent",
            primaryjoin=lambda: owner_episode_id == event_episode_id,
            viewonly=True,
            lazy="selectin",
        )
