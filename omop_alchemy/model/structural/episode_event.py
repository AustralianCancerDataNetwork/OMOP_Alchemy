import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.declarative import declared_attr
from typing import Optional, TYPE_CHECKING, List
from datetime import date
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm.exc import DetachedInstanceError

from omop_alchemy.cdm.base import (
    Base, 
    cdm_table,
    CDMTableBase, 
    required_concept_fk,
    optional_concept_fk,
    PersonScoped,
    optional_int,
    ReferenceContextMixin,
    DomainValidationMixin,
    ExpectedDomain,
)

if TYPE_CHECKING:
    from ..vocabulary import Concept
    from .episode import Episode

@cdm_table
class Episode_Event(CDMTableBase, Base):
    __tablename__ = "episode_event"

    episode_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("episode.episode_id"),nullable=False,primary_key=True)
    event_id: so.Mapped[int] = so.mapped_column(nullable=False,primary_key=True)
    episode_event_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False,primary_key=True)

    def __repr__(self) -> str:
        return f"<EpisodeEvent ep={self.episode_id} event={self.event_id}>"
    
class Episode_EventContext(ReferenceContextMixin):
    episode: so.Mapped["Episode"] = ReferenceContextMixin._reference_relationship(target="Episode",local_fk="episode_id",remote_pk="episode_id",)  # type: ignore[assignment]
    event_field: so.Mapped["Concept"] = ReferenceContextMixin._reference_relationship(target="Concept",local_fk="episode_event_field_concept_id",remote_pk="concept_id",)  # type: ignore[assignment]

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
