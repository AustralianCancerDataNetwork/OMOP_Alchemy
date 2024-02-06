import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from typing import List, Optional
from datetime import datetime
from ..conventions import Modality
from ...db import Base


class Episode_Event(Base):
    __tablename__ = 'episode_event'
    # identifier
    episode_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("episode.episode_id"), primary_key=True)
    event_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    event_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), primary_key=True)