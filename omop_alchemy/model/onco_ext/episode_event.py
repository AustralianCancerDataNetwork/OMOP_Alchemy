import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from typing import List, Optional
from datetime import datetime
from ...db import Base
from ...conventions.concept_enumerators import ModifierFields


class Episode_Event(Base):
    __tablename__ = 'episode_event'
    # identifier
    episode_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("episode.episode_id", name='ee_fk_2'), primary_key=True)
    event_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id', name='ee_fk_1'), primary_key=True)
    episode_event_field_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='ee_fk_3'), primary_key=True)
    
    episode_object: so.Mapped['Episode'] = so.relationship(foreign_keys=[episode_id])
    event_polymorphic: so.Mapped['Modifiable_Table'] = so.relationship(foreign_keys=[event_id])


    def __init__(self, 
                 episode_id, 
                 event_id,
                 episode_event_field_concept_id):
        if episode_event_field_concept_id == ModifierFields.episode_id.value:
            raise ValueError('Object of type episode cannot be an episode event')
        super().__init__(episode_id=episode_id, 
                         event_id=event_id,
                         episode_event_field_concept_id=episode_event_field_concept_id)