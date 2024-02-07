import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from typing import List, Optional
from datetime import datetime
from ..conventions import Modality, EpisodeConcepts
from ...db import Base
from ..clinical.measurement import Measurement
from ..clinical.modifiable_table import Modifiable_Table
from ..clinical.concept_links import Concept_Links


class Episode(Base, Concept_Links):
    __tablename__ = 'episode'
    labels = {'episode': False, 'episode_object': False, 'episode_type': False}

    # identifier
    episode_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # temporal
    episode_start_datetime: so.Mapped[datetime] = so.mapped_column(sa.DateTime)
    episode_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime)
    # strings
    episode_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50))
    # numeric
    episode_number: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"))
    episode_parent_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("episode.episode_id"))

    # relationships
    person_object: so.Mapped['Person'] = so.relationship(back_populates="episodes", foreign_keys=[person_id])
    episode_parent_object: so.Mapped['Episode'] = so.relationship(foreign_keys=[episode_parent_id])

    __mapper_args__ = {
        "polymorphic_identity": "episode",
        'inherit_condition': (episode_id == Modifiable_Table.modifier_id)
    }

    def __init__(self, 
                 episode_concept_id=EpisodeConcepts.episode_of_care.value, 
                 episode_type_concept_id=None,
                 episode_parent_id=None,
                 *args, 
                 **kwargs):
        if episode_concept_id != EpisodeConcepts.episode_of_care.value:
            if episode_parent_id is None:
                raise ValueError('All episodes other than top level episode of care must have a parent value')
        super().__init__(episode_concept_id=episode_concept_id, 
                         episode_type_concept_id=episode_type_concept_id,
                         episode_parent_id=episode_parent_id, 
                         *args, **kwargs)
    
    def __repr__(self):
        ep_type = 'Treatment ' if self.is_tx else 'Diagnostic '
        return f'{ep_type} Episode: episode_id = {self.episode_id}'
    
    @property
    def is_overarching(self):
        return self.episode_concept_id == EpisodeConcepts.episode_of_care.value
    
    @property
    def is_tx(self):
        return self.episode_concept_id in [EpisodeConcepts.treatment_regimen.value, EpisodeConcepts.treatment_cycle.value]
        
    @property
    def is_dx(self):
        return self.episode_concept_id in [EpisodeConcepts.disease_first_occurrence.value, EpisodeConcepts.disease_progression.value]
    
    @hybrid_property
    def modality(self):
        if self.is_dx:
            raise ValueError('modality modifier not valid for diagnostic episodes')
        else:
            try:
                return [m for m in self.modifiers if m.measurement_concept_id in Modality.member_values()][0]
            except:
                return None

    @modality.inplace.setter
    def _modality_setter(self, value: Optional[Modality]) -> None:
        assert value in Modality    
        self.modifiers.append(Measurement(person_id = self.person_id,
                                          modified_object = self, 
                                          measurement_concept_id = value.value))


