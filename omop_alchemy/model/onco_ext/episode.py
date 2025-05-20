import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from typing import List, Optional
from datetime import datetime, date
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy

from ...conventions import Modality, DiseaseEpisodeConcepts, TreatmentEpisode
from ...conventions.concept_enumerators import ModifierFields

from ...db import Base
from ..clinical.measurement import Measurement
from ..clinical.modifiable_table import Modifiable_Table
from ..concept_links import Concept_Links


class Episode(Modifiable_Table, Concept_Links):
    __tablename__ = 'episode'
    labels = {'episode': False, 'episode_object': False, 'episode_type': False, 'episode_source': False}

    # identifier
    episode_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'), primary_key=True, autoincrement=True)

    # temporal
    episode_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    episode_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    episode_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    episode_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)

    # strings
    episode_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # numeric
    episode_number: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("person.person_id"))
    episode_parent_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey("episode.episode_id"), nullable=True)
    # relationships
    person_object: so.Mapped['Person'] = so.relationship(back_populates="episodes", foreign_keys=[person_id])
    events: so.Mapped[List['Episode_Event']] = so.relationship(back_populates="episode_object", lazy="selectin")

    episode_parent_object: so.Mapped[Optional['Episode']] = so.relationship(foreign_keys=[episode_parent_id], remote_side=episode_id)
    children: so.Mapped[List['Episode']] = so.relationship(back_populates="episode_parent_object", lazy="selectin", foreign_keys=[episode_parent_id])

    __mapper_args__ = {
        "polymorphic_identity": "episode",
        'inherit_condition': (episode_id == Modifiable_Table.modifier_id)
    }

    def __init__(self, 
                 episode_concept_id=DiseaseEpisodeConcepts.episode_of_care.value, 
                 episode_type_concept_id=None,
                 episode_parent_id=None,
                 *args, 
                 **kwargs):
        # TODO: removing for now, as it is required for bulk loads if there's an arbitrary depth of parent/child
        # TODO: can this check be put somewhere else, made optional, supported thru limitation of depth instead?
        # if episode_concept_id != DiseaseEpisodeConcepts.episode_of_care.value:
        #     if episode_parent_id is None:
        #         raise ValueError('All episodes other than top level episode of care must have a parent value')
        super().__init__(episode_concept_id=episode_concept_id, 
                         episode_type_concept_id=episode_type_concept_id,
                         episode_parent_id=episode_parent_id, 
                         modifier_of_field_concept_id = ModifierFields.episode_id.value,
                         *args, **kwargs)
    

    def __repr__(self):
        ep_type = 'Treatment ' if self.is_tx else 'Diagnostic '
        return f'{ep_type} Episode: episode_id = {self.episode_id}'
    
    @property 
    def primary_ep(self):
        # todo: recursion risk - check depth on assignment?
        if self.is_overarching:
            return self.episode_id
        if self.episode_parent_object:
            return self.episode_parent_object.primary_ep

    @hybrid_property
    def is_overarching(self):
        return self.episode_concept_id == DiseaseEpisodeConcepts.episode_of_care.value

    @is_overarching.expression
    def is_overarching(cls):
        return cls.episode_concept_id == DiseaseEpisodeConcepts.episode_of_care.value
    
    @property
    def is_tx(self):
        return self.episode_concept_id in TreatmentEpisode.member_values()
        
    @property
    def is_dx(self):
        return self.episode_concept_id in DiseaseEpisodeConcepts.member_values()
    
    @property
    def event_date(self):
        return  self.episode_start_datetime.date()


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


