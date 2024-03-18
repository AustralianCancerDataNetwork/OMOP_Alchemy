from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property

from .modifiable_table import Modifiable_Table
from ...db import Base

class Condition_Occurrence(Modifiable_Table):
    __tablename__ = 'condition_occurrence'
    validators = {}
    
    def __init__(self, 
                 person_id,
                 condition_start_date,
                 condition_concept_id,
                 condition_type_concept_id,
                 condition_start_datetime=None,
                 condition_end_date=None,
                 condition_end_datetime=None,
                 stop_reason=None,
                 condition_source_value=None,
                 condition_status_source_value=None,
                 provider_id=None,
                 visit_occurrence_id=None,
                 visit_detail_id=None,
                 condition_status_concept_id=None,
                 visit_detail=None,
                 *args, 
                 **kwargs):
        condition_start_datetime = condition_start_datetime or datetime.combine(condition_start_date, datetime.min.time())
        super().__init__(person_id=person_id,
                         condition_start_date=condition_start_date,
                         condition_concept_id=condition_concept_id,
                         condition_type_concept_id=condition_type_concept_id,
                         condition_start_datetime=condition_start_datetime,
                         condition_end_date=condition_end_date,
                         condition_end_datetime=condition_end_datetime,
                         stop_reason=stop_reason,
                         condition_source_value=condition_source_value,
                         condition_status_source_value=condition_status_source_value,
                         provider_id=provider_id,
                         visit_occurrence_id=visit_occurrence_id,
                         visit_detail_id=visit_detail_id,
                         condition_status_concept_id=condition_status_concept_id,
                         visit_detail=visit_detail,
                         *args, **kwargs)


    @so.reconstructor
    def init_on_load(self):
        # TODO: need to create the class functions and the setter functions for this
        # Consider what happens if a new measurement is added - if this is fired on loading of
        # condition object, will addition of a new measurement be enough to trigger this update?
        # Probably not?
        self._p = False
        self._t, self._n, self._m, self._tnm = [], [], [], []
        for m in sorted(self.modifiers, key=lambda mod: mod.measurement_date):
            if not self._p and m.measurement_concept_id in self.validators['tnm'].path_stage_concepts:
                self._p=(m.measurement_date - self.condition_start_date).days < 30
            if m.measurement_concept_id in self.validators['tnm'].t_stage_concepts:
                self._t.append(m)
            elif m.measurement_concept_id in self.validators['tnm'].n_stage_concepts:
                self._n.append(m)
            elif m.measurement_concept_id in self.validators['tnm'].m_stage_concepts:
                self._m.append(m)
            elif m.measurement_concept_id in self.validators['tnm'].group_stage_concepts:
                self._tnm.append(m)

    # identifier
    condition_occurrence_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('modifiable_table.modifier_id'), primary_key=True, autoincrement=True)
    # temporal
    condition_start_date: so.Mapped[date] = so.mapped_column(sa.Date)
    condition_start_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    condition_end_date: so.Mapped[Optional[date]] = so.mapped_column(sa.Date, nullable=True)
    condition_end_datetime: so.Mapped[Optional[datetime]] = so.mapped_column(sa.DateTime, nullable=True)
    # strings
    stop_reason: so.Mapped[Optional[str]] = so.mapped_column(sa.String(20), nullable=True)
    condition_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    condition_status_source_value: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    # fks
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id', name='co_fk_1'))
    provider_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('provider.provider_id', name='co_fk_2'), nullable=True)
    visit_occurrence_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_occurrence.visit_occurrence_id', name='co_fk_3'), nullable=True)
    visit_detail_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('visit_detail.visit_detail_id', name='co_fk_4'), nullable=True)
    # concept fks
    condition_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_5'))
    condition_type_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_6'))
    condition_status_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_7'), nullable=True)
    condition_source_concept_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('concept.concept_id', name='co_fk_8'), nullable=True)
    # relationships
    person: so.Mapped['Person'] = so.relationship(foreign_keys=[person_id])
    provider: so.Mapped[Optional['Provider']] = so.relationship(foreign_keys=[provider_id])
    visit_occurrence: so.Mapped[Optional['Visit_Occurrence']] = so.relationship(foreign_keys=[visit_occurrence_id])
    visit_detail: so.Mapped[Optional['Visit_Detail']] = so.relationship(foreign_keys=[visit_detail_id])
    # concept relationships
    condition_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_concept_id])
    condition_type_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_type_concept_id])
    condition_status_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_status_concept_id])
    condition_source_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[condition_source_concept_id])

    __mapper_args__ = {
        "polymorphic_identity": "condition",
        'inherit_condition': (condition_occurrence_id == Modifiable_Table.modifier_id)
    }

    @classmethod
    def set_validators(cls):
        # putting this here so that we can defer the import until after the models have all been instantiated, otherwise 
        # it tries to query the concepts from non-existent tables - there may be a better way?
        from ...conventions.vocab_lookups import tnm_lookup
        cls.validators = {'tnm': tnm_lookup}

    @hybrid_property
    def condition_label(self):
        if self.condition_concept:
            return self.condition_concept.concept_name
        
    @condition_label.expression
    def _condition_label_expression(cls) -> sa.ColumnElement[Optional[str]]:
        return sa.cast("SQLColumnExpression[Optional[str]]", cls.condition_concept.concept_name)

    # TODO: Down the line we should consider if all of these oncology-extension-specific properties need to be refactored out into a subclass?

    @hybrid_property
    def path_confirmation(self):
        # returns true is any of the stage modifiers of this condition are of type pathological
        try:
            return self._p
        except:
            return False

    @hybrid_property
    def group_stage(self):
        # returns group stage modifiers of this condition, ordered by modifier date - at this point just returns earliest
        # TODO: do we want to accommodate most recent as well as first staging? 
        try:
            return self._tnm[0]
        except:
            return None

    @hybrid_property
    def t_stage(self):
        try:
            return self._t[0]
        except:
            return None
            
    @hybrid_property
    def n_stage(self):        
        try:
            return self._n[0]
        except:
            return None

    @hybrid_property
    def m_stage(self):
        try:
            return self._m[0]
        except:
            return None
    
    @hybrid_property
    def event_date(self):
        return  self.condition_start_datetime.date() if self.condition_start_datetime is not None else self.condition_start_date

