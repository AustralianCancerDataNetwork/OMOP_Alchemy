from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
import sqlalchemy as sa
import sqlalchemy.orm as so

# TODO: refactor this out of the clinical subfolder - should be in the parent tables folder?

class Concept_Links():
    labels = {}

    @classmethod
    def add_concepts(cls):
        for label, opt in cls.labels.items():
            # TODO: check that this is handling optional fields properly...
            so.add_mapped_attribute(cls, f'{label}_concept_id', so.mapped_column(sa.Integer, sa.ForeignKey('concept.concept_id', name=f'fk_{cls.__name__[:5].lower()}_{label[:15]}'), nullable=not opt))
            so.add_mapped_attribute(cls, f'{label}_concept', so.relationship("Concept", primaryjoin=f"{cls.__tablename__}.c.{label}_concept_id==Concept.concept_id"))
