import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional

def required_concept_fk(*, index: bool = True):
    """
    OMOP-required concept foreign key.

    Semantics:
    - Must exist
    - Unknown allowed (concept_id = 0)
    - Matches CDM Field-Level spec
    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=False,
        default=0,
        index=index,
    )

def optional_concept_fk(*, index: bool = False):
    """
    Optional concept foreign key.
    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=True,
        index=index,
    )

def optional_fk(target: str, *, index: bool = False):
    return so.mapped_column(
        sa.ForeignKey(target),
        nullable=True,
        index=index,
    )

def required_int():
    return so.mapped_column(sa.Integer, nullable=False)

def optional_int():
    return so.mapped_column(sa.Integer, nullable=True)
