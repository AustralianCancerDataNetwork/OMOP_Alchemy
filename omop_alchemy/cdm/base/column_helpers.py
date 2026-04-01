import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional

def required_concept_fk(*, index: bool = False):
    """
    *required_concept_fk*

    OMOP-required concept foreign key.

    This pattern is used when the CDM requires a concept reference,
    but allows an explicit “unknown” value (`concept_id = 0`).

    Semantics:

    - Must exist
    - Unknown allowed (concept_id = 0)
    - Matches CDM Field-Level spec
    - foreign key to `concept.concept_id`
    - indexing is opt-in when explicitly requested

    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=False,
        default=0,
        index=index,
    )

def optional_concept_fk(*, index: bool = False):
    """
    *optional_concept_fk*

    Used when a concept reference is genuinely optional.
    
    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=True,
        index=index,
    )

def optional_fk(target: str, *, index: bool = False):
    """
    *optional_fk*
    
    Optional foreign keys to non-concept tables.

    """
    return so.mapped_column(
        sa.ForeignKey(target),
        nullable=True,
        index=index,
    )

def required_int():
    """
    *required_int*

    Required integer column.
    """
    return so.mapped_column(sa.Integer, nullable=False)

def optional_int():
    """
    *optional_int*

    Optional integer column.
    """
    return so.mapped_column(sa.Integer, nullable=True)
