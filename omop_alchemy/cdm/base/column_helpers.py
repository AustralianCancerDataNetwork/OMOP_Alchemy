import sqlalchemy as sa
import sqlalchemy.orm as so

def required_concept_fk():
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

    To index this column, add an explicit `omop_index(...)` to the
    model's `__table_args__` rather than indexing the column directly —
    see `omop_alchemy.cdm.base.indexing`.

    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=False,
        default=0,
    )

def optional_concept_fk():
    """
    *optional_concept_fk*

    Used when a concept reference is genuinely optional.

    To index this column, add an explicit `omop_index(...)` to the
    model's `__table_args__` rather than indexing the column directly —
    see `omop_alchemy.cdm.base.indexing`.

    """
    return so.mapped_column(
        sa.ForeignKey("concept.concept_id"),
        nullable=True,
    )

def optional_fk(target: str):
    """
    *optional_fk*

    Optional foreign keys to non-concept tables.

    To index this column, add an explicit `omop_index(...)` to the
    model's `__table_args__` rather than indexing the column directly —
    see `omop_alchemy.cdm.base.indexing`.

    """
    return so.mapped_column(
        sa.ForeignKey(target),
        nullable=True,
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
