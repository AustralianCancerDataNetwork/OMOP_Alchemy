from typing import Type, TypeVar
from .cdm_table_base import CDMTableBase

T = TypeVar("T", bound=type)

def cdm_table(cls: T) -> T:
    """
    Mark a SQLAlchemy declarative class as a concrete OMOP CDM table.

    - Forces __abstract__ = False
    - Ensures __tablename__ is defined
    - Inherits from CDMTableBase
    - Used to clearly distinguish real CDM tables from mixins
    """

    # Safety: require an explicit tablename
    if not hasattr(cls, "__tablename__"):
        raise TypeError(
            f"@cdm_table applied to {cls.__name__} "
            "but __tablename__ is not defined"
        )

    if not issubclass(cls, CDMTableBase):
        raise TypeError(
            f"{cls.__name__} must inherit from CDMTableBase "
        )

    # Explicitly mark as concrete
    cls.__abstract__ = False

    return cls
