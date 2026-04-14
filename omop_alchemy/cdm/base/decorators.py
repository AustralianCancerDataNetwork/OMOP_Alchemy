from typing import Type, TypeVar
from .cdm_table_base import CDMTableBase

T = TypeVar("T", bound=type)
MODEL_MODULE_PREFIX = "omop_alchemy.cdm.model."


def _infer_table_category(cls: type) -> str | None:
    model_module = cls.__module__
    if not model_module.startswith(MODEL_MODULE_PREFIX):
        return None
    suffix = model_module.removeprefix(MODEL_MODULE_PREFIX)
    return suffix.split(".", 1)[0]


def _infer_table_schema(cls: type) -> str | None:
    for base in cls.__mro__[1:]:
        schema_name = getattr(base, "__omop_schema__", None)
        if schema_name is not None:
            return schema_name
    return None

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
    cls.__omop_is_cdm_table__ = True
    cls.__omop_table_category__ = _infer_table_category(cls)

    schema_name = _infer_table_schema(cls)
    table = getattr(cls, "__table__", None)
    if schema_name is not None and table is not None and table.schema is None:
        table.schema = schema_name

    return cls
