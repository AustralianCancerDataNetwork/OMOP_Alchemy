import sqlalchemy.orm as so
import sqlalchemy as sa
from typing import TYPE_CHECKING, Type, Optional

from omop_alchemy.cdm.base.declarative import Base
from omop_alchemy.cdm.base.typing import HasTableName
from omop_alchemy.cdm.utils import CDMValidationError
from omop_alchemy.cdm.specification import TableSpec, FieldSpec

if TYPE_CHECKING:
    from .registry import ValidationIssue

CDM_TO_SA = {
    "integer": {"integer", "biginteger"},
    "float": {"float", "numeric", "decimal", "real"},
    "date": {"date"},
    "datetime": {"datetime"},
    "varchar": {"string", "varchar", "unicode", "unicodetext", "text"}
}

def normalize_omop_type(raw: str) -> str:
    raw = raw.lower()
    if raw.startswith("varchar"):
        return "varchar"
    if raw in {"integer", "int"}:
        return "integer"
    return raw

def _sa_type_name(coltype: sa.types.TypeEngine) -> str:
    return coltype.__class__.__name__.lower()

def is_type_compatible(cdm_datatype: str, sa_col: sa.Column) -> bool:
    dt = cdm_datatype.strip().lower()
    sa_name = _sa_type_name(sa_col.type)

    # Normalize varchar(N) to generic varchar
    if dt.startswith("varchar(") and dt not in CDM_TO_SA:
        dt = "varchar"

    allowed = CDM_TO_SA.get(dt)
    if not allowed:
        # unknown in map: treat as warning elsewhere
        return True
    return sa_name in allowed

def varchar_length(sa_col: sa.Column) -> Optional[int]:
    if hasattr(sa_col.type, "length"):
        return sa_col.type.length # type: ignore
    return None

def validate_primary_key(table: sa.Table, spec_pk_fields: set[str]) -> tuple[bool, set[str], set[str]]:
    actual_pk = {c.name for c in table.primary_key.columns}
    missing = spec_pk_fields - actual_pk
    extra = actual_pk - spec_pk_fields
    ok = not missing and not extra
    return ok, missing, extra

def fk_targets(sa_col: sa.Column) -> set[tuple[str, str]]:
    targets: set[tuple[str, str]] = set()
    for fk in sa_col.foreign_keys:
        # fk.target_fullname like "concept.concept_id"
        t = fk.target_fullname.split(".")
        if len(t) == 2:
            targets.add((t[0], t[1]))
    return targets

class CDMValidatedTableMixin:
    """
    Adds structural validation of ORM models against
    the official OMOP CDM CSV specifications.
    """
    __cdm_extra_checks__: list[str] = []


    @classmethod
    def extra_validate(cls) -> list["ValidationIssue"]:
        return []

    @classmethod
    def validate_against_spec(
        cls: Type[HasTableName],
        *,
        table_spec: TableSpec,
        field_specs: dict[str, FieldSpec],
    ) -> None:
        if cls.__tablename__ != table_spec.table_name:
            raise CDMValidationError(
                f"{cls.__name__}: table name mismatch "
                f"({cls.__tablename__} != {table_spec.table_name})"
            )

        mapper = sa.inspect(cls)

        if not mapper:
            raise CDMValidationError(f"{cls.__name__} is not a mapped ORM class")
    
        orm_columns = {c.key.lower() for c in mapper.columns}

        # Required fields must exist
        for fname, spec in field_specs.items():
            if spec.is_required and fname not in orm_columns:
                raise CDMValidationError(
                    f"{cls.__tablename__}: missing required column '{fname}'"
                )