

import csv
import sqlalchemy.orm as so
import sqlalchemy as sa
from pathlib import Path
from typing import Type

from omop_alchemy.cdm.utils import CDMValidationError, perform_cast
from omop_alchemy.cdm.registry import ValidationIssue
from omop_alchemy.cdm.specification import TableSpec, FieldSpec
from .typing import HasTableName


class CDMTableBase:
    """
    Base class for CDM tables that support CSV loading and validation.
    """
    __abstract__ = True

    """
    Adds structural validation of ORM models against
    the official OMOP CDM CSV specifications.
    """
    __cdm_extra_checks__: list[str] = []

    @classmethod
    def load_csv(
        cls: Type[HasTableName],
        session: so.Session,
        path: Path,
        *,
        strict: bool = True,
        delimiter: str = "\t",
    ) -> int:
        if path.stem.lower() != cls.__tablename__:
            raise ValueError(
                f"CSV filename '{path.name}' does not match table '{cls.__tablename__}'"
            )

        mapper = sa.inspect(cls)
        model_columns = {c.key: c for c in mapper.columns} # type: ignore

        rows = []
        with path.open() as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                for key, value in row.items():
                    if key not in model_columns:
                        if strict:
                            raise ValueError(
                                f"Column '{key}' not found in table '{cls.__tablename__}'"
                            )
                        else:
                            continue
                    column = model_columns[key]
                    row[key] = perform_cast(value, column.type)
                rows.append(cls(**row))

        session.add_all(rows)
        return len(rows)


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