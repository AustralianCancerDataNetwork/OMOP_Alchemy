import sqlalchemy.orm as so
import sqlalchemy as sa
import pandas as pd
import json
import hashlib
from pathlib import Path
from typing import Type, Any, Tuple, cast

from ..registry import ValidationIssue
from ..specification import TableSpec, FieldSpec
from ..utils import CDMValidationError, get_logger
from .file_helpers import load_by_chunk, normalise_csv_to_model, dedupe_csv, _json_default
from .typing import HasTableName

logger = get_logger(__name__)

class IdAllocator:
    """
    This class is used instead of database sequences, due to requirement for supporting
    SQLite which does not have sequence support.

    ID allocation assumes a single writer per table.
    Concurrent writers will fail fast with PK violations.
    """
    def __init__(self, start: int):
        self._next = start + 1

    def next(self) -> int:
        val = self._next
        self._next += 1
        return val

    def reserve(self, n: int) -> range:
        start = self._next
        self._next += n
        return range(start, start + n)


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
    def mapper_for(cls: Type) -> so.Mapper:
        return cast(so.Mapper, sa.inspect(cls))

    @classmethod
    def pk_columns(cls) -> list[sa.Column]:
        pks = list(cls.mapper_for().primary_key)
        if not pks:
            raise ValueError(f"{cls.__name__} has no primary key")
        return pks # type: ignore
    
    @classmethod
    def pk_values(cls, obj: Any) -> dict[str, Any]:
        """
        Return primary key values as a dict:
        {"person_id": 123, "visit_occurrence_id": 456}
        """
        return {
            col.key: getattr(obj, col.key)
            for col in cls.pk_columns()
        }

    @classmethod
    def pk_tuple(cls, obj: Any) -> Tuple[Any, ...]:
        """
        Return PK values as a positional tuple in PK order.
        Suitable for:
        - set membership
        - dedupe
        - tuple_(*cols).in_(...)
        """
        return tuple(
            getattr(obj, col.key)
            for col in cls.pk_columns()
        )

    @classmethod
    def pk_names(cls) -> list[str]:
        return [c.key for c in cls.pk_columns()]

    @classmethod
    def allocator(cls, session) -> IdAllocator:
        start = cls.max_id(session)
        return IdAllocator(start)

    @classmethod
    def max_id(cls, session) -> int:
        pks = cls.pk_columns()
        if len(pks) != 1:
            raise ValueError(f"{cls.__name__} has composite PK; max_id() not supported")
        pk = pks[0]
        return session.query(sa.func.max(pk)).scalar() or 0

    @classmethod
    def next_id(cls, session) -> int:
        return cls.max_id(session) + 1

    @classmethod
    def load_csv(
        cls: Type[HasTableName],
        session: so.Session,
        path: Path,
        *,
        strict: bool = True,
        delimiter: str = "\t",
        dedupe: bool = False,
        normalise: bool = True,
        chunk_size: int = 10_000,
    ) -> int:
        
        logger.debug(f'Loading csv file for {cls.__tablename__}')
        if path.stem.lower() != cls.__tablename__:
            raise ValueError(f"CSV filename '{path.name}' does not match table '{cls.__tablename__}'")

        mapper = cls.mapper_for()
        model_columns: dict[str, sa.Column] = {c.key: c for c in mapper.columns}
        pk_names = cls.pk_names()
        df = pd.read_csv(path, delimiter=delimiter, dtype=str)

        if strict:
            logger.debug(f'Checking csv file against model for {cls.__tablename__}')
            unknown = set(df.columns) - set(model_columns)
            if unknown:
                raise ValueError(f"Unknown columns in file: {cls.__tablename__}: {unknown}")
            missing = set(model_columns) - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns in {cls.__tablename__}: {missing}")

        df = df[[c for c in df.columns if c in model_columns]]

        if normalise:
            df = normalise_csv_to_model(model_columns, df, cls)
        if dedupe:
            df = dedupe_csv(df, pk_names, cls, session)

        total = load_by_chunk(
            cls=cls,
            session=session,
            dataframe=df,
            chunk_size=chunk_size,
        )
        logger.info("%s: inserted %d rows from CSV",cls.__tablename__,total)
        return total

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
            
    
    def to_dict(
        self,
        *,
        include_nulls: bool = False,
        only: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, Any]:
        mapper = self.mapper_for()

        data = {}
        for col in mapper.columns:
            key = col.key
            if only and key not in only:
                continue
            if exclude and key in exclude:
                continue

            value = getattr(self, key)
            if value is None and not include_nulls:
                continue

            data[key] = value

        return data
    

    def to_json(self, **kwargs) -> str:
        return json.dumps(
            self.to_dict(**kwargs),
            default=_json_default,
            sort_keys=True,
        )
    
    def row_fingerprint(self) -> str:
        payload = self.to_json(include_nulls=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()