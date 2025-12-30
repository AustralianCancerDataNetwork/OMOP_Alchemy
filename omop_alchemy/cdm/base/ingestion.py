import csv
from pathlib import Path
from typing import Iterable, Type
import sqlalchemy.orm as so
import sqlalchemy as sa
from .typing import HasTableName
from omop_alchemy.cdm.utils.type_management import perform_cast

class CSVSourceMixin:
    """
    Load a CDM table from a CSV file whose name matches the table.
    """

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

