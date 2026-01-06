import pandas as pd
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Type
from collections import defaultdict
import sqlalchemy as sa
import sqlalchemy.orm as so

from .typing import HasTableName
from ..utils import CDMValidationError, perform_cast, get_logger

logger = get_logger(__name__)

def _json_default(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Not JSON serializable: {type(obj)}")


def iter_chunks(df: pd.DataFrame, size: int):
    for start in range(0, len(df), size):
        yield df.iloc[start : start + size]

def load_by_chunk(
    cls: Type[HasTableName],
    session: so.Session,
    dataframe: pd.DataFrame,
    chunk_size: int = 10_000) -> int:

    total = 0

    for chunk in iter_chunks(dataframe, chunk_size):
        records = chunk.to_dict(orient="records")
        session.execute(
            sa.insert(cls.__table__),
            records
        )
        total += len(records)
    return total


@dataclass
class CastingStats:
    count: int = 0
    examples: defaultdict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )
    def record(self, value: str, col_name: str, example_limit: int = 3):
        self.count += 1
        if len(self.examples[col_name]) < example_limit:
            self.examples[col_name].append(value)

def normalise_csv_to_model(model_columns: dict[str, sa.Column], df: pd.DataFrame, cls: Type[HasTableName]) -> pd.DataFrame:
    if df.empty:
        return df
    
    cast_errors: dict[str, CastingStats] = defaultdict(CastingStats)

    for col, sa_col in model_columns.items():
        if col not in df.columns:
                    continue
        
        def _on_cast_error(value: str, *, _col_type=f'{sa_col.type}', _col=col):
            cast_errors[_col_type].record(value, _col)

        df[col] = df[col].map(lambda v: perform_cast(v, sa_col.type))#, on_cast_error=_on_cast_error))

    required_cols = [name for name, col in model_columns.items() if not col.nullable]
    for c in required_cols:
        null_mask = df[c].isna()
        null_count = int(null_mask.sum())
        if null_count > 0:
            logger.warning(f"Found {null_count} rows with unexpected nulls in {cls.__tablename__}.{c}")

    null_mask = df[required_cols].isna().any(axis=1)
    df = df.loc[~null_mask]

    for error_type, details in cast_errors.items():
        for col, stats in details.items():
            logger.warning(f"{error_type.upper()} {cls.__tablename__}.{col}: cast issue with {stats.count} row(s). Examples: {stats.examples}")
    return df
                
def dedupe_csv(df: pd.DataFrame, pk_names: list[str], cls: Type[HasTableName], session: so.Session, max_bind_vars=1_000) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=pk_names, keep="first")
    dropped = before - len(df)
    logger.info(f'Dropping {dropped} duplicates from csv file for table {cls.__tablename__}')
    pk_tuples = list(df[pk_names].itertuples(index=False, name=None))
    pk_cols = [getattr(cls, c) for c in pk_names]

    vars_per_row = len(pk_cols)
    chunk_size = max_bind_vars // vars_per_row
    existing_rows: list[tuple] = []
    for i in range(0, len(pk_tuples), chunk_size):
        chunk = pk_tuples[i : i + chunk_size]

        rows = (
            session.query(*pk_cols)
            .filter(sa.tuple_(*pk_cols).in_(chunk))
            .all()
        )
        existing_rows.extend(rows)

    if not existing_rows:
        return df

    existing = pd.DataFrame(existing_rows, columns=pk_names)

    if len(existing) > 0:
        logger.warning(f'{len(existing)} duplicate records in csv file {cls.__tablename__} will be dropped')
        df = df.merge(
            existing,
            on=pk_names,
            how="left",
            indicator=True,
        )
        df = df[df["_merge"] == "left_only"].drop(columns="_merge")
    return df