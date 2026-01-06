from typing import Any, Callable
from sqlalchemy import Integer, Float, Boolean, Date, DateTime, String, Text
from datetime import date, datetime 
import math
import re

_NUMERIC_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

_ATHENA_DATE_FORMATS = (
    "%d-%b-%Y",        # 24-AUG-2017 (Athena standard)
    "%Y-%m-%d",        # ISO (sometimes appears)
    "%d/%m/%Y",        # defensive
)

def _parse_date(value: str) -> date | None:
    for fmt in _ATHENA_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_datetime(value: str) -> datetime | None:
    # Try datetime first
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass

    # Fallback to date-only formats + midnight
    d = _parse_date(value)
    if d:
        return datetime.combine(d, datetime.min.time())

    return None

def _to_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "t", "yes", "y", "1"}:
        return True
    if s in {"false", "f", "no", "n", "0"}:
        return False
    return None


def _to_numeric_string(value: str | None) -> str | None:
    if value is None:
        return None

    if not _NUMERIC_RE.match(value):
        return value  

    if "." in value:
        f = float(value)
        if f.is_integer():
            return str(int(f))
        return str(f)

    return str(int(value))

def perform_cast(value: Any, col_type: Any, *, on_cast_error: Callable | None = None) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    
    # Integer
    if type(col_type) == Integer:
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # Float
    if type(col_type) == Float:
        try:
            return float(value)
        except (ValueError, TypeError):
            if on_cast_error:
                on_cast_error(value)
            return None
    # Boolean
    if type(col_type) == Boolean:
        v = _to_bool(value)
        if v is None and on_cast_error:
            on_cast_error(value)
        return v

    # Date, DateTime
    if type(col_type) == Date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            v = _parse_date(value)
            if not v and on_cast_error:
                on_cast_error(value)
            return v
        return None
    
    if type(col_type) == DateTime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            v = _parse_datetime(value)
            if not v and on_cast_error:
                on_cast_error(value)
        return v

    # String / Text
    if isinstance(col_type, String) or isinstance(col_type, Text):
        # Canonicalise numeric-looking identifiers
        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return None
            # conservative numeric normalisation for string types - avoiding scientific notation and floating point precision issues
            v = _to_numeric_string(v)
            if col_type.length and len(v) > col_type.length:
                if on_cast_error:
                    on_cast_error(value)
                v = v[: col_type.length]
            assert not col_type.length or len(v) <= col_type.length, (f"{v!r} exceeds {col_type.length} chars")
            return v

        return str(value)
    # Fallback: leave as is
    return value
