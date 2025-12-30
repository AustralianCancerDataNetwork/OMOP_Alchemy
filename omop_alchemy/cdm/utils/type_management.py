from typing import Any
from sqlalchemy import Integer, Float, Boolean, Date, DateTime, Enum, String, Text
from typing import Any
from datetime import date, datetime 
import math


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

def perform_cast(value: Any, col_type: Any) -> Any:
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
            return None
    # Boolean
    if type(col_type) == Boolean:
        return _to_bool(value)

    # Date, DateTime
    if type(col_type) == Date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return _parse_date(value)
        return None
    
    if type(col_type) == DateTime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            return _parse_datetime(value)
        return None

    # String / Text
    if type(col_type) == String or type(col_type) == Text:
        # Canonicalise numeric-looking identifiers
        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        if isinstance(value, str):
            v = value.strip()
            if v == "":
                return None
            if v.isdigit():
                return str(int(v))
            try:
                f = float(v)
                if f.is_integer():
                    return str(int(f))
            except ValueError:
                pass
            return v

        return str(value)
    # Fallback: leave as is
    return value
