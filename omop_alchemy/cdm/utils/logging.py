from __future__ import annotations
import logging
from typing import Literal, Optional
import re

SENSITIVE_KEYS = {
    "password",
    "passwd",
    "secret",
    "token",
    "key",
    "dsn",
    "uri",
    "url",
}
LOGGING_NAMESPACE = "omop_alchemy"

def _coerce_log_level(level: int | str) -> int:
    if isinstance(level, int):
        return level

    if isinstance(level, str):
        s = level.strip().upper()
        if s.isdigit():
            return int(s)

        mapping = logging.getLevelNamesMapping()
        if s in mapping:
            return mapping[s]

        raise ValueError(f"Invalid log level: {level!r}")

    raise TypeError(f"Invalid log level type: {type(level)}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a namespaced logger.

    Examples:
        get_logger() -> omop_alchemy
        get_logger("graph") -> omop_alchemy.graph
    """
    full_name = LOGGING_NAMESPACE if name is None else f"{LOGGING_NAMESPACE}.{name}"
    return logging.getLogger(full_name)


class RedactingFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pattern = re.compile(
            r"(?i)\\b(" + "|".join(SENSITIVE_KEYS) + r")\\b\\s*[:=]\\s*[^\\s,;]+"
        )

    def format(self, record):
        msg = super().format(record)
        return self._pattern.sub(r"\\1=<REDACTED>", msg)
    
def configure_logging(
    *,
    level: int | str = logging.INFO,
    handler: Optional[logging.Handler] = None,
    format: Optional[str] = None,
    propagate: bool = True,
    redact: bool = True,
) -> None:
    """
    Enable logging output for omop_alchemy.

    Safe to call multiple times.
    """
    logger = get_logger()
    logger.setLevel(_coerce_log_level(level))

    if handler is None:
        handler = logging.StreamHandler()

    if format is None:
        format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    formatter_cls = RedactingFormatter if redact else logging.Formatter
    handler.setFormatter(formatter_cls(format))

    if not any(isinstance(h, type(handler)) for h in logger.handlers):
        logger.addHandler(handler)

    logger.propagate = propagate


logging.getLogger(LOGGING_NAMESPACE).addHandler(logging.NullHandler())