import functools
import logging

from .db import ConnectionDefaults, defaults_path

@functools.lru_cache(maxsize=None)
def configure_logging() -> None:
    mode = (ConnectionDefaults.load().logging or "file").strip().lower()
    if mode not in {"file", "console", "off"}:
        mode = "file"
    if mode == "off":
        return

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    if mode == "file":
        log_path = defaults_path().parent / "logging" / "omop-alchemy.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler: logging.Handler = logging.FileHandler(log_path, encoding="utf-8")
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if root_logger.level in {logging.NOTSET, logging.WARNING, logging.ERROR, logging.CRITICAL}:
        root_logger.setLevel(logging.INFO)