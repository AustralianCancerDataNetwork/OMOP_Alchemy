from __future__ import annotations

from typing import Any


def default_semantics_runtime() -> Any:
    """
    Return the bundled omop-semantics runtime, or fail with install guidance.

    omop-semantics is an optional dependency of omop-alchemy. Toolkit modules
    that use governed default concept sets should call this lazily when those
    defaults are actually needed, so importing core CDM models does not require
    the semantics package.
    """
    try:
        from omop_semantics.runtime.default_valuesets import runtime
    except ImportError as exc:
        raise ImportError(
            "This feature requires omop-semantics. Install omop-alchemy with "
            "the 'semantics' extra, for example: omop-alchemy[semantics]."
        ) from exc
    return runtime
