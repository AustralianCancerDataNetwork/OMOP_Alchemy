try:
    import pyarrow  # noqa: F401
    import meds  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "The omop_alchemy MEDS handler requires optional dependencies. "
        "Install them with: pip install omop_alchemy[meds]"
    ) from exc
