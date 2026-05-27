from oa_configurator import configure_logging as _configure_logging


def configure_logging(verbosity: int = 0) -> None:
    _configure_logging(verbosity=verbosity, extra_namespaces=["omop_alchemy"])
