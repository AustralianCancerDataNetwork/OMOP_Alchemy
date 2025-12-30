from .ingestion import CSVSourceMixin
from ..registry import CDMValidatedTableMixin

class CDMTableBase(CSVSourceMixin, CDMValidatedTableMixin):
    """
    Base class for CDM tables that support CSV loading and validation.
    """
    __abstract__ = True