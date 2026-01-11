from orm_loader.tables import CSVLoadableTableInterface, SerialisableTableInterface


class CDMTableBase(CSVLoadableTableInterface, SerialisableTableInterface):  
    """
    Base class for CDM tables that support CSV loading and validation.
    """
    __abstract__ = True

    """
    Adds structural validation of ORM models against
    the official OMOP CDM CSV specifications.
    """
    __cdm_extra_checks__: list[str] = []