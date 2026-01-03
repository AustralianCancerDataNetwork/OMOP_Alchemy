from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from omop_alchemy.cdm.utils import get_logger
from .typing import ORMTable

logger = get_logger(__name__)

NAMING_CONVENTIONS = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTIONS)

@event.listens_for(Engine, "connect")
def enable_sqlite_foreign_keys(dbapi_connection, connection_record):
    # SQLite only
    if dbapi_connection.__class__.__module__.startswith("sqlite3"):
        logger.debug("Enabling SQLite foreign key enforcement (PRAGMA foreign_keys=ON)")
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA defer_foreign_keys = ON;")
        cursor.close()


def explain_sqlite_fk_error(session, exc: IntegrityError, raise_error: bool = True) -> None:
    engine = session.get_bind()

    if engine.dialect.name != "sqlite":
        raise exc
    
    with engine.connect() as conn:
        logger.info("SQLite integrity violation detected, running PRAGMA foreign_key_check")
        rows = conn.execute(text("PRAGMA foreign_key_check")).fetchall()
        if len(rows) == 0:
            logger.info("No foreign key violations found by PRAGMA foreign_key_check")
            return
        logger.error(
            "SQLite foreign key violations found: %d issue(s)", 
            len(rows)
        )

    for r in rows:
        logger.debug(
            "FK violation: table=%s rowid=%s references=%s fk_index=%s",
            r[0], r[1], r[2], r[3]
        )
    if raise_error:
        raise exc


class Base(DeclarativeBase):
    metadata = metadata

def create_db(Base, engine):
    logger.debug("Database dialect: %s", engine.dialect.name)
    Base.metadata.create_all(engine)

def bootstrap(engine, *, create: bool = True):
    """
    Initialise OMOP schema against a provided SQLAlchemy engine.
    """
    from omop_alchemy.model import __all__ as model_modules
    # for module_name in model_modules:
    #     __import__(f"omop_alchemy.model.{module_name}")
    #     print(module_name)
    logger.info("Bootstrapping OMOP schema (create=%s)", create)
    if create:
        logger.info("Schema creation enabled")
        create_db(Base, engine)
    else:
        logger.info("Schema creation skipped (existing schema assumed)")

        
def get_table_by_name(tablename: str) -> ORMTable | None:
    for cls in Base.__subclasses__():
        if cls.__tablename__ == tablename:
            if isinstance(cls, ORMTable):
                return cls
    return None
