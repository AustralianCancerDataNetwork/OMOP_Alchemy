from sqlalchemy import MetaData, event, text
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
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
        logger.debug(f"FK violation: table={r[0]} rowid={r[1]} references={r[2]} fk_index={r[3]}")
    if raise_error:
        raise exc


class Base(DeclarativeBase):
    metadata = metadata

def create_db(Base, engine):
    logger.debug(f"Database dialect: {engine.dialect.name}")
    Base.metadata.create_all(engine)

def bootstrap(engine, *, create: bool = True):
    """
    Initialise OMOP schema against a provided SQLAlchemy engine.
    """
    logger.info(f"Bootstrapping OMOP schema (create={create})")
    if create:
        logger.info("Schema creation enabled")
        create_db(Base, engine)
    else:
        logger.info("Schema creation skipped (existing schema assumed)")


@contextmanager
def bulk_load_context(
    session: Session,
    *,
    disable_fk: bool = True,
    no_autoflush: bool = True,
):
    """
    Context manager for trusted bulk loads (e.g. Athena vocabulary).

    - Disables FK enforcement where supported
    - Suppresses autoflush
    - Restores state reliably
    """

    engine = session.get_bind()
    dialect = engine.dialect.name

    # FK control
    fk_disabled = False

    try:
        if disable_fk:
            if dialect == "postgresql":
                session.execute(text(
                    "SET session_replication_role = replica"
                ))
                fk_disabled = True

            elif dialect == "sqlite":
                session.execute(text(
                    "PRAGMA foreign_keys = OFF"
                ))
                fk_disabled = True

        if no_autoflush:
            with session.no_autoflush:
                yield
        else:
            yield

    
    except Exception:
        session.rollback()
        raise

    finally:
        if fk_disabled:
            if dialect == "postgresql":
                session.execute(text(
                    "SET session_replication_role = DEFAULT"
                ))
            elif dialect == "sqlite":
                session.execute(text(
                    "PRAGMA foreign_keys = ON"
                ))

def get_table_by_name(tablename: str) -> ORMTable | None:
    for cls in Base.__subclasses__():
        if cls.__tablename__ == tablename.lower().strip():
            if isinstance(cls, ORMTable):
                return cls
    return None
