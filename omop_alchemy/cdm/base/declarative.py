from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

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
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.close()


def explain_sqlite_fk_error(session, exc: IntegrityError):
    engine = session.get_bind()

    if engine.dialect.name != "sqlite":
        raise exc

    with engine.connect() as conn:
        rows = conn.execute(text("PRAGMA foreign_key_check")).fetchall()

    details = "\n".join(
        f"- table={r[0]}, rowid={r[1]}, references={r[2]}, fk_index={r[3]}"
        for r in rows
    )

    raise IntegrityError(
        f"{exc}\n\nForeign key violations detected:\n{details}",
        exc.params,
        exc.orig, # type: ignore
    )


class Base(DeclarativeBase):
    metadata = metadata

def create_db(Base, engine):
    Base.metadata.create_all(engine)

def bootstrap(engine, *, create: bool = True):
    """
    Initialise OMOP schema against a provided SQLAlchemy engine.
    """
    if create:
        create_db(Base, engine)

        
