# tests/fixtures/load_athena.py

import pytest
from pathlib import Path
from orm_loader.helpers import configure_logging, bootstrap
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from omop_alchemy.cdm.model.vocabulary import (
    Domain,
    Vocabulary,
    Concept_Class,
    Relationship,
    Concept,
    Concept_Ancestor,
    Concept_Relationship,
)

ATHENA_LOAD_ORDER = [
    Domain,
    Vocabulary,
    Concept_Class,
    Relationship,
    Concept,
    Concept_Ancestor,
    Concept_Relationship,
]


# def load_athena_vocab(session, base_path: Path):
#     for model in ATHENA_LOAD_ORDER:
#         model.load_csv(
#             session,
#             base_path / f"{model.__tablename__}.csv",
#         )
#     session.flush()



@pytest.fixture(scope="session")
def engine():
    """
    In-memory SQLite database for tests.
    """
    engine = sa.create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
    )

    bootstrap(engine, create=True)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(engine):
    """
    SQLAlchemy session per test.
    """
    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    try:
        yield session
        session.commit()
    finally:
        session.rollback()
        session.close()


@pytest.fixture(scope="session")
def athena_vocab(engine):
    """
    Load a minimal, internally consistent Athena vocabulary
    using the real ORM CSV loader.
    """
    Session = sessionmaker(bind=engine, future=True)
    session = Session()

    base_path = (
        Path(__file__).parent
        / "fixtures"
        / "athena_source"
    )

    for model in ATHENA_LOAD_ORDER:
        csv_path = base_path / f"{model.__tablename__}.csv"
        if not csv_path.exists():
            raise RuntimeError(f"Missing vocab CSV: {csv_path}")

        model.load_csv(session, csv_path)

    session.commit()
    session.close()

    yield

def test_concept_loaded(db_session, athena_vocab):
    concept = db_session.get(Concept, 101)
    assert concept is not None
    assert concept.concept_name == "Lung cancer"
    assert concept.domain_id == "Condition"

def test_concept_ancestor(db_session, athena_vocab):
    ancestors = (
        db_session.query(Concept_Ancestor)
        .filter_by(descendant_concept_id=101)
        .all()
    )

    assert len(ancestors) == 1
    assert ancestors[0].ancestor_concept_id == 100

def test_condition_domain_is_correct(db_session, athena_vocab):
    condition = db_session.get(Concept, 101)
    assert condition.domain_id == "Condition"

    procedure = db_session.get(Concept, 201)
    assert procedure.domain_id == "Procedure"

def test_relationship_vocab_loaded(db_session, athena_vocab):
    rel = (
        db_session.query(Relationship)
        .filter_by(relationship_id="Uses drug")
        .one()
    )

    assert rel.reverse_relationship_id == "Drug used by"