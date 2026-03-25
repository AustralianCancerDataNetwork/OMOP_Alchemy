import pytest
from pathlib import Path
from orm_loader.helpers import bootstrap
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


@pytest.fixture(scope="session")
def connection():
    """
    In-memory SQLite database for tests.
    """
    engine = sa.create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
    )

    connection = engine.connect()
    bootstrap(connection, create=True)
    yield connection
    connection.close()
    engine.dispose()


@pytest.fixture
def db_session(connection):
    """
    SQLAlchemy session per test.
    """
    Session = sessionmaker(bind=connection, future=True)
    session = Session()
    try:
        yield session
        session.commit()
    finally:
        session.rollback()
        session.close()


@pytest.fixture(scope="session")
def athena_vocab(connection):
    """
    Load a minimal, internally consistent Athena vocabulary
    using the real ORM CSV loader.
    """
    Session = sessionmaker(bind=connection, future=True)
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
    """Test concept loaded."""
    concept = db_session.get(Concept, 1)
    assert concept is not None
    assert concept.concept_name == "Domain"
    assert concept.domain_id == "Metadata"  

def test_concept_ancestor(db_session, athena_vocab):
    """Test concept ancestor."""
    ancestors = (
        # running tests with metadata concepts so that they are definitely present
        # assuming the logic to produce test db is stable
        db_session.query(Concept_Ancestor)
        .filter_by(descendant_concept_id=1147371)
        .all()
    )
    assert len(ancestors) == 2
    a = [a.ancestor_concept_id for a in ancestors]
    assert 1147371 in a
    assert 1147423 in a

def test_all_concepts_reference_valid_domain(db_session, athena_vocab):
    """Test all concepts reference valid domain."""
    invalid = (
        db_session.query(Concept)
        .outerjoin(Domain, Concept.domain_id == Domain.domain_id)
        .filter(Domain.domain_id == None)
        .count()
    )

    assert invalid == 0

def test_relationship_vocab_loaded(db_session, athena_vocab):
    """Test relationship vocab loaded."""
    rel = (
        db_session.query(Relationship)
        .filter_by(relationship_id="Has type")
        .one()
    )

    assert rel.reverse_relationship_id == "Type of"

def test_expected_domains_exist(db_session, athena_vocab):
    """Test expected domains exist."""
    domains = {
        d.domain_id
        for d in db_session.query(Domain.domain_id).all()
    }

    assert "Condition" in domains
    assert "Procedure" in domains
    assert "Drug" in domains

def test_domains_are_consistent(db_session, athena_vocab):
    """Test domains are consistent."""
    concepts = (
        db_session.query(Concept)
        .filter(Concept.domain_id.in_(["Condition", "Procedure"]))
        .all()
    )

    assert concepts 

    for c in concepts:
        assert c.domain_id in {"Condition", "Procedure"}

def test_procedure_concepts_exist(db_session, athena_vocab):
    """Test procedure concepts exist."""
    assert (
        db_session.query(Concept)
        .filter(Concept.domain_id == "Procedure")
        .count()
        > 0
    )

def test_relationships_reference_valid_concepts(db_session, athena_vocab):
    """Test relationships reference valid concepts."""
    rels = db_session.query(Concept_Relationship).limit(50).all()

    for r in rels:
        assert r.concept_id_1 is not None
        assert r.concept_id_2 is not None
