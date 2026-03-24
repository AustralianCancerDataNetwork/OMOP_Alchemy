from datetime import date
from pathlib import Path

import pytest
import sqlalchemy as sa
from orm_loader.helpers import bootstrap
from sqlalchemy.orm import Session, sessionmaker

from omop_alchemy.cdm.model.clinical import Condition_Occurrence, Person
from omop_alchemy.cdm.model.derived import Observation_Period
from omop_alchemy.cdm.model.structural import Episode, Episode_Event
from omop_alchemy.cdm.model.vocabulary import (
    Concept,
    Concept_Ancestor,
    Concept_Class,
    Concept_Relationship,
    Domain,
    Relationship,
    Vocabulary,
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


def _athena_source_path() -> Path:
    return Path(__file__).parent / "fixtures" / "athena_source"


def _load_fixture_vocabulary(session: Session) -> None:
    base_path = _athena_source_path()

    for model in ATHENA_LOAD_ORDER:
        csv_path = base_path / f"{model.__tablename__.upper()}.csv"
        model.load_csv(session, csv_path)


def _concept_id(
    session: Session,
    *,
    concept_name: str | None = None,
    domain_id: str | None = None,
) -> int:
    query = sa.select(Concept.concept_id)

    if concept_name is not None:
        query = query.where(Concept.concept_name == concept_name)
    if domain_id is not None:
        query = query.where(Concept.domain_id == domain_id)

    concept_id = session.scalar(query.order_by(Concept.concept_id).limit(1))
    if concept_id is None:
        raise RuntimeError(
            f"Missing concept fixture for concept_name={concept_name!r}, "
            f"domain_id={domain_id!r}"
        )
    return int(concept_id)


def _seed_basic_clinical_data(session: Session) -> None:
    male_concept_id = _concept_id(session, concept_name="MALE", domain_id="Gender")
    race_concept_id = _concept_id(session, concept_name="White", domain_id="Race")
    ethnicity_concept_id = _concept_id(
        session,
        concept_name="Not Hispanic or Latino",
        domain_id="Ethnicity",
    )
    type_concept_id = _concept_id(session, domain_id="Type Concept")
    condition_concept_id = _concept_id(session, domain_id="Condition")
    episode_concept_id = _concept_id(session, domain_id="Episode")
    event_field_concept_id = _concept_id(
        session,
        concept_name="condition_occurrence.condition_occurrence_id",
        domain_id="Metadata",
    )

    session.add(
        Person(
            person_id=1,
            year_of_birth=1980,
            month_of_birth=1,
            day_of_birth=1,
            gender_concept_id=male_concept_id,
            race_concept_id=race_concept_id,
            ethnicity_concept_id=ethnicity_concept_id,
            person_source_value="fixture-person-1",
        )
    )
    session.add(
        Observation_Period(
            observation_period_id=1,
            person_id=1,
            observation_period_start_date=date(2019, 1, 1),
            observation_period_end_date=date(2021, 12, 31),
            period_type_concept_id=type_concept_id,
        )
    )
    session.add(
        Condition_Occurrence(
            condition_occurrence_id=1,
            person_id=1,
            condition_concept_id=condition_concept_id,
            condition_start_date=date(2020, 1, 1),
            condition_end_date=date(2020, 1, 15),
            condition_type_concept_id=type_concept_id,
            condition_source_value="fixture-condition-1",
        )
    )
    session.add(
        Episode(
            episode_id=100,
            person_id=1,
            episode_start_date=date(2020, 1, 1),
            episode_end_date=date(2020, 1, 31),
            episode_concept_id=episode_concept_id,
            episode_object_concept_id=condition_concept_id,
            episode_type_concept_id=type_concept_id,
            episode_source_value="fixture-parent-episode",
        )
    )
    session.add(
        Episode(
            episode_id=101,
            episode_parent_id=100,
            person_id=1,
            episode_start_date=date(2020, 1, 2),
            episode_end_date=date(2020, 1, 20),
            episode_concept_id=episode_concept_id,
            episode_object_concept_id=condition_concept_id,
            episode_type_concept_id=type_concept_id,
            episode_source_value="fixture-child-episode",
        )
    )
    session.add(
        Episode_Event(
            episode_id=101,
            event_id=1,
            episode_event_field_concept_id=event_field_concept_id,
        )
    )

    session.commit()


@pytest.fixture(scope="session")
def engine(tmp_path_factory: pytest.TempPathFactory):
    """
    Session-scoped SQLite engine built from repo fixtures.

    The database is created fresh for each test session, so it behaves
    the same on the host and inside containers.
    """
    db_dir = tmp_path_factory.mktemp("omop-alchemy")
    db_path = db_dir / "test.db"
    engine = sa.create_engine(
        f"sqlite:///{db_path}",
        future=True,
        echo=False,
    )

    bootstrap(engine, create=True)

    SessionLocal = sessionmaker(bind=engine, future=True, expire_on_commit=False)
    with SessionLocal() as seed_session:
        _load_fixture_vocabulary(seed_session)
        _seed_basic_clinical_data(seed_session)

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="function")
def session(engine) -> Session:  # type: ignore
    """
    Function-scoped SQLAlchemy session.

    Each test gets a clean transactional boundary.
    """
    SessionLocal = sessionmaker(
        bind=engine,
        future=True,
        expire_on_commit=False,
    )

    session = SessionLocal()

    try:
        yield session  # type: ignore
        session.rollback()
    finally:
        session.close()
