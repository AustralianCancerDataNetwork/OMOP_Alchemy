from __future__ import annotations
"""
This script rebuilds the SQLite test fixture from Athena vocabulary CSVs, then exports dummy clinical tables as CSV files.

It assumes you have a terse sample set of appropriate concepts in the Athena source, but will attempt to fall back to any available concepts if the ideal ones are not present. The generated clinical data is deterministic based on the provided random seed, but otherwise arbitrary and not meant to reflect any real patient population.
"""
import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from random import Random

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from omop_alchemy.cdm.model.clinical import Condition_Occurrence, Death, Measurement, Person
from omop_alchemy.cdm.model.derived import Observation_Period
from omop_alchemy.cdm.model.health_system import Care_Site, Location, Provider, Visit_Occurrence
from omop_alchemy.cdm.model.structural import Episode, Episode_Event
from omop_alchemy.cdm.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.load_vocab import load_vocab_source


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ATHENA_SOURCE = ROOT / "tests" / "fixtures" / "athena_source"
DEFAULT_DB_PATH = ROOT / "tests" / "fixtures" / "test.db"
DEFAULT_CLINICAL_CSV_DIR = ROOT / "tests" / "fixtures" / "test_clinical_csvs"

CLINICAL_EXPORT_MODELS = (
    Location,
    Care_Site,
    Provider,
    Person,
    Visit_Occurrence,
    Observation_Period,
    Death,
    Condition_Occurrence,
    Measurement,
    Episode,
    Episode_Event,
)


@dataclass(frozen=True)
class FixtureConcepts:
    genders: tuple[int, ...]
    ethnicities: tuple[int, ...]
    races: tuple[int, ...]
    visit_concepts: tuple[int, ...]
    location_concepts: tuple[int, ...]
    provider_specialties: tuple[int, ...]
    type_concepts: tuple[int, ...]
    condition_concepts: tuple[int, ...]
    stage_concepts: tuple[int, ...]
    episode_concept_id: int
    condition_event_field_concept_id: int
    measurement_event_field_concept_id: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the SQLite test fixture from Athena vocabulary CSVs, "
            "then export dummy clinical tables as CSV files."
        )
    )
    parser.add_argument(
        "--athena-source",
        type=Path,
        default=DEFAULT_ATHENA_SOURCE,
        help="Path to the curated Athena fixture directory.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Target SQLite database path.",
    )
    parser.add_argument(
        "--clinical-csv-dir",
        type=Path,
        default=DEFAULT_CLINICAL_CSV_DIR,
        help="Directory to receive exported dummy clinical CSV files.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=54,
        help="Deterministic random seed for dummy clinical data generation.",
    )
    parser.add_argument(
        "--person-count",
        type=int,
        default=24,
        help="Number of dummy people to generate.",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Ignore any existing fixture database and rebuild from scratch.",
    )
    return parser.parse_args()


def _reset_outputs(db_path: Path, clinical_csv_dir: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    clinical_csv_dir.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        db_path.unlink()

    for csv_path in clinical_csv_dir.glob("*.csv"):
        csv_path.unlink()


def _fixture_db_has_people(db_path: Path) -> bool:
    if not db_path.exists():
        return False

    engine = sa.create_engine(f"sqlite:///{db_path}", future=True, echo=False)
    try:
        inspector = sa.inspect(engine)
        if not inspector.has_table("person"):
            return False

        with engine.connect() as connection:
            person_count = connection.scalar(sa.text("SELECT COUNT(*) FROM person"))
        return bool(person_count and int(person_count) > 0)
    except Exception:
        return False
    finally:
        engine.dispose()


def _missing_clinical_csv_exports(output_dir: Path) -> tuple[str, ...]:
    missing: list[str] = []
    for model in CLINICAL_EXPORT_MODELS:
        output_path = output_dir / f"{model.__table__.name.upper()}.csv"
        if not output_path.exists():
            missing.append(output_path.name)
    return tuple(missing)


def _concept_ids(
    session: Session,
    *,
    domain_id: str | None = None,
    concept_name: str | None = None,
    vocabulary_id: str | None = None,
    concept_class_id: str | None = None,
    standard_only: bool = False,
    limit: int | None = None,
) -> tuple[int, ...]:
    stmt = sa.select(Concept.concept_id)
    if domain_id is not None:
        stmt = stmt.where(Concept.domain_id == domain_id)
    if concept_name is not None:
        stmt = stmt.where(Concept.concept_name == concept_name)
    if vocabulary_id is not None:
        stmt = stmt.where(Concept.vocabulary_id == vocabulary_id)
    if concept_class_id is not None:
        stmt = stmt.where(Concept.concept_class_id == concept_class_id)
    if standard_only:
        stmt = stmt.where(Concept.standard_concept == "S")
    stmt = stmt.order_by(Concept.concept_id)
    if limit is not None:
        stmt = stmt.limit(limit)
    return tuple(int(value) for value in session.scalars(stmt))


def _single_concept_id(session: Session, **filters: object) -> int:
    values = _concept_ids(session, limit=1, **filters)
    if not values:
        raise RuntimeError(f"Missing concept fixture for filters: {filters}")
    return values[0]


def _stage_concept_ids(session: Session) -> tuple[int, ...]:
    stage_root_id = 734320
    parent_stmt = (
        sa.select(Concept.concept_id)
        .join(
            Concept_Ancestor,
            Concept.concept_id == Concept_Ancestor.descendant_concept_id,
        )
        .where(Concept_Ancestor.ancestor_concept_id == stage_root_id)
        .where(Concept_Ancestor.max_levels_of_separation == 1)
        .where(
            sa.or_(
                Concept.concept_name.contains("T"),
                Concept.concept_name.contains("N"),
                Concept.concept_name.contains("M"),
                Concept.concept_name.contains("Stage"),
            )
        )
        .order_by(Concept.concept_id)
    )
    parent_ids = tuple(int(value) for value in session.scalars(parent_stmt))

    if parent_ids:
        descendant_stmt = (
            sa.select(Concept.concept_id)
            .join(
                Concept_Ancestor,
                Concept.concept_id == Concept_Ancestor.descendant_concept_id,
            )
            .where(Concept_Ancestor.ancestor_concept_id.in_(parent_ids))
            .where(Concept.concept_code.ilike("%8th%"))
            .where(~Concept.concept_code.ilike("%yp%"))
            .order_by(Concept.concept_id)
        )
        stage_ids = tuple(int(value) for value in session.scalars(descendant_stmt))
        if stage_ids:
            return stage_ids

    fallback = _concept_ids(session, domain_id="Measurement", standard_only=True, limit=12)
    if fallback:
        return fallback

    return _concept_ids(session, domain_id="Condition", standard_only=True, limit=12)


def _collect_fixture_concepts(session: Session) -> FixtureConcepts:
    genders = _concept_ids(session, domain_id="Gender", standard_only=True, limit=4)
    ethnicities = _concept_ids(session, domain_id="Ethnicity", standard_only=True, limit=4)
    races = _concept_ids(session, domain_id="Race", standard_only=True, limit=6)
    visit_concepts = _concept_ids(session, domain_id="Visit", standard_only=True, limit=8)
    location_concepts = _concept_ids(
        session,
        concept_class_id="Location",
        standard_only=True,
        limit=8,
    )
    provider_specialties = _concept_ids(session, domain_id="Provider", standard_only=True, limit=8)
    type_concepts = _concept_ids(session, domain_id="Type Concept", standard_only=True, limit=12)
    condition_concepts = _concept_ids(
        session,
        domain_id="Condition",
        vocabulary_id="ICDO3",
        standard_only=True,
        limit=24,
    ) or _concept_ids(session, domain_id="Condition", standard_only=True, limit=24)
    stage_concepts = _stage_concept_ids(session)

    if not all((genders, ethnicities, races, visit_concepts, type_concepts, condition_concepts)):
        raise RuntimeError("Fixture vocabulary does not contain the minimum concepts required for dummy clinical data.")

    return FixtureConcepts(
        genders=genders,
        ethnicities=ethnicities,
        races=races,
        visit_concepts=visit_concepts,
        location_concepts=location_concepts,
        provider_specialties=provider_specialties,
        type_concepts=type_concepts,
        condition_concepts=condition_concepts,
        stage_concepts=stage_concepts,
        episode_concept_id=_single_concept_id(
            session,
            domain_id="Episode",
            concept_name="Disease Episode",
        ),
        condition_event_field_concept_id=_single_concept_id(
            session,
            domain_id="Metadata",
            concept_name="condition_occurrence.condition_occurrence_id",
        ),
        measurement_event_field_concept_id=_single_concept_id(
            session,
            domain_id="Metadata",
            concept_name="measurement.measurement_id",
        ),
    )


def _pick(values: tuple[int, ...], rng: Random, index: int) -> int:
    if not values:
        raise RuntimeError("Expected at least one fixture concept value.")
    return values[(index + rng.randint(0, len(values) - 1)) % len(values)]


def _seed_dummy_clinical_data(
    session: Session,
    *,
    concepts: FixtureConcepts,
    rng: Random,
    person_count: int,
) -> None:
    locations: list[Location] = []
    care_sites: list[Care_Site] = []
    providers: list[Provider] = []

    for index in range(1, 7):
        country_concept_id = (
            _pick(concepts.location_concepts, rng, index)
            if concepts.location_concepts
            else None
        )
        locations.append(
            Location(
                location_id=index,
                city=f"Fixture City {index}",
                state="NS",
                zip=f"20{index:02d}",
                country_concept_id=country_concept_id,
                location_source_value=f"fixture-location-{index}",
            )
        )

    for index in range(1, 9):
        location = locations[(index - 1) % len(locations)]
        care_sites.append(
            Care_Site(
                care_site_id=index,
                care_site_name=f"Fixture Care Site {index}",
                location_id=location.location_id,
                place_of_service_concept_id=_pick(concepts.visit_concepts, rng, index),
                care_site_source_value=f"fixture-care-site-{index}",
            )
        )

    for index in range(1, 13):
        care_site = care_sites[(index - 1) % len(care_sites)]
        specialty = (
            _pick(concepts.provider_specialties, rng, index)
            if concepts.provider_specialties
            else None
        )
        providers.append(
            Provider(
                provider_id=index,
                provider_name=f"Fixture Provider {index}",
                care_site_id=care_site.care_site_id,
                specialty_concept_id=specialty,
                gender_concept_id=_pick(concepts.genders, rng, index),
                provider_source_value=f"fixture-provider-{index}",
            )
        )

    session.add_all(locations)
    session.add_all(care_sites)
    session.add_all(providers)
    session.flush()

    people: list[Person] = []
    visits: list[Visit_Occurrence] = []
    observation_periods: list[Observation_Period] = []
    deaths: list[Death] = []
    conditions: list[Condition_Occurrence] = []
    measurements: list[Measurement] = []
    episodes: list[Episode] = []
    episode_events: list[Episode_Event] = []

    visit_id = 1
    observation_period_id = 1
    condition_id = 1
    measurement_id = 1
    episode_id = 1
    base_date = date(2020, 1, 1)

    for person_id in range(1, person_count + 1):
        location = locations[(person_id - 1) % len(locations)]
        care_site = care_sites[(person_id - 1) % len(care_sites)]
        provider = providers[(person_id - 1) % len(providers)]

        person = Person(
            person_id=person_id,
            year_of_birth=1950 + (person_id % 55),
            month_of_birth=(person_id % 12) + 1,
            day_of_birth=(person_id % 28) + 1,
            gender_concept_id=_pick(concepts.genders, rng, person_id),
            race_concept_id=_pick(concepts.races, rng, person_id),
            ethnicity_concept_id=_pick(concepts.ethnicities, rng, person_id),
            location_id=location.location_id,
            provider_id=provider.provider_id,
            care_site_id=care_site.care_site_id,
            person_source_value=f"fixture-person-{person_id}",
        )
        people.append(person)

        visit_count = 1 + (person_id % 3)
        person_visits: list[Visit_Occurrence] = []
        for visit_index in range(visit_count):
            visit_date = base_date + timedelta(days=(person_id * 9) + (visit_index * 14))
            person_visits.append(
                Visit_Occurrence(
                    visit_occurrence_id=visit_id,
                    person_id=person_id,
                    visit_concept_id=_pick(concepts.visit_concepts, rng, visit_id),
                    visit_start_date=visit_date,
                    visit_end_date=visit_date + timedelta(days=1),
                    visit_type_concept_id=_pick(concepts.type_concepts, rng, visit_id),
                    provider_id=provider.provider_id,
                    care_site_id=care_site.care_site_id,
                    visit_source_value=f"fixture-visit-{visit_id}",
                )
            )
            visit_id += 1

        visits.extend(person_visits)

        first_visit_date = person_visits[0].visit_start_date
        last_visit_date = person_visits[-1].visit_end_date
        death_date = None
        if person_id % 8 == 0:
            death_date = last_visit_date + timedelta(days=30 + person_id)
            deaths.append(
                Death(
                    person_id=person_id,
                    death_date=death_date,
                    death_type_concept_id=_pick(concepts.type_concepts, rng, person_id),
                )
            )

        observation_periods.append(
            Observation_Period(
                observation_period_id=observation_period_id,
                person_id=person_id,
                observation_period_start_date=first_visit_date,
                observation_period_end_date=death_date or last_visit_date,
                period_type_concept_id=_pick(concepts.type_concepts, rng, observation_period_id),
            )
        )
        observation_period_id += 1

        primary_visit = person_visits[0]
        condition = Condition_Occurrence(
            condition_occurrence_id=condition_id,
            person_id=person_id,
            condition_concept_id=_pick(concepts.condition_concepts, rng, condition_id),
            condition_start_date=primary_visit.visit_start_date,
            condition_end_date=primary_visit.visit_end_date + timedelta(days=28),
            condition_type_concept_id=_pick(concepts.type_concepts, rng, condition_id),
            visit_occurrence_id=primary_visit.visit_occurrence_id,
            provider_id=provider.provider_id,
            condition_source_value=f"fixture-condition-{condition_id}",
        )
        conditions.append(condition)

        episode = Episode(
            episode_id=episode_id,
            person_id=person_id,
            episode_parent_id=(episode_id - 1) if person_id % 6 == 0 else None,
            episode_concept_id=concepts.episode_concept_id,
            episode_object_concept_id=condition.condition_concept_id,
            episode_start_date=condition.condition_start_date,
            episode_end_date=death_date or (condition.condition_end_date or condition.condition_start_date),
            episode_type_concept_id=_pick(concepts.type_concepts, rng, episode_id),
            episode_source_value=f"fixture-episode-{episode_id}",
        )
        episodes.append(episode)

        for offset in range(3):
            stage_concept_id = _pick(concepts.stage_concepts, rng, measurement_id + offset)
            measurement = Measurement(
                measurement_id=measurement_id,
                person_id=person_id,
                measurement_concept_id=stage_concept_id,
                measurement_date=condition.condition_start_date + timedelta(days=offset * 7),
                measurement_type_concept_id=_pick(concepts.type_concepts, rng, measurement_id),
                measurement_event_id=condition.condition_occurrence_id,
                meas_event_field_concept_id=concepts.condition_event_field_concept_id,
                visit_occurrence_id=primary_visit.visit_occurrence_id,
                provider_id=provider.provider_id,
                value_as_number=float(offset + 1),
                measurement_source_value=f"fixture-measurement-{measurement_id}",
            )
            measurements.append(measurement)
            episode_events.append(
                Episode_Event(
                    episode_id=episode.episode_id,
                    event_id=measurement.measurement_id,
                    episode_event_field_concept_id=concepts.measurement_event_field_concept_id,
                )
            )
            measurement_id += 1

        episode_events.append(
            Episode_Event(
                episode_id=episode.episode_id,
                event_id=condition.condition_occurrence_id,
                episode_event_field_concept_id=concepts.condition_event_field_concept_id,
            )
        )

        condition_id += 1
        episode_id += 1

    session.add_all(people)
    session.add_all(visits)
    session.add_all(observation_periods)
    session.add_all(deaths)
    session.add_all(conditions)
    session.add_all(episodes)
    session.add_all(measurements)
    session.add_all(episode_events)
    session.commit()


def _export_table_csvs(engine: sa.Engine, output_dir: Path) -> None:
    with engine.connect() as connection:
        for model in CLINICAL_EXPORT_MODELS:
            table = model.__table__
            stmt = sa.select(table)
            primary_keys = list(table.primary_key.columns)
            if primary_keys:
                stmt = stmt.order_by(*primary_keys)
            frame = pd.read_sql_query(stmt, connection)
            output_path = output_dir / f"{table.name.upper()}.csv"
            frame.to_csv(output_path, index=False)


def main() -> None:
    args = parse_args()
    athena_source = args.athena_source.expanduser().resolve()
    db_path = args.db_path.expanduser().resolve()
    clinical_csv_dir = args.clinical_csv_dir.expanduser().resolve()
    rng = Random(args.seed)

    clinical_csv_dir.mkdir(parents=True, exist_ok=True)

    if not args.force_rebuild and _fixture_db_has_people(db_path):
        print(f"Using existing SQLite fixture at {db_path}")
        missing_exports = _missing_clinical_csv_exports(clinical_csv_dir)
        if not missing_exports:
            print(f"Clinical CSV fixtures already present in {clinical_csv_dir}")
            return

        print(
            "Existing SQLite fixture is valid but some clinical CSV exports are missing: "
            + ", ".join(missing_exports)
        )
        engine = sa.create_engine(f"sqlite:///{db_path}", future=True, echo=False)
        try:
            for csv_path in clinical_csv_dir.glob("*.csv"):
                csv_path.unlink()
            _export_table_csvs(engine, clinical_csv_dir)
            print(f"Exported clinical CSV fixtures to {clinical_csv_dir}")
            return
        finally:
            engine.dispose()

    _reset_outputs(db_path, clinical_csv_dir)

    engine = sa.create_engine(f"sqlite:///{db_path}", future=True, echo=False)
    try:
        print(f"Loading Athena vocabulary from {athena_source}")
        vocab_report = load_vocab_source(
            engine,
            source_path=athena_source,
            merge_strategy="upsert",
        )
        loaded_count = sum(1 for result in vocab_report.results if result.status == "loaded")
        print(f"Loaded {loaded_count} vocabulary table(s)")

        creation_results = create_missing_tables(engine, vocabulary_included=False)
        created_count = sum(1 for result in creation_results if result.status == "created")
        print(f"Created {created_count} non-vocabulary table(s)")

        SessionLocal = sessionmaker(bind=engine, future=True, expire_on_commit=False)
        with SessionLocal() as session:
            concepts = _collect_fixture_concepts(session)
            _seed_dummy_clinical_data(
                session,
                concepts=concepts,
                rng=rng,
                person_count=args.person_count,
            )

        _export_table_csvs(engine, clinical_csv_dir)
        print(f"Wrote SQLite fixture to {db_path}")
        print(f"Exported clinical CSV fixtures to {clinical_csv_dir}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
