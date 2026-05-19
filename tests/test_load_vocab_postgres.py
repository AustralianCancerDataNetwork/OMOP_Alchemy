"""
PostgreSQL integration tests for OMOP_Alchemy vocabulary loading.

These tests require a running PostgreSQL container. Start one with:
    docker compose -f tests/docker-compose.yaml up -d

Then run:
    pytest -m postgres
"""
import shutil
from pathlib import Path

import pytest
import sqlalchemy as sa

from omop_alchemy.cdm.model.vocabulary import Concept
from omop_alchemy.maintenance.load_vocab import (
    REQUIRED_VOCAB_MODELS,
    _load_vocab_model_csv,
    load_vocab_source,
)

_FIXTURE_SOURCE = Path(__file__).parent / "fixtures" / "athena_source"


def _make_concept_source(
    base_dir: Path,
    *,
    concept_id: int,
    concept_name: str,
) -> Path:
    """
    Build a minimal vocabulary source where CONCEPT.csv contains exactly one
    test concept with a Gender domain reference, and all other required tables
    are copied from the shared fixture (which has the Gender domain row).
    """
    source_path = base_dir / "athena_source"
    source_path.mkdir(parents=True)

    for fname in (
        "DOMAIN.csv",
        "VOCABULARY.csv",
        "CONCEPT_CLASS.csv",
        "RELATIONSHIP.csv",
        "CONCEPT_ANCESTOR.csv",
        "CONCEPT_RELATIONSHIP.csv",
        "CONCEPT_SYNONYM.csv",
    ):
        shutil.copy(_FIXTURE_SOURCE / fname, source_path / fname)

    (source_path / "CONCEPT.csv").write_text(
        "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\t"
        "standard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason\n"
        f"{concept_id}\t{concept_name}\tGender\tGender\tGender\tS\tTEST\t19700101\t20991231\t\n",
        encoding="utf-8",
    )
    return source_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.postgres
def test_end_to_end_vocab_load_on_postgres(pg_session, pg_engine):
    """load_vocab_source() completes end-to-end on real Postgres via orm-loader>=0.4.0."""
    report = load_vocab_source(pg_engine, source_path=_FIXTURE_SOURCE)

    assert report.merge_strategy == "replace"
    assert all(r.status == "loaded" for r in report.results if r.required)
    assert all(r.status == "skipped" for r in report.results if not r.required)

    count = pg_session.execute(sa.text("SELECT COUNT(*) FROM concept")).scalar()
    assert count == 7


@pytest.mark.postgres
def test_quote_mode_auto_regression_on_postgres(pg_session, pg_engine, tmp_path):
    """
    quote_mode='auto' strips RFC-4180 double-quotes via PostgreSQL COPY.

    Under the old quote_mode='literal' a concept_name of exactly 255 chars
    wrapped in double-quotes would be stored as 257 chars and violate the
    VARCHAR(255) constraint. This test would fail under literal mode.
    """
    source_path = tmp_path / "athena_source"
    source_path.mkdir()

    long_name = "A" * 255  # exactly at VARCHAR(255) limit when unquoted

    for model in REQUIRED_VOCAB_MODELS:
        table_name = model.__tablename__.upper()
        csv_path = source_path / f"{table_name}.csv"
        if table_name == "CONCEPT":
            # Wrap the 255-char name in double-quotes so it's 257 chars raw.
            csv_path.write_text(
                "concept_id\tconcept_name\tdomain_id\tvocabulary_id\t"
                "concept_class_id\tstandard_concept\tconcept_code\t"
                "valid_start_date\tvalid_end_date\tinvalid_reason\n"
                f'1\t"{long_name}"\tGender\tGender\tGender\tS\tTEST\t19700101\t20991231\t\n',
                encoding="utf-8",
            )
        elif table_name == "DOMAIN":
            csv_path.write_text(
                "domain_id\tdomain_name\tdomain_concept_id\nGender\tGender\t0\n",
                encoding="utf-8",
            )
        elif table_name == "VOCABULARY":
            csv_path.write_text(
                "vocabulary_id\tvocabulary_name\tvocabulary_reference\t"
                "vocabulary_version\tvocabulary_concept_id\n"
                "Gender\tOMOP Gender\tOHDSI\tv1.0\t0\n",
                encoding="utf-8",
            )
        elif table_name == "CONCEPT_CLASS":
            csv_path.write_text(
                "concept_class_id\tconcept_class_name\tconcept_class_concept_id\n"
                "Gender\tGender\t0\n",
                encoding="utf-8",
            )
        else:
            shutil.copy(_FIXTURE_SOURCE / f"{table_name}.csv", csv_path)

    # Should not raise: literal mode would produce a 257-char value and fail.
    load_vocab_source(pg_engine, source_path=source_path)

    concept_name = pg_session.execute(
        sa.text("SELECT concept_name FROM concept WHERE concept_id = 1")
    ).scalar()
    assert concept_name is not None
    assert len(concept_name) == 255, (
        f"Expected 255-char name; got {len(concept_name)}: {concept_name!r}"
    )
    assert not concept_name.startswith('"'), "Surrounding quotes were not stripped"


@pytest.mark.postgres
def test_load_vocab_model_csv_on_postgres(pg_session):
    """
    _load_vocab_model_csv loads data correctly on a real PostgreSQL session.

    orm-loader>=0.4.0 handles staging-table creation internally, so we test
    the end-to-end path: CSV → staging → concept table on real Postgres.
    """
    csv_path = _FIXTURE_SOURCE / "CONCEPT.csv"

    row_count = _load_vocab_model_csv(
        pg_session,
        model=Concept,
        csv_path=csv_path,
        merge_strategy="replace",
    )
    pg_session.commit()

    assert row_count == 7
    count = pg_session.execute(sa.text("SELECT COUNT(*) FROM concept")).scalar()
    assert count == 7


@pytest.mark.postgres
def test_replace_strategy_overwrites_existing_rows(pg_session, pg_engine, tmp_path):
    """merge_strategy='replace' fully replaces rows with the same PKs on second load."""
    concept_id = 99999
    source_v1 = _make_concept_source(
        tmp_path / "v1", concept_id=concept_id, concept_name="name_v1"
    )
    source_v2 = _make_concept_source(
        tmp_path / "v2", concept_id=concept_id, concept_name="name_v2"
    )

    load_vocab_source(pg_engine, source_path=source_v1, merge_strategy="replace")
    load_vocab_source(pg_engine, source_path=source_v2, merge_strategy="replace")

    name = pg_session.execute(
        sa.text("SELECT concept_name FROM concept WHERE concept_id = :cid"),
        {"cid": concept_id},
    ).scalar()
    assert name == "name_v2", f"Expected 'name_v2' after replace, got {name!r}"


@pytest.mark.postgres
def test_upsert_strategy_is_non_destructive(pg_session, pg_engine, tmp_path):
    """merge_strategy='upsert' preserves existing rows on second load with same PKs."""
    concept_id = 99998
    source_v1 = _make_concept_source(
        tmp_path / "v1", concept_id=concept_id, concept_name="name_v1"
    )
    source_v2 = _make_concept_source(
        tmp_path / "v2", concept_id=concept_id, concept_name="name_v2"
    )

    load_vocab_source(pg_engine, source_path=source_v1, merge_strategy="upsert")
    load_vocab_source(pg_engine, source_path=source_v2, merge_strategy="upsert")

    name = pg_session.execute(
        sa.text("SELECT concept_name FROM concept WHERE concept_id = :cid"),
        {"cid": concept_id},
    ).scalar()
    assert name == "name_v1", (
        f"Expected 'name_v1' after upsert (existing row preserved), got {name!r}"
    )


@pytest.mark.postgres
def test_chunksize_forwarded_to_loader(pg_session, pg_engine, monkeypatch):
    """chunksize is forwarded from load_vocab_source through to _load_vocab_model_csv."""
    from omop_alchemy.maintenance import load_vocab as _lv_module

    received_chunksizes: list[int | None] = []
    original = _lv_module._load_vocab_model_csv

    def tracking_load(session, *, model, csv_path, merge_strategy, quote_mode="auto", chunksize=None):
        received_chunksizes.append(chunksize)
        return original(
            session,
            model=model,
            csv_path=csv_path,
            merge_strategy=merge_strategy,
            quote_mode=quote_mode,
            chunksize=chunksize,
        )

    monkeypatch.setattr(_lv_module, "_load_vocab_model_csv", tracking_load)

    load_vocab_source(pg_engine, source_path=_FIXTURE_SOURCE, chunksize=500)

    assert received_chunksizes, "Expected at least one table to be loaded"
    assert all(c == 500 for c in received_chunksizes), (
        f"Expected chunksize=500 for all tables, got: {received_chunksizes}"
    )


@pytest.mark.postgres
def test_db_schema_search_path_on_postgres(pg_engine):
    """
    load_vocab_source with db_schema creates vocabulary tables in the requested
    PostgreSQL schema and loads data into them correctly.
    """
    schema = "vocab_test"

    with pg_engine.connect() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {schema}"))
        conn.commit()

    try:
        report = load_vocab_source(
            pg_engine,
            source_path=_FIXTURE_SOURCE,
            db_schema=schema,
        )

        assert any(r.status == "loaded" for r in report.results if r.required)

        inspector = sa.inspect(pg_engine)
        assert inspector.has_table("concept", schema=schema), (
            f"Expected concept table in schema '{schema}'"
        )

        with pg_engine.connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {schema}.concept")
            ).scalar()
        assert count == 7
    finally:
        with pg_engine.connect() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.commit()
