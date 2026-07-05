"""
PostgreSQL integration tests for OMOP_Alchemy vocabulary loading.

These tests require a running PostgreSQL container. Start one with:
    docker compose -f tests/docker-compose.yaml up -d

Then run:
    pytest -m postgres
"""
from pathlib import Path

import pytest
import sqlalchemy as sa

from omop_alchemy.cdm.model.vocabulary import Concept
from omop_alchemy.config import OmopAlchemyConfig
from omop_alchemy.maintenance.cli_vocab import (
    _load_vocab_model_csv,
    load_vocab_source,
)
from tests.conftest import _ATHENA_FIXTURE_DATA, _write_fixture_csv


def _copy_fixture_source(base_dir: Path) -> Path:
    """Write the shared in-memory Athena fixture set into an isolated per-test source dir."""
    source_path = base_dir / "athena_source"
    source_path.mkdir(parents=True)
    for table_name, data in _ATHENA_FIXTURE_DATA.items():
        _write_fixture_csv(source_path, table_name, data)
    return source_path


def _make_concept_source(
    base_dir: Path,
    *,
    concept_id: int,
    concept_name: str,
) -> Path:
    """
    Build a minimal vocabulary source where CONCEPT.csv contains exactly one
    test concept with a Gender domain reference, and all other required tables
    are written from the shared in-memory fixture.
    """
    source_path = base_dir / "athena_source"
    source_path.mkdir(parents=True)

    for table_name, data in _ATHENA_FIXTURE_DATA.items():
        if table_name != "concept":
            _write_fixture_csv(source_path, table_name, data)

    concept_cols = list(_ATHENA_FIXTURE_DATA["concept"].keys())
    concept_row = [concept_id, concept_name, "Gender", "Gender", "Gender", "S", "TEST", "19700101", "20991231", None]
    _write_fixture_csv(source_path, "concept", {col: (val,) for col, val in zip(concept_cols, concept_row)})
    return source_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
def test_end_to_end_vocab_load_on_postgres(pg_session, pg_engine, tmp_path):
    """load_vocab_source() completes end-to-end on real Postgres via orm-loader>=0.4.0."""
    source_path = _copy_fixture_source(tmp_path)
    report = load_vocab_source(pg_engine, source_path=source_path)

    assert report.merge_strategy == "replace"
    assert all(r.status == "loaded" for r in report.results if r.required)
    assert all(r.status == "skipped" for r in report.results if not r.required)

    count = pg_session.execute(sa.text("SELECT COUNT(*) FROM concept")).scalar()
    assert count == 7



@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
def test_default_quote_mode_preserves_literal_quotes_on_postgres(pg_session, pg_engine, tmp_path):
    """
    The default quote_mode='by_delimiter' treats double-quotes in tab-delimited
    Athena input as literal data, not RFC-4180 field wrappers, so a
    concept_name containing double-quotes is stored verbatim via COPY.

    Athena vocabulary exports are tab-delimited and do not RFC-4180-quote
    fields, so a double-quote in the source is part of the value and must
    survive the load intact. (Callers with genuinely quote-wrapped sources can
    opt into stripping with quote_mode='csv' — see the companion test below.)
    """
    source_path = tmp_path / "athena_source"
    source_path.mkdir()

    # Begins and ends with a literal double-quote: under RFC-4180 (csv) these
    # boundary quotes would be stripped as wrappers; under the by_delimiter
    # default (tab -> literal) they are data and must be preserved verbatim.
    # Comfortably within VARCHAR(255).
    quoted_name = '"BRCA1 positive"'

    for table_name, data in _ATHENA_FIXTURE_DATA.items():
        if table_name != "concept":
            _write_fixture_csv(source_path, table_name, data)

    concept_cols = list(_ATHENA_FIXTURE_DATA["concept"].keys())
    concept_row = [1, quoted_name, "Gender", "Gender", "Gender", "S", "TEST", "19700101", "20991231", None]
    _write_fixture_csv(source_path, "concept", {col: (val,) for col, val in zip(concept_cols, concept_row)})

    # Default quote_mode resolves by_delimiter -> literal for tab-delimited input.
    load_vocab_source(pg_engine, source_path=source_path)

    concept_name = pg_session.execute(
        sa.text("SELECT concept_name FROM concept WHERE concept_id = 1")
    ).scalar()
    assert concept_name == quoted_name, (
        f"by_delimiter default must preserve literal double-quotes verbatim; got {concept_name!r}"
    )


@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
def test_explicit_csv_quote_mode_strips_quotes_on_postgres(pg_session, pg_engine, tmp_path):
    """
    Passing quote_mode='csv' explicitly still applies RFC-4180 stripping via
    PostgreSQL COPY, for sources that genuinely wrap fields in double-quotes.

    A concept_name of exactly 255 chars wrapped in quotes is 257 chars in the
    file; csv mode strips the wrappers back to 255 so it fits VARCHAR(255).
    Under the by_delimiter default this same input would overflow, which is the
    reason the opt-in exists (see CHANGELOG 0.6.3 / 0.8.1).
    """
    source_path = tmp_path / "athena_source"
    source_path.mkdir()

    long_name = "A" * 255  # exactly at the VARCHAR(255) limit once unwrapped

    for table_name, data in _ATHENA_FIXTURE_DATA.items():
        if table_name != "concept":
            _write_fixture_csv(source_path, table_name, data)

    concept_cols = list(_ATHENA_FIXTURE_DATA["concept"].keys())
    concept_row = [1, f'"{long_name}"', "Gender", "Gender", "Gender", "S", "TEST", "19700101", "20991231", None]
    _write_fixture_csv(source_path, "concept", {col: (val,) for col, val in zip(concept_cols, concept_row)})

    load_vocab_source(pg_engine, source_path=source_path, quote_mode="csv")

    concept_name = pg_session.execute(
        sa.text("SELECT concept_name FROM concept WHERE concept_id = 1")
    ).scalar()
    assert concept_name is not None
    assert len(concept_name) == 255, (
        f"Expected 255-char name after csv stripping; got {len(concept_name)}: {concept_name!r}"
    )
    assert not concept_name.startswith('"'), "csv mode should strip surrounding quotes"



@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
def test_load_vocab_model_csv_on_postgres(pg_session, tmp_path):
    """
    _load_vocab_model_csv loads data correctly on a real PostgreSQL session.

    orm-loader>=0.4.0 handles staging-table creation internally, so we test
    the end-to-end path: CSV → staging → concept table on real Postgres.
    """
    source_path = _copy_fixture_source(tmp_path)
    csv_path = source_path / "CONCEPT.csv"

    row_count = _load_vocab_model_csv(
        pg_session,
        model=Concept,  # type: ignore[arg-type]
        csv_path=csv_path,
        merge_strategy="replace",
    )
    pg_session.commit()

    assert row_count == 7
    count = pg_session.execute(sa.text("SELECT COUNT(*) FROM concept")).scalar()
    assert count == 7



@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
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



@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
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



@pytest.mark.requires_resource(OmopAlchemyConfig.TEST_DB)
def test_db_schema_search_path_on_postgres(pg_engine, tmp_path):
    """
    load_vocab_source with db_schema creates vocabulary tables in the requested
    PostgreSQL schema and loads data into them correctly.
    """
    schema = 'VocabTest'
    source_path = _copy_fixture_source(tmp_path)
    quoted_schema = '"' + schema.replace('"', '""') + '"'

    with pg_engine.connect() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        conn.execute(sa.text(f"CREATE SCHEMA {quoted_schema}"))
        conn.commit()

    try:
        report = load_vocab_source(
            pg_engine,
            source_path=source_path,
            db_schema=schema,
        )

        assert any(r.status == "loaded" for r in report.results if r.required)

        inspector = sa.inspect(pg_engine)
        assert inspector.has_table("concept", schema=schema), (
            f"Expected concept table in schema '{schema}'"
        )

        with pg_engine.connect() as conn:
            count = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {quoted_schema}.concept")
            ).scalar()
        assert count == 7
    finally:
        with pg_engine.connect() as conn:
            conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
            conn.commit()
