from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from typer.testing import CliRunner

from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.defaults import defaults_path
from omop_alchemy.maintenance.load_vocab import (
    OPTIONAL_VOCAB_MODELS,
    REQUIRED_VOCAB_MODELS,
    _load_vocab_model_csv,
    load_vocab_source,
)
from omop_alchemy.cdm.model.vocabulary import Drug_Strength


runner = CliRunner()


def _athena_source_path() -> Path:
    return Path(__file__).parent / "fixtures" / "athena_source"


def _write_athena_csv(source_path: Path, table_name: str) -> Path:
    csv_path = source_path / f"{table_name.upper()}.csv"
    csv_path.write_text("stub\n", encoding="utf-8")
    return csv_path


def _build_required_athena_source(
    tmp_path: Path,
    *,
    include_optional: tuple[str, ...] = (),
) -> Path:
    source_path = tmp_path / "athena_source"
    source_path.mkdir()

    for model in REQUIRED_VOCAB_MODELS:
        _write_athena_csv(source_path, model.__tablename__)

    for table_name in include_optional:
        _write_athena_csv(source_path, table_name)

    return source_path


def _write_csv_with_size(source_path: Path, table_name: str, size_bytes: int) -> Path:
    csv_path = source_path / f"{table_name.upper()}.csv"
    csv_path.write_text("x" * size_bytes, encoding="utf-8")
    return csv_path


def test_load_vocab_source_on_sqlite_creates_tables_and_reports_loaded_results(
    monkeypatch,
    tmp_path,
):
    """Test load vocab source on sqlite creates tables and reports loaded results."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source.db'}", future=True)
    source_path = _build_required_athena_source(tmp_path)
    loaded_tables: list[tuple[str, str, str, Path]] = []

    def fake_load_vocab_model_csv(
        session,
        *,
        model,
        csv_path,
        merge_strategy,
        quote_mode="csv",
    ) -> int:
        loaded_tables.append((model.__tablename__, merge_strategy, quote_mode, csv_path))
        return 1

    monkeypatch.setattr(
        "omop_alchemy.maintenance.load_vocab._load_vocab_model_csv",
        fake_load_vocab_model_csv,
    )

    report = load_vocab_source(engine, source_path=source_path)

    result_by_name = {
        result.table_name: result
        for result in report.results
    }

    assert report.merge_strategy == "upsert"
    assert all(result_by_name[model.__tablename__].status == "loaded" for model in REQUIRED_VOCAB_MODELS)
    assert all(result_by_name[model.__tablename__].status == "skipped" for model in OPTIONAL_VOCAB_MODELS)
    assert all(merge_strategy == "upsert" for _, merge_strategy, _, _ in loaded_tables)
    assert all(quote_mode == "literal" for _, _, quote_mode, _ in loaded_tables)
    assert {table_name for table_name, _, _, _ in loaded_tables} == {
        model.__tablename__
        for model in REQUIRED_VOCAB_MODELS
    }

    inspector = sa.inspect(engine)
    assert inspector.has_table("concept")


def test_load_vocab_source_requires_full_required_athena_fixture(tmp_path):
    """Test load vocab source requires full required athena fixture."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_missing_required.db'}", future=True)

    with pytest.raises(RuntimeError) as exc_info:
        load_vocab_source(
            engine,
            source_path=_athena_source_path(),
        )

    assert "Missing required Athena vocabulary CSV files" in str(exc_info.value)


def test_drug_strength_model_matches_athena_vocabulary_shape():
    """Test drug strength model matches athena vocabulary shape."""
    column_names = set(Drug_Strength.__table__.columns.keys())

    assert "valid_start_date" in column_names
    assert "valid_end_date" in column_names
    assert "start_date" not in column_names
    assert "start_datetime" not in column_names
    assert "end_date" not in column_names
    assert "end_datetime" not in column_names


def test_load_vocab_source_dry_run_does_not_create_tables(tmp_path):
    """Test load vocab source dry run does not create tables."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_dry_run.db'}", future=True)
    source_path = _build_required_athena_source(tmp_path)

    report = load_vocab_source(
        engine,
        source_path=source_path,
        dry_run=True,
    )

    assert all(result.status == "planned" for result in report.results if result.required)
    assert report.created_table_count == 0
    inspector = sa.inspect(engine)
    assert not inspector.has_table("concept")


def test_load_vocab_source_cli_uses_saved_athena_source(monkeypatch):
    """Test load vocab source cli uses saved athena source."""
    calls: dict[str, object] = {}

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "sqlite:///:memory:"

    def fake_create_engine(url: str, *, future: bool) -> str:
        calls["engine_url"] = url
        calls["future"] = future
        return "ENGINE"

    def fake_load_vocab_source(
        engine: object,
        *,
        source_path: str | Path,
        db_schema: str | None = None,
        dry_run: bool = False,
        merge_strategy: str = "upsert",
        progress_callback=None,
    ):
        from omop_alchemy.maintenance.load_vocab import VocabularyLoadReport, VocabularyLoadResult

        calls["engine"] = engine
        calls["source_path"] = str(source_path)
        calls["db_schema"] = db_schema
        calls["dry_run"] = dry_run
        calls["merge_strategy"] = merge_strategy
        return VocabularyLoadReport(
            source_path=str(source_path),
            backend="sqlite",
            db_schema=db_schema,
            merge_strategy=merge_strategy,
            created_table_count=0,
            sequence_reset_count=0,
            results=(
                VocabularyLoadResult(
                    table_name="concept",
                    status="planned",
                    row_count=None,
                    csv_path=str(Path(source_path) / "CONCEPT.csv"),
                    required=True,
                    detail="Athena CSV would be loaded via staged ORM CSV loader",
                ),
            ),
        )

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_environment",
        fake_load_environment,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.get_engine_name",
        fake_get_engine_name,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.create_engine_with_dependencies",
        fake_create_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_vocab_source",
        fake_load_vocab_source,
    )

    with runner.isolated_filesystem():
        athena_dir = Path("athena_source")
        athena_dir.mkdir()
        set_result = runner.invoke(
            app,
            [
                "config",
                "set-overrides",
                "--athena-source",
                str(athena_dir),
                "--engine-schema",
                "cdm",
            ],
        )
        assert set_result.exit_code == 0

        result = runner.invoke(
            app,
            ["load-vocab-source", "--dry-run"],
        )

        expected_source_path = str((defaults_path().parent / "athena_source").resolve())
        assert result.exit_code == 0
        assert calls["engine"] == "ENGINE"
        assert calls["source_path"] == expected_source_path
        assert calls["merge_strategy"] == "upsert"
        assert "load-vocab-source" in result.stdout
        assert "concept" in result.stdout


def test_load_vocab_model_csv_passes_quote_mode(monkeypatch, tmp_path):
    """Test load vocab model csv passes quote mode."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_quote_mode.db'}", future=True)

    class FakeModel:
        __tablename__ = "concept"

        @staticmethod
        def staging_tablename() -> str:
            return "_staging_concept"

        @staticmethod
        def load_csv(session, path, *, merge_strategy, quote_mode):
            return 7

        @staticmethod
        def create_staging_table(session):
            raise NotImplementedError

    calls: dict[str, object] = {}

    def fake_load_csv(session, path, *, merge_strategy, quote_mode):
        calls["merge_strategy"] = merge_strategy
        calls["quote_mode"] = quote_mode
        calls["path"] = path
        return 7

    monkeypatch.setattr(FakeModel, "load_csv", fake_load_csv)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        row_count = _load_vocab_model_csv(
            session,
            model=FakeModel,
            csv_path=_athena_source_path() / "CONCEPT.csv",
            merge_strategy="upsert",
            quote_mode="literal",
        )

    assert row_count == 7
    assert calls["merge_strategy"] == "upsert"
    assert calls["quote_mode"] == "literal"


def test_load_vocab_source_loads_smallest_files_first(monkeypatch, tmp_path):
    """Test load vocab source loads smallest files first."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_order.db'}", future=True)
    source_path = _build_required_athena_source(tmp_path)

    for model in REQUIRED_VOCAB_MODELS:
        _write_csv_with_size(source_path, model.__tablename__, 500)

    _write_csv_with_size(source_path, "domain", 10)
    _write_csv_with_size(source_path, "vocabulary", 200)
    _write_csv_with_size(source_path, "concept_class", 50)

    loaded_order: list[str] = []

    def fake_load_vocab_model_csv(
        session,
        *,
        model,
        csv_path,
        merge_strategy,
        quote_mode="csv",
    ) -> int:
        loaded_order.append(model.__tablename__)
        return 1

    monkeypatch.setattr(
        "omop_alchemy.maintenance.load_vocab._load_vocab_model_csv",
        fake_load_vocab_model_csv,
    )

    load_vocab_source(engine, source_path=source_path)

    assert loaded_order[:3] == ["domain", "concept_class", "vocabulary"]


def test_load_vocab_source_reports_weighted_progress(monkeypatch, tmp_path):
    """Test load vocab source reports weighted progress."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_progress.db'}", future=True)
    source_path = _build_required_athena_source(tmp_path)

    _write_csv_with_size(source_path, "domain", 10)
    _write_csv_with_size(source_path, "vocabulary", 40)

    events: list[object] = []

    def fake_load_vocab_model_csv(
        session,
        *,
        model,
        csv_path,
        merge_strategy,
        quote_mode="csv",
    ) -> int:
        return 1

    monkeypatch.setattr(
        "omop_alchemy.maintenance.load_vocab._load_vocab_model_csv",
        fake_load_vocab_model_csv,
    )

    load_vocab_source(
        engine,
        source_path=source_path,
        progress_callback=events.append,
    )

    assert events
    percents = [event.percent for event in events]
    assert percents[0] == pytest.approx(0.0)
    assert percents[-1] == pytest.approx(100.0)
    assert percents == sorted(percents)


def test_load_vocab_source_wraps_failed_table_load(monkeypatch, tmp_path):
    """Test load vocab source wraps failed table load."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_error.db'}", future=True)
    source_path = _build_required_athena_source(tmp_path)

    def fake_load_vocab_model_csv(session, *, model, csv_path, merge_strategy, quote_mode="csv"):
        if model.__tablename__ == "domain":
            raise sa.exc.ProgrammingError(
                "COPY domain FROM STDIN",
                {},
                Exception("value too long for type character varying(255)"),
            )
        return 1

    monkeypatch.setattr(
        "omop_alchemy.maintenance.load_vocab._load_vocab_model_csv",
        fake_load_vocab_model_csv,
    )

    with pytest.raises(RuntimeError) as exc_info:
        load_vocab_source(
            engine,
            source_path=source_path,
        )

    message = str(exc_info.value)
    assert "table `domain`" in message
    assert "merge strategy `upsert`" in message
    assert "ProgrammingError" in message
    assert "value too long for type character varying(255)" in message


def test_load_vocab_model_csv_retries_missing_staging_table(monkeypatch, tmp_path):
    """Test load vocab model csv retries missing staging table."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'load_vocab_source_retry.db'}", future=True)

    class FakeModel:
        __tablename__ = "drug_strength"

        @staticmethod
        def staging_tablename() -> str:
            return "_staging_drug_strength"

        @staticmethod
        def load_csv(session, path, *, merge_strategy, quote_mode):
            raise NotImplementedError

        @staticmethod
        def create_staging_table(session):
            raise NotImplementedError

    calls = {"load_csv": 0, "create_staging_table": 0}

    def fake_load_csv(session, path, *, merge_strategy, quote_mode):
        calls["load_csv"] += 1
        if calls["load_csv"] == 1:
            raise sa.exc.ProgrammingError(
                'INSERT INTO _staging_drug_strength ("drug_concept_id") VALUES (1)',
                {},
                Exception('relation "_staging_drug_strength" does not exist'),
            )
        return 123

    def fake_create_staging_table(session):
        calls["create_staging_table"] += 1

    monkeypatch.setattr(FakeModel, "load_csv", fake_load_csv)
    monkeypatch.setattr(FakeModel, "create_staging_table", fake_create_staging_table)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        row_count = _load_vocab_model_csv(
            session,
            model=FakeModel,
            csv_path=_athena_source_path() / "DRUG_STRENGTH.csv",
            merge_strategy="upsert",
        )

    assert row_count == 123
    assert calls["load_csv"] == 2
    assert calls["create_staging_table"] == 1


def test_load_vocab_source_cli_surfaces_database_error_detail(monkeypatch):
    """Test load vocab source cli surfaces database error detail."""
    def fake_build_engine(*, dotenv: str | None, engine_schema: str | None):
        return "ENGINE"

    def fail_load_vocab_source(*args, **kwargs):
        raise sa.exc.ProgrammingError(
            "COPY concept FROM STDIN",
            {},
            Exception("value too long for type character varying(255)"),
        )

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli._build_engine",
        fake_build_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.load_vocab_source",
        fail_load_vocab_source,
    )

    result = runner.invoke(
        app,
        [
            "load-vocab-source",
            "--athena-source",
            str(_athena_source_path()),
        ],
    )

    assert result.exit_code == 1
    assert "Database operation failed: ProgrammingError." in result.stdout
    assert "value too long for type character varying(255)" in result.stdout
