import sqlalchemy as sa
import pytest
from typer.testing import CliRunner

from omop_alchemy.cdm.handlers.fulltext import (
    CONCEPT_NAME_TSVECTOR_COLUMN,
    CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN,
    FullTextAction,
    FullTextResult,
    concept_name_tsvector_expression,
    drop_fulltext_columns,
    install_fulltext_columns,
    populate_fulltext_columns,
    register_optional_fulltext_columns,
    unregister_optional_fulltext_columns,
)
from omop_alchemy.cdm.model.vocabulary.concept import Concept
from omop_alchemy.cdm.model.vocabulary.concept_synonym import Concept_Synonym
from omop_alchemy.maintenance.cli import app


runner = CliRunner()


class _FakeDialect:
    name = "postgresql"


class _FakeResult:
    def __init__(self, rowcount: int | None = None):
        self.rowcount = rowcount


class _FakeConnection:
    def __init__(self, *, rowcount: int = 7):
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []
        self.rowcount = rowcount

    def exec_driver_sql(
        self,
        statement: str,
        parameters: dict[str, object] | None = None,
    ) -> _FakeResult:
        self.calls.append(("driver", " ".join(statement.split()), parameters))
        return _FakeResult()

    def execute(
        self,
        statement: sa.Executable,
        parameters: dict[str, object] | None = None,
    ) -> _FakeResult:
        self.calls.append(("execute", " ".join(str(statement).split()), parameters))
        return _FakeResult(self.rowcount)


class _FakeBegin:
    def __init__(self, connection: _FakeConnection):
        self.connection = connection

    def __enter__(self) -> _FakeConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeEngine:
    dialect = _FakeDialect()

    def __init__(self, *, rowcount: int = 7):
        self.connection = _FakeConnection(rowcount=rowcount)

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self.connection)


def test_register_and_unregister_optional_fulltext_columns_toggle_metadata():
    """Register/unregister helpers toggle optional tsvector metadata columns."""
    unregister_optional_fulltext_columns()
    assert CONCEPT_NAME_TSVECTOR_COLUMN not in Concept.__table__.c
    assert CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN not in Concept_Synonym.__table__.c

    register_optional_fulltext_columns()
    assert CONCEPT_NAME_TSVECTOR_COLUMN in Concept.__table__.c
    assert CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN in Concept_Synonym.__table__.c

    unregister_optional_fulltext_columns()
    assert CONCEPT_NAME_TSVECTOR_COLUMN not in Concept.__table__.c
    assert CONCEPT_SYNONYM_NAME_TSVECTOR_COLUMN not in Concept_Synonym.__table__.c


def test_concept_name_tsvector_expression_prefers_registered_column():
    """Expression helper falls back to computed SQL unless the stored column is registered."""
    unregister_optional_fulltext_columns()
    fallback = concept_name_tsvector_expression()
    assert "to_tsvector" in str(fallback)

    register_optional_fulltext_columns()
    try:
        stored = concept_name_tsvector_expression()
        assert stored is Concept.__table__.c[CONCEPT_NAME_TSVECTOR_COLUMN]
    finally:
        unregister_optional_fulltext_columns()


def test_install_fulltext_columns_builds_postgresql_ddl_and_registers_metadata():
    """Install emits expected PostgreSQL DDL and registers optional metadata columns."""
    unregister_optional_fulltext_columns()
    engine = _FakeEngine()

    results = install_fulltext_columns(
        engine,
        db_schema="public",
        create_indexes=True,
        fastupdate=True,
    )

    assert [result.action for result in results] == [FullTextAction.INSTALL, FullTextAction.INSTALL]
    assert all(result.status == "applied" for result in results)
    statements = [call[1] for call in engine.connection.calls]
    assert any(
        "ALTER TABLE public.concept ADD COLUMN IF NOT EXISTS concept_name_tsvector tsvector" in statement
        for statement in statements
    )
    assert any(
        "CREATE INDEX IF NOT EXISTS public.idx_gin_concept_name_tsvector" in statement
        for statement in statements
    )
    assert CONCEPT_NAME_TSVECTOR_COLUMN in Concept.__table__.c
    unregister_optional_fulltext_columns()


def test_populate_fulltext_columns_issues_update_with_regconfig_and_row_counts():
    """Populate issues parameterized UPDATE statements and reports row counts."""
    unregister_optional_fulltext_columns()
    engine = _FakeEngine(rowcount=11)

    results = populate_fulltext_columns(
        engine,
        db_schema="public",
        regconfig="simple",
    )

    assert all(result.status == "applied" for result in results)
    assert [result.row_count for result in results] == [11, 11]
    execute_calls = [call for call in engine.connection.calls if call[0] == "execute"]
    assert any("UPDATE public.concept" in call[1] for call in execute_calls)
    assert any("CAST(:regconfig AS regconfig)" in call[1] for call in execute_calls)
    assert all(call[2] == {"regconfig": "simple"} for call in execute_calls)
    unregister_optional_fulltext_columns()


def test_drop_fulltext_columns_drops_schema_objects_and_unregisters_metadata():
    """Drop removes fulltext schema objects and unregisters optional metadata columns."""
    register_optional_fulltext_columns()
    engine = _FakeEngine()

    results = drop_fulltext_columns(
        engine,
        db_schema="public",
        drop_indexes=True,
    )

    assert [result.action for result in results] == [FullTextAction.DROP, FullTextAction.DROP]
    assert all(result.status == "applied" for result in results)
    statements = [call[1] for call in engine.connection.calls]
    assert any("DROP INDEX IF EXISTS public.idx_gin_concept_name_tsvector" in statement for statement in statements)
    assert any(
        "ALTER TABLE public.concept DROP COLUMN IF EXISTS concept_name_tsvector" in statement
        for statement in statements
    )
    assert CONCEPT_NAME_TSVECTOR_COLUMN not in Concept.__table__.c


@pytest.mark.parametrize(
    "fn_name",
    [
        "install_fulltext_columns",
        "populate_fulltext_columns",
        "drop_fulltext_columns",
    ],
)
def test_fulltext_management_requires_postgresql(tmp_path, fn_name):
    """Fulltext management APIs reject non-PostgreSQL engines."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'fulltext.db'}", future=True)
    fn = {
        "install_fulltext_columns": install_fulltext_columns,
        "populate_fulltext_columns": populate_fulltext_columns,
        "drop_fulltext_columns": drop_fulltext_columns,
    }[fn_name]

    with pytest.raises(RuntimeError) as exc_info:
        fn(engine)

    assert "only supported for PostgreSQL engines" in str(exc_info.value)


def test_fulltext_install_cli_passes_options(monkeypatch):
    """CLI forwards install options to the fulltext handler implementation."""
    calls: dict[str, object] = {}

    def fake_build_engine(*, dotenv: str | None, engine_schema: str | None):
        calls["dotenv"] = dotenv
        calls["engine_schema"] = engine_schema
        return "ENGINE"

    def fake_install_fulltext_columns(
        engine: object,
        *,
        db_schema: str | None = None,
        create_indexes: bool = True,
        fastupdate: bool = False,
        dry_run: bool = False,
    ):
        calls["engine"] = engine
        calls["db_schema"] = db_schema
        calls["create_indexes"] = create_indexes
        calls["fastupdate"] = fastupdate
        calls["dry_run"] = dry_run
        return (
            FullTextResult(
                target_name="concept",
                table_name="concept",
                source_column_name="concept_name",
                vector_column_name=CONCEPT_NAME_TSVECTOR_COLUMN,
                index_name="idx_gin_concept_name_tsvector",
                action=FullTextAction.INSTALL,
                status="planned",
                detail="tsvector column and GIN index would be installed",
            ),
        )

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli._build_engine",
        fake_build_engine,
    )
    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli.install_fulltext_columns",
        fake_install_fulltext_columns,
    )

    result = runner.invoke(
        app,
        [
            "fulltext",
            "install",
            "--dry-run",
            "--db-schema",
            "public",
            "--fastupdate",
        ],
    )

    assert result.exit_code == 0
    assert calls["engine"] == "ENGINE"
    assert calls["db_schema"] == "public"
    assert calls["fastupdate"] is True
    assert calls["dry_run"] is True
    assert "fulltext install" in result.stdout
