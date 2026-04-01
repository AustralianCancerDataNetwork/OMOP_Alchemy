import sqlalchemy as sa
from typer.testing import CliRunner

from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY, omop_index_name
from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.indexes import (
    IndexAction,
    IndexManagementResult,
    collect_index_targets,
    manage_indexes,
)
from omop_alchemy.maintenance.tables import collect_maintenance_tables
from omop_alchemy.maintenance.tables import TableCategory


runner = CliRunner()
PERSON_GENDER_INDEX = omop_index_name("person", "gender_concept_id")
CONCEPT_DOMAIN_INDEX = omop_index_name("concept", "domain_id")
EPISODE_PERSON_INDEX = omop_index_name("episode", "person_id")


def _fresh_engine(tmp_path):
    db_path = tmp_path / "indexes.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    create_missing_tables(engine)
    return engine


def test_collect_index_targets_excludes_vocabulary_by_default(tmp_path):
    """Test collect index targets excludes vocabulary by default."""
    engine = _fresh_engine(tmp_path)
    targets = {
        (target.table_name, target.index_name)
        for target in collect_index_targets(engine)
    }

    assert ("person", PERSON_GENDER_INDEX) in targets
    assert ("concept", CONCEPT_DOMAIN_INDEX) not in targets


def test_collect_index_targets_can_include_vocabulary(tmp_path):
    """Test collect index targets can include vocabulary."""
    engine = _fresh_engine(tmp_path)
    targets = {
        (target.table_name, target.index_name)
        for target in collect_index_targets(engine, vocabulary_included=True)
    }

    assert ("concept", CONCEPT_DOMAIN_INDEX) in targets


def test_orm_index_metadata_carries_cluster_configuration():
    """Test orm index metadata carries cluster configuration."""
    tables = {
        table.table_name: table
        for table in collect_maintenance_tables()
    }

    person = tables["person"]
    assert person.table.info[OMOP_CLUSTER_INDEX_INFO_KEY] == "pk_person"

    episode = tables["episode"]
    episode_indexes = {
        index.name: index
        for index in episode.table.indexes
    }
    assert episode_indexes[EPISODE_PERSON_INDEX].info[OMOP_CLUSTER_INDEX_INFO_KEY] is True


def test_manage_indexes_disable_and_enable_on_sqlite(tmp_path):
    """Test manage indexes disable and enable on sqlite."""
    engine = _fresh_engine(tmp_path)

    inspector = sa.inspect(engine)
    before = {
        index["name"]
        for index in inspector.get_indexes("person")
    }
    assert PERSON_GENDER_INDEX in before

    disabled = manage_indexes(
        engine,
        action=IndexAction.DISABLE,
    )
    assert disabled

    inspector = sa.inspect(engine)
    after_disable = {
        index["name"]
        for index in inspector.get_indexes("person")
    }
    assert PERSON_GENDER_INDEX not in after_disable

    enabled = manage_indexes(
        engine,
        action=IndexAction.ENABLE,
    )
    assert enabled
    assert any(
        result.operation == "cluster" and result.status == "skipped"
        for result in enabled
    )
    assert any(
        "unsupported on SQLite" in result.detail
        for result in enabled
        if result.operation == "cluster"
    )

    inspector = sa.inspect(engine)
    after_enable = {
        index["name"]
        for index in inspector.get_indexes("person")
    }
    assert PERSON_GENDER_INDEX in after_enable


def test_disable_indexes_cli_invokes_management(monkeypatch):
    """Test disable indexes cli invokes management."""
    calls: dict[str, object] = {}

    def fake_load_environment(dotenv: str) -> None:
        calls["dotenv"] = dotenv

    def fake_get_engine_name(schema: str | None = None) -> str:
        calls["engine_schema"] = schema
        return "postgresql+psycopg://example"

    def fake_create_engine(url: str, *, future: bool) -> str:
        calls["engine_url"] = url
        calls["future"] = future
        return "ENGINE"

    def fake_manage_indexes(
        engine: object,
        *,
        action: IndexAction,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
    ) -> list[IndexManagementResult]:
        calls["engine"] = engine
        calls["action"] = action
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
        return [
            IndexManagementResult(
                operation="index",
                table_name="person",
                category=TableCategory.CLINICAL,
                model_name="Person",
                model_module="omop_alchemy.cdm.model.clinical.person",
                index_name=PERSON_GENDER_INDEX,
                column_names=("gender_concept_id",),
                unique=False,
                clustered=False,
                action=IndexAction.DISABLE,
                status="planned",
                detail="metadata-defined index would be dropped",
            )
        ]

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
        "omop_alchemy.maintenance.cli.manage_indexes",
        fake_manage_indexes,
    )

    result = runner.invoke(
        app,
        [
            "indexes",
            "disable",
            "--dotenv",
            ".env.test",
            "--engine-schema",
            "cdm",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "indexes disable" in result.stdout
    assert "person" in result.stdout
    assert "disable" in result.stdout
    assert "PLANNED" in result.stdout
    assert "Planned disable on 1 metadata operation(s)." in result.stdout
