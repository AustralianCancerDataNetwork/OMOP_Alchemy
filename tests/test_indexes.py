import pytest
import sqlalchemy as sa
from typer.testing import CliRunner
from oa_configurator import StackConfig, DatabaseConfig, ResourceConfig

from omop_alchemy.backends.sqlite import SQLiteBackend
from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY, omop_index_name
from omop_alchemy.maintenance.cli import app
from omop_alchemy.maintenance.cli_schema import create_missing_tables
from omop_alchemy.maintenance.cli_indexes import (
    IndexManagementResult,
    _schema_metadata_indexes,
    collect_index_targets,
    manage_indexes,
)
from omop_alchemy.maintenance.tables import collect_maintenance_tables
from omop_alchemy.maintenance.tables import TableCategory
from omop_alchemy.maintenance.tables import select_omop_tables


runner = CliRunner()
PERSON_GENDER_INDEX = omop_index_name("person", "gender_concept_id")
CONCEPT_DOMAIN_INDEX = omop_index_name("concept", "domain_id")
EPISODE_PERSON_INDEX = omop_index_name("episode", "person_id")
CONCEPT_NAME_LOWER_INDEX = "ix_concept_concept_name_lower"
CONCEPT_SYNONYM_NAME_LOWER_INDEX = "ix_concept_synonym_concept_synonym_name_lower"


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


@pytest.mark.filterwarnings(
    "ignore:Skipped unsupported reflection of expression-based index:sqlalchemy.exc.SAWarning"
)
def test_collect_index_targets_can_include_vocabulary(tmp_path):
    """Test collect index targets can include vocabulary.
    
    Notes
    -----
    collect_index_targets relies on SQLAlchemy's reflection, which cannot describe
    expression-based indexes on SQLite (e.g. concept.py ix_concept_concept_name_lower).
    The lower(concept_name) index is invisible to collect_index_targets even though it exists. 
    See test_manage_indexes_enable_is_idempotent_for_expression_indexes
    for coverage of the indexes themselves.
    
    """
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
    assert episode_indexes[EPISODE_PERSON_INDEX].info[OMOP_CLUSTER_INDEX_INFO_KEY] is True  # type: ignore[index]


def test_schema_metadata_indexes_keys_match_unadjusted_indexes():
    """Test schema metadata indexes keys match unadjusted indexes.

    manage_indexes() looks up schema-adjusted indexes using names taken from
    the original, unadjusted ORM tables. If a column's index name is resolved
    implicitly (e.g. via `index=True` rather than an explicit omop_index()),
    SQLAlchemy's naming convention embeds the schema into the generated name,
    so the schema-adjusted copy gets a different name and the lookup misses.
    """
    tables = select_omop_tables(vocabulary_included=True)
    indexes = _schema_metadata_indexes(tables, db_schema="public")

    for table in tables:
        for index in table.table.indexes:
            assert (table.table_name, str(index.name)) in indexes


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
        enable=False,
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
        enable=True,
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


def test_manage_indexes_enable_analyzes_tables_with_new_indexes(tmp_path, monkeypatch):
    """Test manage indexes enable analyzes tables with new indexes."""
    engine = _fresh_engine(tmp_path)
    manage_indexes(engine, enable=False)

    analyzed_tables: list[str] = []
    original_analyze = SQLiteBackend.analyze_table

    def recording_analyze(self, conn, table_name, db_schema, *, vacuum=False):
        analyzed_tables.append(table_name)
        return original_analyze(self, conn, table_name, db_schema, vacuum=vacuum)

    monkeypatch.setattr(SQLiteBackend, "analyze_table", recording_analyze)

    manage_indexes(engine, enable=True)

    assert "person" in analyzed_tables


def test_manage_indexes_enable_skips_analyze_when_nothing_created(tmp_path, monkeypatch):
    """Test manage indexes enable skips analyze when nothing created."""
    engine = _fresh_engine(tmp_path)

    analyzed_tables: list[str] = []
    monkeypatch.setattr(
        SQLiteBackend,
        "analyze_table",
        lambda self, conn, table_name, db_schema, *, vacuum=False: analyzed_tables.append(table_name),
    )

    # All ORM-defined indexes already exist on a freshly created schema, so
    # enabling again should be a no-op and must not trigger any ANALYZE calls.
    manage_indexes(engine, enable=True)

    assert analyzed_tables == []


@pytest.mark.filterwarnings(
    "ignore:Skipped unsupported reflection of expression-based index:sqlalchemy.exc.SAWarning"
)
def test_manage_indexes_enable_is_idempotent_for_expression_indexes(tmp_path):
    """Test manage indexes enable is idempotent for expression indexes.

    SQLite cannot reflect expression-based indexes (e.g. lower(concept_name)),
    so manage_indexes can never see them as already existing via
    inspector.get_indexes(). Re-running 'enable' must not crash on the
    resulting duplicate-create attempt and must report it as skipped rather
    than falsely claiming the index was (re)created.
    """
    engine = _fresh_engine(tmp_path)

    for _ in range(2):
        results = manage_indexes(engine, enable=True, vocabulary_included=True)
        lower_index_results = {
            result.index_name: result
            for result in results
            if result.index_name in (CONCEPT_NAME_LOWER_INDEX, CONCEPT_SYNONYM_NAME_LOWER_INDEX)
        }
        assert set(lower_index_results) == {
            CONCEPT_NAME_LOWER_INDEX,
            CONCEPT_SYNONYM_NAME_LOWER_INDEX,
        }
        for result in lower_index_results.values():
            assert result.status == "skipped"
            assert "already exists" in result.detail


@pytest.mark.filterwarnings(
    "ignore:Skipped unsupported reflection of expression-based index:sqlalchemy.exc.SAWarning"
)
def test_manage_indexes_disable_drops_expression_indexes_on_sqlite(tmp_path):
    """Test manage indexes disable drops expression indexes on sqlite.

    SQLite can't reflect expression-based indexes, so `disable` must not gate
    the drop on inspector.get_indexes() (`exists` would always be False) and
    must not rely on Index.drop(checkfirst=True) either, since checkfirst
    does its own reflection-based check internally and would silently no-op
    the same way. The index must actually be removed, and a result row must
    always be reported.
    """
    engine = _fresh_engine(tmp_path)

    def _index_exists(name: str) -> bool:
        with engine.connect() as connection:
            row = connection.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (name,),
            ).fetchone()
        return row is not None

    assert _index_exists(CONCEPT_NAME_LOWER_INDEX)
    assert _index_exists(CONCEPT_SYNONYM_NAME_LOWER_INDEX)

    results = manage_indexes(engine, enable=False, vocabulary_included=True)
    lower_index_results = {
        result.index_name: result
        for result in results
        if result.index_name in (CONCEPT_NAME_LOWER_INDEX, CONCEPT_SYNONYM_NAME_LOWER_INDEX)
    }
    assert set(lower_index_results) == {
        CONCEPT_NAME_LOWER_INDEX,
        CONCEPT_SYNONYM_NAME_LOWER_INDEX,
    }
    for result in lower_index_results.values():
        assert result.status == "applied"

    assert not _index_exists(CONCEPT_NAME_LOWER_INDEX)
    assert not _index_exists(CONCEPT_SYNONYM_NAME_LOWER_INDEX)


@pytest.mark.filterwarnings(
    "ignore:Skipped unsupported reflection of expression-based index:sqlalchemy.exc.SAWarning"
)
def test_manage_indexes_disable_is_idempotent_on_sqlite(tmp_path):
    """A second disable run should report already-absent indexes as skipped."""
    engine = _fresh_engine(tmp_path)

    first_results = manage_indexes(engine, enable=False, vocabulary_included=True)
    second_results = manage_indexes(engine, enable=False, vocabulary_included=True)

    first_lower = {
        result.index_name: result
        for result in first_results
        if result.index_name in (CONCEPT_NAME_LOWER_INDEX, CONCEPT_SYNONYM_NAME_LOWER_INDEX)
    }
    second_lower = {
        result.index_name: result
        for result in second_results
        if result.index_name in (CONCEPT_NAME_LOWER_INDEX, CONCEPT_SYNONYM_NAME_LOWER_INDEX)
    }

    for result in first_lower.values():
        assert result.status == "applied"

    for result in second_lower.values():
        assert result.status == "skipped"
        assert "already absent" in result.detail


def test_manage_indexes_enable_clusters_then_analyzes(tmp_path, monkeypatch):
    """Test manage indexes enable clusters then analyzes, even with nothing created.

    `indexes enable --cluster` must ANALYZE a table whenever it was clustered,
    not only when a new index was created (a table can be selected for
    clustering with all its indexes already in place), and must do so *after*
    clustering so planner stats reflect the final physical layout -- matching
    the standalone `indexes cluster` command's order.
    """
    engine = _fresh_engine(tmp_path)

    calls: list[str] = []

    def fake_cluster_table(self, conn, table_name, index_name, db_schema):
        calls.append(f"cluster:{table_name}")

    def fake_analyze_table(self, conn, table_name, db_schema, *, vacuum=False):
        calls.append(f"analyze:{table_name}")

    monkeypatch.setattr(SQLiteBackend, "cluster_table", fake_cluster_table)
    monkeypatch.setattr(SQLiteBackend, "analyze_table", fake_analyze_table)

    # All ORM-defined indexes already exist on a freshly created schema, so
    # `created_any` stays False -- only the cluster step causes any change.
    manage_indexes(engine, enable=True, cluster=True)

    assert "cluster:person" in calls
    assert "analyze:person" in calls
    assert calls.index("cluster:person") < calls.index("analyze:person")


def test_disable_indexes_cli_invokes_management(monkeypatch):
    """Test disable indexes cli invokes management."""

    calls: dict[str, object] = {}

    cfg = StackConfig.for_session(
        databases={"db": DatabaseConfig(dialect="sqlite", database_name=":memory:")},
        resources={"cdm_db": ResourceConfig(database="db", cdm_schema="main")},
    )
    monkeypatch.setattr(
        "omop_alchemy.config.load_stack_config",
        lambda: cfg,
    )

    def fake_manage_indexes(
        engine: object,
        *,
        enable: bool,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
    ) -> list[IndexManagementResult]:
        calls["engine"] = engine
        calls["enable"] = enable
        calls["db_schema"] = db_schema
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
        return [
            IndexManagementResult(
                operation="index",
                table_name="person",
                category=TableCategory.CLINICAL,
                index_name=PERSON_GENDER_INDEX,
                column_names=("gender_concept_id",),
                unique=False,
                clustered=False,
                enable=enable,
                status="planned",
                detail="metadata-defined index would be dropped",
            )
        ]

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli_indexes.manage_indexes",
        fake_manage_indexes,
    )

    result = runner.invoke(
        app,
        [
            "indexes",
            "disable",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "indexes disable" in result.stdout
    assert "person" in result.stdout
    assert "disable" in result.stdout
    assert "PLANNED" in result.stdout
    assert "Planned disable on 1 metadata operation(s)." in result.stdout


def test_enable_indexes_cli_no_cluster_flag_passes_through(monkeypatch):
    """Test enable indexes cli no cluster flag passes through."""

    calls: dict[str, object] = {}

    cfg = StackConfig.for_session(
        databases={"db": DatabaseConfig(dialect="sqlite", database_name=":memory:")},
        resources={"cdm_db": ResourceConfig(database="db", cdm_schema="main")},
    )
    monkeypatch.setattr(
        "omop_alchemy.config.load_stack_config",
        lambda: cfg,
    )

    def fake_manage_indexes(
        engine: object,
        *,
        enable: bool,
        db_schema: str | None = None,
        vocabulary_included: bool = False,
        dry_run: bool = False,
        cluster: bool = True,
    ) -> list[IndexManagementResult]:
        calls["enable"] = enable
        calls["vocabulary_included"] = vocabulary_included
        calls["dry_run"] = dry_run
        calls["cluster"] = cluster
        return [
            IndexManagementResult(
                operation="index",
                table_name="person",
                category=TableCategory.CLINICAL,
                index_name=PERSON_GENDER_INDEX,
                column_names=("gender_concept_id",),
                unique=False,
                clustered=False,
                enable=enable,
                status="planned",
                detail="metadata-defined index would be created",
            )
        ]

    monkeypatch.setattr(
        "omop_alchemy.maintenance.cli_indexes.manage_indexes",
        fake_manage_indexes,
    )

    result = runner.invoke(
        app,
        [
            "indexes",
            "enable",
            "--no-cluster",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert calls["cluster"] is False
    assert calls["enable"] is True
    assert calls["dry_run"] is True
