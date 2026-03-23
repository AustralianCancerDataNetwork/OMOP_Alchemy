import sqlalchemy as sa

from omop_alchemy.cdm.base.indexing import OMOP_CLUSTER_INDEX_INFO_KEY
from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.indexes import (
    IndexAction,
    collect_index_targets,
    manage_indexes,
)
from omop_alchemy.maintenance.tables import collect_maintenance_tables


def _fresh_engine(tmp_path):
    db_path = tmp_path / "indexes.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    create_missing_tables(engine)
    return engine


def test_collect_index_targets_excludes_vocabulary_by_default(tmp_path):
    engine = _fresh_engine(tmp_path)
    targets = {
        (target.table_name, target.index_name)
        for target in collect_index_targets(engine)
    }

    assert ("person", "idx_gender") in targets
    assert ("concept", "idx_concept_domain_id") not in targets


def test_collect_index_targets_can_include_vocabulary(tmp_path):
    engine = _fresh_engine(tmp_path)
    targets = {
        (target.table_name, target.index_name)
        for target in collect_index_targets(engine, vocabulary_included=True)
    }

    assert ("concept", "idx_concept_domain_id") in targets


def test_orm_index_metadata_carries_cluster_configuration():
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
    assert episode_indexes["idx_episode_person_id_1"].info[OMOP_CLUSTER_INDEX_INFO_KEY] is True


def test_manage_indexes_disable_and_enable_on_sqlite(tmp_path):
    engine = _fresh_engine(tmp_path)

    inspector = sa.inspect(engine)
    before = {
        index["name"]
        for index in inspector.get_indexes("person")
    }
    assert "idx_gender" in before

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
    assert "idx_gender" not in after_disable

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
    assert "idx_gender" in after_enable
