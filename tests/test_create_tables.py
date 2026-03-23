import sqlalchemy as sa

from omop_alchemy.maintenance.create_tables import collect_missing_tables, create_missing_tables


def _engine(tmp_path):
    return sa.create_engine(f"sqlite:///{tmp_path / 'create_tables.db'}", future=True)


def test_collect_missing_tables_on_empty_database(tmp_path):
    engine = _engine(tmp_path)
    missing = collect_missing_tables(engine)

    table_names = {table.table_name for table in missing}
    assert "person" in table_names
    assert "concept" in table_names


def test_create_missing_tables_reports_blocked_tables_when_vocabulary_is_missing(tmp_path):
    engine = _engine(tmp_path)
    results = create_missing_tables(engine, vocabulary_included=False)

    inspector = sa.inspect(engine)
    assert results
    assert not inspector.has_table("concept")
    result_by_name = {
        result.table_name: result
        for result in results
    }
    assert result_by_name["person"].status == "blocked"
    assert "concept" in result_by_name["person"].detail


def test_create_missing_tables_can_recreate_non_vocabulary_tables_when_dependencies_exist(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine, vocabulary_included=True)

    with engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE cdm_source")

    results = create_missing_tables(engine, vocabulary_included=False)

    inspector = sa.inspect(engine)
    assert any(result.table_name == "cdm_source" and result.status == "created" for result in results)
    assert inspector.has_table("cdm_source")
    assert inspector.has_table("concept")


def test_create_missing_tables_can_create_vocabulary(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine, vocabulary_included=True)

    inspector = sa.inspect(engine)
    assert inspector.has_table("person")
    assert inspector.has_table("concept")
