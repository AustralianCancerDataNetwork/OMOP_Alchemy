import sqlalchemy as sa

from omop_alchemy.maintenance.create_tables import create_missing_tables
from omop_alchemy.maintenance.data_summary import collect_data_summary


def _engine(tmp_path):
    return sa.create_engine(f"sqlite:///{tmp_path / 'data_summary.db'}", future=True)


def test_collect_data_summary_can_include_missing_tables(tmp_path):
    engine = _engine(tmp_path)
    results = collect_data_summary(engine, existing_only=False)
    assert results
    assert any(result.exists is False for result in results)


def test_collect_data_summary_reports_row_counts(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    with engine.begin() as connection:
        connection.execute(
            sa.text("INSERT INTO location (location_id) VALUES (1)")
        )

    results = {
        result.table_name: result
        for result in collect_data_summary(engine, vocabulary_included=True)
    }

    assert results["location"].exists is True
    assert results["location"].row_count == 1


def test_collect_data_summary_excludes_vocabulary_by_default(tmp_path):
    engine = _engine(tmp_path)
    create_missing_tables(engine)

    table_names = {
        result.table_name
        for result in collect_data_summary(engine)
    }
    assert "person" in table_names
    assert "concept" not in table_names
