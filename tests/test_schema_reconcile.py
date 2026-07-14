import sqlalchemy as sa

from omop_alchemy.cdm.base.indexing import omop_index_name
from omop_alchemy.maintenance.cli_schema import create_missing_tables
from omop_alchemy.maintenance.cli_schema_reconcile import reconcile_schema

PERSON_GENDER_INDEX = omop_index_name("person", "gender_concept_id")


def _fresh_engine(tmp_path):
    db_path = tmp_path / "reconcile.db"
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    create_missing_tables(engine)
    return engine


def _person_gender_issues(report):
    return [
        issue
        for issue in report.issues
        if issue.table_name == "person"
        and issue.component == "index"
        and (issue.object_name == PERSON_GENDER_INDEX or issue.actual == "idx_gender")
    ]


def test_reconcile_schema_reports_no_drift_on_fresh_database(tmp_path):
    engine = _fresh_engine(tmp_path)
    report = reconcile_schema(engine)

    person_result = next(r for r in report.table_results if r.table_name == "person")
    assert person_result.status == "matched"
    assert person_result.issue_count == 0


def test_reconcile_schema_reports_renamed_for_foreign_named_equivalent_index(tmp_path):
    engine = _fresh_engine(tmp_path)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP INDEX {PERSON_GENDER_INDEX}")
        connection.exec_driver_sql("CREATE INDEX idx_gender ON person (gender_concept_id)")

    report = reconcile_schema(engine)
    issues = _person_gender_issues(report)

    assert len(issues) == 1
    issue = issues[0]
    assert issue.status == "renamed"
    assert issue.expected == PERSON_GENDER_INDEX
    assert issue.actual == "idx_gender"


def test_reconcile_schema_renamed_index_does_not_flip_table_to_drifted(tmp_path):
    engine = _fresh_engine(tmp_path)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP INDEX {PERSON_GENDER_INDEX}")
        connection.exec_driver_sql("CREATE INDEX idx_gender ON person (gender_concept_id)")

    report = reconcile_schema(engine)
    person_result = next(r for r in report.table_results if r.table_name == "person")

    assert person_result.status == "matched"
    assert person_result.issue_count == 1
