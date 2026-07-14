import sqlalchemy as sa

from omop_alchemy.backends.sqlite import SQLiteBackend
from omop_alchemy.cdm.base.indexing import omop_index_name
from omop_alchemy.maintenance.cli_schema import create_missing_tables
from omop_alchemy.maintenance.cli_schema_reconcile import is_blocking_issue, reconcile_schema

PERSON_GENDER_INDEX = omop_index_name("person", "gender_concept_id")
EPISODE_PERSON_INDEX = omop_index_name("episode", "person_id")


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


def test_is_blocking_issue_excludes_renamed_only():
    from omop_alchemy.maintenance.cli_schema_reconcile import ReconciliationIssue
    from omop_alchemy.maintenance._cli_utils import Status
    from omop_alchemy.maintenance.tables import TableCategory

    renamed = ReconciliationIssue(
        table_name="person", category=TableCategory.CLINICAL, component="index",
        object_name=PERSON_GENDER_INDEX, status=Status.RENAMED,
        expected=PERSON_GENDER_INDEX, actual="idx_gender", detail="...",
    )
    missing = ReconciliationIssue(
        table_name="person", category=TableCategory.CLINICAL, component="index",
        object_name=PERSON_GENDER_INDEX, status=Status.MISSING,
        expected=PERSON_GENDER_INDEX, actual=None, detail="...",
    )
    assert is_blocking_issue(renamed) is False
    assert is_blocking_issue(missing) is True


def test_reconcile_schema_cluster_check_reports_renamed_for_foreign_cluster_index(tmp_path, monkeypatch):
    """A table physically clustered on a foreign-named equivalent of the ORM's
    cluster index (e.g. captured/restored under its original name by
    manage_indexes()) must report a 'renamed' cluster issue, not 'mismatch'."""
    engine = _fresh_engine(tmp_path)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP INDEX {EPISODE_PERSON_INDEX}")
        connection.exec_driver_sql("CREATE INDEX idx_episode_person ON episode (person_id)")

    monkeypatch.setattr(
        SQLiteBackend,
        "get_clustered_index_name",
        lambda self, conn, table_name, db_schema: (
            "idx_episode_person" if table_name == "episode" else None
        ),
    )

    report = reconcile_schema(engine)
    episode_result = next(r for r in report.table_results if r.table_name == "episode")
    cluster_issues = [
        issue for issue in report.issues
        if issue.table_name == "episode" and issue.component == "cluster"
    ]

    assert len(cluster_issues) == 1
    assert cluster_issues[0].status == "renamed"
    assert cluster_issues[0].expected == EPISODE_PERSON_INDEX
    assert cluster_issues[0].actual == "idx_episode_person"
    assert episode_result.status == "matched"


def test_reconcile_schema_cluster_check_still_reports_real_mismatch(tmp_path, monkeypatch):
    """A genuinely different physical cluster state (not just a foreign-named
    equivalent) must still be reported as drift."""
    engine = _fresh_engine(tmp_path)

    monkeypatch.setattr(
        SQLiteBackend,
        "get_clustered_index_name",
        lambda self, conn, table_name, db_schema: (
            "some_unrelated_index" if table_name == "episode" else None
        ),
    )

    report = reconcile_schema(engine)
    episode_result = next(r for r in report.table_results if r.table_name == "episode")
    cluster_issues = [
        issue for issue in report.issues
        if issue.table_name == "episode" and issue.component == "cluster"
    ]

    assert len(cluster_issues) == 1
    assert cluster_issues[0].status == "mismatch"
    assert episode_result.status == "drifted"
