import sqlalchemy as sa
import pytest

from omop_alchemy.maintenance.reset_sequences import collect_sequence_targets, reset_model_sequences


def test_collect_sequence_targets_excludes_vocabulary_by_default():
    targets = {
        (target.table_name, target.pk_column_name)
        for target in collect_sequence_targets()
    }

    assert ("person", "person_id") in targets
    assert ("concept", "concept_id") not in targets


def test_collect_sequence_targets_can_include_vocabulary():
    targets = {
        (target.table_name, target.pk_column_name)
        for target in collect_sequence_targets(vocabulary_included=True)
    }

    assert ("concept", "concept_id") in targets


def test_reset_model_sequences_requires_postgresql(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'seq.db'}", future=True)

    with pytest.raises(RuntimeError) as exc_info:
        reset_model_sequences(engine)

    assert "only supported for PostgreSQL engines" in str(exc_info.value)
