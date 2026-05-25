from importlib import import_module, reload

from omop_alchemy.cdm.base import (
    ClinicalSchemaMixin,
    DerivedSchemaMixin,
    HealthEconomicSchemaMixin,
    HealthSystemSchemaMixin,
    MetadataSchemaMixin,
    StructuralSchemaMixin,
    UnstructuredSchemaMixin,
    VocabularySchemaMixin,
)
from omop_alchemy.cdm.model.clinical.person import Person
from omop_alchemy.cdm.model.derived.cohort import Cohort
from omop_alchemy.cdm.model.metadata.metadata import Metadata
from omop_alchemy.cdm.model.vocabulary.concept import Concept


def test_schema_mixins_default_layout() -> None:
    assert ClinicalSchemaMixin.__omop_schema__ == "omop"
    assert HealthSystemSchemaMixin.__omop_schema__ == "omop"
    assert HealthEconomicSchemaMixin.__omop_schema__ == "omop"
    assert StructuralSchemaMixin.__omop_schema__ == "omop"
    assert UnstructuredSchemaMixin.__omop_schema__ == "omop"
    assert MetadataSchemaMixin.__omop_schema__ == "omop"
    assert VocabularySchemaMixin.__omop_schema__ == "vocabulary"
    assert DerivedSchemaMixin.__omop_schema__ == "results"


def test_representative_tables_have_static_schemas() -> None:
    assert Person.__table__.schema == "omop"
    assert Concept.__table__.schema == "vocabulary"
    assert Cohort.__table__.schema == "results"
    assert Metadata.__table__.schema == "omop"


def test_schema_mixins_can_read_environment(monkeypatch) -> None:
    monkeypatch.setenv("OMOP_VOCABULARY_SCHEMA", "staging_vocab")
    module = reload(import_module("omop_alchemy.cdm.base.schema_mixins"))

    assert module.VocabularySchemaMixin.__omop_schema__ == "staging_vocab"
    assert module.ClinicalSchemaMixin.__omop_schema__ == "omop"