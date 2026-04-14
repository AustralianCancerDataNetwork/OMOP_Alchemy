from __future__ import annotations

import os


def _schema_from_env(env_var: str, default_schema: str | None) -> str | None:
    raw_value = os.getenv(env_var)
    if raw_value is None:
        return default_schema

    normalized = raw_value.strip()
    if not normalized:
        return default_schema
    if normalized.lower() in {"none", "null"}:
        return None
    return normalized


class ClinicalSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_CLINICAL_SCHEMA", "omop")


class DerivedSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_DERIVED_SCHEMA", "results")


class HealthEconomicSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_HEALTH_ECONOMIC_SCHEMA", "omop")


class HealthSystemSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_HEALTH_SYSTEM_SCHEMA", "omop")


class MetadataSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_METADATA_SCHEMA", "omop")


class StructuralSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_STRUCTURAL_SCHEMA", "omop")


class UnstructuredSchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_UNSTRUCTURED_SCHEMA", "omop")


class VocabularySchemaMixin:
    __omop_schema__ = _schema_from_env("OMOP_VOCABULARY_SCHEMA", "vocabulary")