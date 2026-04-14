# Configuration

OMOP Alchemy resolves table schemas at import time from a small set of environment
variables. This keeps the ORM static and easy to read while still allowing a
deployment to choose its schema layout once.

## Schema variables

![OMOP CDM v5.4 category map](https://ohdsi.github.io/CommonDataModel/images/cdm54.png)

The schema categories in OMOP Alchemy are based on the OMOP CDM v5.4 groupings
shown above. In practice, that means each ORM table is assigned to one of these
category buckets, and each bucket resolves to one schema variable.

OMOP Alchemy uses these category buckets as the source of truth:

- `clinical`
- `health_system`
- `health_economic`
- `structural`
- `unstructured`
- `metadata`
- `vocabulary`
- `derived`

Each table belongs to one of those categories, and therefore resolves its schema
from the corresponding `OMOP_*_SCHEMA` variable.

The table categories are grouped by default as follows:

- `OMOP_CLINICAL_SCHEMA=omop`
- `OMOP_HEALTH_SYSTEM_SCHEMA=omop`
- `OMOP_HEALTH_ECONOMIC_SCHEMA=omop`
- `OMOP_STRUCTURAL_SCHEMA=omop`
- `OMOP_UNSTRUCTURED_SCHEMA=omop`
- `OMOP_METADATA_SCHEMA=omop`
- `OMOP_VOCABULARY_SCHEMA=vocabulary`
- `OMOP_DERIVED_SCHEMA=results`

Variable-to-tables mapping:

- `OMOP_CLINICAL_SCHEMA`
	- `person`
	- `condition_occurrence`
	- `death`
	- `device_exposure`
	- `drug_exposure`
	- `measurement`
	- `observation`
	- `procedure_occurrence`
	- `specimen`
- `OMOP_HEALTH_SYSTEM_SCHEMA`
	- `care_site`
	- `location`
	- `provider`
	- `visit_occurrence`
	- `visit_detail`
- `OMOP_HEALTH_ECONOMIC_SCHEMA`
	- `cost`
	- `payer_plan_period`
- `OMOP_STRUCTURAL_SCHEMA`
	- `episode`
	- `episode_event`
	- `fact_relationship`
- `OMOP_UNSTRUCTURED_SCHEMA`
	- `note`
	- `note_nlp`
- `OMOP_METADATA_SCHEMA`
	- `cdm_source`
	- `metadata`
- `OMOP_VOCABULARY_SCHEMA`
	- `concept`
	- `concept_ancestor`
	- `concept_class`
	- `concept_relationship`
	- `concept_synonym`
	- `domain`
	- `drug_strength`
	- `relationship`
	- `source_to_concept_map`
	- `vocabulary`
- `OMOP_DERIVED_SCHEMA`
	- `cohort`
	- `cohort_definition`
	- `condition_era`
	- `dose_era`
	- `drug_era`
	- `observation_period`

Set any of these to a different schema name to override the default. Set the value
to `none` or `null` to leave that category schema-less.

Operational implications when changing a category schema:

- Queries against that category will target the new schema because ORM table
	objects carry the resolved schema.
- Maintenance commands will inspect/create/manage those tables in the same schema
	unless `--db-schema` is explicitly provided as an override.
- Existing tables are not migrated automatically. If you change a schema variable,
	move data/DDL separately or create the target tables before running workloads.

## Import order

Load environment variables before importing the CDM model package. The ORM classes
read their schema mixins during class construction.

For example:

```python
from omop_alchemy import load_environment

load_environment(".env")

from omop_alchemy.cdm.model.vocabulary import Concept

print(Concept.__table__.schema)
```

## Engine variables

The database engine is still resolved from the existing engine variables:

- `ENGINE_<SCHEMA>` when `engine_schema` is provided
- `ENGINE` as the fallback