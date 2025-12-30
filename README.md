# OMOP Alchemy

Purpose: to provide a canonical, typed, SQLAlchemy-first representation of the OMOP CDM.

Prior versions offered opinionated enforcement of conventions, but this has been stripped back to define only tables, columns, relationships and ensure it is safe to import anywhere, with no side effects.

> For reference, see [background 2023 OHDSI APAC symposium paper](https://github.com/AustralianCancerDataNetwork/OMOP_Alchemy/blob/main/notebooks/ORMforResearchReadyData_APAC2023.pdf).
