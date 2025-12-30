# OMOP Alchemy

Purpose: to provide a canonical, typed, SQLAlchemy-first representation of the OMOP CDM.

Prior versions offered opinionated enforcement of conventions, but this has been stripped back to define only tables, columns, relationships and ensure it is safe to import anywhere, with no side effects.

> For reference, see [background 2023 OHDSI APAC symposium paper](https://github.com/AustralianCancerDataNetwork/OMOP_Alchemy/blob/main/notebooks/ORMforResearchReadyData_APAC2023.pdf).

### Notes

If you don't want to install and configure a sagecipher keyring for credential management, you can instead create file `pwd_override.txt` under oa_config.app_root, containing just plaintext password, which will override the keyring functionality. 

Create `oa_system_config.yaml` in this same directory to specify your local or system database parameters for connection. `oa_system_config_demo.yaml` has been provided as a template example.