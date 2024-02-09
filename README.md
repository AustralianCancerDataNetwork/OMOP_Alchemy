# OMOP Alchemy

Purpose: to provide an oncology-extension focused implementation of SQLAlchemy ORM definitions with opinionated enforcement of data conventions.

> For reference, see [background 2023 OHDSI APAC symposium paper](https://github.com/AustralianCancerDataNetwork/OMOP_Alchemy/blob/main/notebooks/ORMforResearchReadyData_APAC2023.pdf).

### Notes

If you don't want to install and configure a sagecipher keyring for credential management, you can instead create file pwd_override.txt under oa_config.app_root, containing just plaintext password, which will override the keyring functionality. 

Look in oa_system_config.yaml to specify your local or system database parameters for connection.