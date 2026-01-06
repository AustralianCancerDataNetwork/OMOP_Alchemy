## 0.2.0
- Initial public release
- SQLAlchemy 2.0 typed OMOP CDM models
- Domain validation helpers
- Episode + event scaffolding

## 0.2.1
- minor modification to support chunked csv dedupe queries for larger load files 

## 0.2.2
- changed load_environment function to accept a string parameter for path to a .env file holding parameters for ENGINE and SOURCE_PATH to accommodate easier downstream usage

## 0.2.3
- added bulk_load_context for trusted bulk loads (e.g. Athena vocabulary)
- Disables FK enforcement where supported
- Suppresses autoflush during bulk load operations