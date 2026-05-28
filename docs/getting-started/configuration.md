# Configuration

OMOP_Alchemy reads all database connection and schema settings from
[oa_configurator](https://github.com/AustralianCancerDataNetwork/oa-configurator) — no
`.env` files or `ENGINE` environment variables needed.

## Minimal config

Run the interactive configure command to set up the CDM database connection and write
`~/.config/omop/config.toml`:

```bash
omop-config configure omop_alchemy
```

This prompts for connection details (host, dialect, credentials) and schema names, then
saves them under the canonical resource name `cdm_db` that all OMOP stack packages
recognise.

The resulting TOML looks like:

```toml
[connections.cdm]
dialect   = "postgresql+psycopg2"
host      = "localhost"
port      = 5432
user      = "omop"
password  = "changeme"
database  = "omop_cdm"

[resources.cdm_db]
primary_db = "cdm"
cdm_schema = "omop"
```

You can also write or edit this file manually.

## Vocabulary loading

If you plan to load OMOP vocabulary from Athena CSV files, add the path to the package
extras section:

```toml
[tools.omop_alchemy.extra]
athena_source_path = "/path/to/athena/csvs"
```

Or set it interactively:

```bash
omop-config configure omop_alchemy
```

## Verify

```bash
omop-alchemy info
```

This prints the resolved config file path, connection details, and schema. A successful
run confirms that OMOP_Alchemy can reach your database.

## Further reading

- [oa_configurator quickstart](https://AustralianCancerDataNetwork.github.io/oa-configurator/) — full config reference, multiple profiles, env var export
