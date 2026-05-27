# Configuration

OMOP_Alchemy reads all database connection and schema settings from
[oa_configurator](https://github.com/AustralianCancerDataNetwork/oa-configurator) — no
`.env` files or `ENGINE` environment variables needed.

## Minimal config

Create `~/.config/omop/config.toml` with at least one connection and one resource:

```toml
[connections.cdm]
dialect   = "postgresql+psycopg2"
host      = "localhost"
port      = 5432
user      = "omop"
password  = "changeme"
database  = "omop_cdm"

[resources.default]
primary_db = "cdm"
cdm_schema = "omop"
```

Run `omop-config init` to create this file interactively, or write it manually.

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
