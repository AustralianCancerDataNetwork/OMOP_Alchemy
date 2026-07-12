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
[databases.cdm]
dialect       = "postgresql+psycopg2"
host          = "localhost"
port          = 5432
user          = "omop"
password      = "changeme"
database_name = "omop_cdm"

[resources.cdm_db]
database   = "cdm"
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

## Docker Compose

The included `docker-compose.yaml` spins up a PostgreSQL database and a `python-alchemy`
container. Default credentials work out of the box — no additional setup needed:

```bash
docker compose up
```

The `python-alchemy` container runs `omop-config configure omop_alchemy` automatically at
startup. Your `~/.config/omop/config.toml` on the host is written on first start and
safe to re-run on subsequent starts: connection flags always apply, and any values already stored in `config.toml` are preserved for fields not explicitly provided.

### Overriding default values

The compose file uses built-in defaults for all database credentials. To use different
values, create a `.env` file in this directory with any of the following variables:

| Variable | Default | Description |
|---|---|---|
| `OMOP_CDM_DB_USER` | `omop` | CDM database username |
| `OMOP_CDM_DB_PASSWORD` | `omop` | CDM database password |
| `OMOP_CDM_DB_NAME` | `omop_cdm` | CDM database name |

Copy the example and edit as needed:

```bash
cp .env.example .env
# edit .env
docker compose up
```

The `.env` file is only read by Docker Compose for variable substitution — it is not
loaded by OMOP_Alchemy at runtime.

## Multiple instances

To configure a second CDM database (e.g. for production), use `--resource-name`:

```bash
omop-config configure omop_alchemy --resource-name cdm_db_prod
```

This creates `cdm_db_prod` without touching the existing `cdm_db`. Because two
resources now exist, configure automatically prompts you to choose the default at
the end of the same run — no second invocation needed.

To change the default later, set `default_resource` directly in `config.toml`:

```toml
[tools.omop_alchemy]
default_resource = "cdm_db_prod"
```

See the [oa-configurator integration guide](https://AustralianCancerDataNetwork.github.io/oa-configurator/integration/#multiple-environments) for the full multi-environment guide.

## Further reading

- [oa_configurator quickstart](https://AustralianCancerDataNetwork.github.io/oa-configurator/) — full config reference, multiple profiles, env var export
- [oa_configurator integration guide](https://AustralianCancerDataNetwork.github.io/oa-configurator/integration/) — Docker Compose details and multi-package setups
