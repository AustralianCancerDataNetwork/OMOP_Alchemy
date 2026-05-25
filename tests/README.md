# Running the test suite

## Quick start

```bash
# Unit and SQLite tests — no database required
uv run --extra dev pytest -m "not postgres"

# PostgreSQL integration tests — requires the Docker container below
docker compose -f tests/docker-compose.yaml up -d
uv run --extra dev --extra postgres pytest -m postgres -v
```

## PostgreSQL integration tests

The `postgres`-marked tests connect to a local PostgreSQL 16 container on
port **55432**.

```bash
# Start
docker compose -f tests/example-docker-compose.yaml up -d

# Run (this will run all tests)
uv run --extra dev --extra postgres pytest -m "postgres or not postgres" -v

# Stop
docker compose -f tests/docker-compose.yaml down
```

## Test markers

| Marker | Meaning |
|--------|---------|
| *(none)* | Runs on SQLite, no external dependencies |
| `postgres` | Requires the Docker container on port 55432 |

## Fixture data

`tests/fixtures/athena_source/` contains a minimal set of Athena vocabulary
CSVs (7 concepts) used to seed the SQLite test database. These are committed
to the repo and are sufficient for all non-postgres tests.
