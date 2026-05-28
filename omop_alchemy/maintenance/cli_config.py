"""Configure a local test database resource for running PostgreSQL tests."""

from __future__ import annotations

from typing import Annotated

import sqlalchemy as sa
import typer
from rich.console import Console
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

console = Console()
err_console = Console(stderr=True)

_TEST_RESOURCE = "test_cdm_db"
_CONN_NAME = "pg_test"

_DEFAULTS = dict(
    dialect="postgresql+psycopg",
    host="localhost",
    port=55432,
    user="test",
    password="test",
    database="test_db",
    cdm_schema="public",
)

app = typer.Typer()


def _quote_id(name: str) -> str:
    """Double-quote a PostgreSQL identifier, escaping embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Single-quote a PostgreSQL string literal, escaping embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


def _get_admin_engine(*, yes: bool) -> sa.engine.Engine:
    """Resolve admin connection: suggest cdm_db resource first, then prompt."""
    from oa_configurator import Resolver, load_stack_config

    suggested_url: str | None = None
    try:
        stack = load_stack_config()
        resolved = Resolver(stack).resolve_resource("cdm_db")
        suggested_url = resolved.primary_db.url
    except Exception:
        pass

    if yes:
        if suggested_url is None:
            err_console.print(
                "[red]--provision --yes requires a configured 'cdm_db' resource"
                " as the admin connection.[/red]"
            )
            raise typer.Exit(1)
        return sa.create_engine(suggested_url)

    console.print("\n[bold]Admin connection[/bold] (needs CREATEDB + CREATEROLE):")
    if suggested_url:
        display = sa.engine.make_url(suggested_url).render_as_string(hide_password=True)
        console.print(f"  Suggested from [dim]cdm_db[/dim]: [cyan]{display}[/cyan]")
        if typer.confirm("Use this connection?", default=True):
            return sa.create_engine(suggested_url)
    else:
        console.print(
            "  [dim]No PostgreSQL connection found in your config.[/dim]\n"
            "  Enter a superuser URL, e.g.:"
            " [dim]postgresql+psycopg://postgres:<pw>@localhost/postgres[/dim]"
        )

    admin_url = typer.prompt("Admin connection URL")
    return sa.create_engine(admin_url.strip())


def _provision_test_db(
    *,
    admin_engine: sa.engine.Engine,
    user: str,
    password: str,
    database: str,
    yes: bool,
) -> None:
    """Create test user and database on the target Postgres instance if they don't exist."""
    admin_url = sa.engine.make_url(admin_engine.url)
    if not admin_url.drivername.startswith("postgresql"):
        err_console.print(
            f"[red]--provision requires a PostgreSQL admin connection."
            f" Got dialect: {admin_url.drivername}[/red]"
        )
        raise typer.Exit(1)

    console.print(
        f"\n[bold]Provision:[/bold] user=[cyan]{user}[/cyan]  database=[cyan]{database}[/cyan]"
    )
    if not yes:
        typer.confirm("Create these objects now?", default=True, abort=True)

    _superuser_note = (
        "[yellow]Note:[/yellow] SUPERUSER is required to disable FK constraint triggers"
        " during bulk loads. This grants full access to all databases on this instance."
    )
    with admin_engine.connect() as conn:
        role_row = conn.execute(
            text("SELECT rolsuper FROM pg_roles WHERE rolname = :n"), {"n": user}
        ).fetchone()
        if role_row is None:
            conn.execute(
                text(f"CREATE USER {_quote_id(user)} WITH PASSWORD {_quote_literal(password)} SUPERUSER")
            )
            conn.commit()
            console.print(f"[green]✓[/green] User [bold]{user!r}[/bold] created (SUPERUSER).")
            console.print(_superuser_note)
        elif not role_row[0]:
            conn.execute(text(f"ALTER USER {_quote_id(user)} SUPERUSER"))
            conn.commit()
            console.print(f"[green]✓[/green] User [bold]{user!r}[/bold] upgraded to SUPERUSER.")
            console.print(_superuser_note)
        else:
            console.print(f"[dim]User {user!r} already exists with SUPERUSER — skipped.[/dim]")

    with admin_engine.connect() as conn:
        db_exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": database}
        ).fetchone()

    if db_exists:
        console.print(f"[dim]Database {database!r} already exists — skipped.[/dim]")
    else:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"CREATE DATABASE {_quote_id(database)} OWNER {_quote_id(user)}"))
        console.print(
            f"[green]✓[/green] Database [bold]{database!r}[/bold] created (owner: {user!r})."
        )


@app.command(name="configure-test-db")
def configure_test_db(
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Accept all defaults without prompting."),
    ] = False,
    provision: Annotated[
        bool,
        typer.Option(
            "--provision",
            help=(
                "Also create the PostgreSQL user and database via an admin connection. "
                "Uses cdm_db as admin if configured, otherwise prompts for a superuser URL."
            ),
        ),
    ] = False,
) -> None:
    """Register a dedicated test database in ~/.config/omop/config.toml.

    Writes a 'test_cdm_db' resource for running local PostgreSQL tests.
    Defaults: localhost:55432, postgresql+psycopg, user/password/database = test.

    WARNING: the test suite runs DROP SCHEMA public CASCADE on every run.
    'test_cdm_db' must point to a dedicated, empty database — never to real data.

    Use --provision to also bootstrap the user and database on an existing Postgres instance.
    """
    from oa_configurator.io import save_stack_config
    from oa_configurator.loader import DEFAULT_CONFIG_PATH, load_stack_config
    from oa_configurator.models import ConnectionConfig, ResourceConfig, StackConfig

    try:
        config = load_stack_config()
    except FileNotFoundError:
        config = StackConfig()
        console.print(f"[dim]No config found — will create {DEFAULT_CONFIG_PATH}[/dim]")

    if _TEST_RESOURCE in config.resources:
        if not yes:
            overwrite = typer.confirm(
                f"Resource '{_TEST_RESOURCE}' already exists. Overwrite?",
                default=False,
            )
            if not overwrite:
                console.print("[yellow]Aborted.[/yellow]")
                raise typer.Exit(0)
        else:
            console.print(f"[yellow]Overwriting existing '{_TEST_RESOURCE}' resource.[/yellow]")

    if yes:
        dialect = _DEFAULTS["dialect"]
        host = _DEFAULTS["host"]
        port = _DEFAULTS["port"]
        user = _DEFAULTS["user"]
        password = _DEFAULTS["password"]
        database = _DEFAULTS["database"]
        cdm_schema = _DEFAULTS["cdm_schema"]
    else:
        console.print("\n[bold]Test PostgreSQL connection[/bold]")
        console.print("[dim]Press Enter to accept each default.[/dim]\n")
        dialect = typer.prompt("Dialect", default=_DEFAULTS["dialect"])
        host = typer.prompt("Host", default=_DEFAULTS["host"])
        port = int(typer.prompt("Port", default=str(_DEFAULTS["port"])))
        user = typer.prompt("User", default=_DEFAULTS["user"])
        password = typer.prompt("Password", default=_DEFAULTS["password"], hide_input=True)
        database = typer.prompt("Database", default=_DEFAULTS["database"])
        cdm_schema = typer.prompt("CDM schema", default=_DEFAULTS["cdm_schema"])

    # Safety guard: refuse if the connection details collide with any existing non-test resource
    for res_name, existing_res in config.resources.items():
        if res_name == _TEST_RESOURCE:
            continue
        existing_conn = config.connections.get(existing_res.primary_db)
        if (
            existing_conn is not None
            and existing_conn.host == host
            and existing_conn.port == port
            and existing_conn.database == database
        ):
            err_console.print(
                f"\n[red bold]DANGER[/red bold]: these connection details match the"
                f" [bold]{res_name!r}[/bold] resource.\n"
                f"Tests run DROP SCHEMA public CASCADE — this would destroy your database.\n"
                f"Configure a dedicated test database with a different host, port, or database name."
            )
            raise typer.Exit(1)

    if provision:
        admin_engine = _get_admin_engine(yes=yes)
        try:
            _provision_test_db(
                admin_engine=admin_engine,
                user=user,
                password=password,
                database=database,
                yes=yes,
            )
        except SQLAlchemyError as exc:
            err_console.print(f"[red]Provision failed:[/red] {exc.__class__.__name__}: {exc}")
            err_console.print("Check that the admin user has CREATEDB and CREATEROLE privileges.")
            raise typer.Exit(1)
        except Exception as exc:
            err_console.print(f"[red]Provision failed:[/red] {exc}")
            raise typer.Exit(1)
        finally:
            admin_engine.dispose()

    conn = ConnectionConfig(
        dialect=dialect,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    resource = ResourceConfig(primary_db=_CONN_NAME, cdm_schema=cdm_schema)

    config.connections[_CONN_NAME] = conn
    config.resources[_TEST_RESOURCE] = resource
    save_stack_config(config)

    console.print(
        f"\n[green]✓[/green] Resource [bold]{_TEST_RESOURCE!r}[/bold] written to"
        f" [dim]{DEFAULT_CONFIG_PATH}[/dim]"
    )
    console.print(f"[green]✓[/green] Connection: [dim]{conn.safe_url()}[/dim]")
    console.print("\nRun PostgreSQL tests:")
    console.print("  [bold]pytest -v tests/test_load_vocab_postgres.py[/bold]")
