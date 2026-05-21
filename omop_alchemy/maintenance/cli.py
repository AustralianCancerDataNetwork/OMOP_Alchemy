from __future__ import annotations

import typer

from . import (
    cli_backup as backup,
    cli_config as config,
    cli_foreign_keys as foreign_keys,
    cli_fulltext as fulltext,
    cli_indexes as indexes,
    cli_schema as schema,
    cli_tables as tables,
    cli_vocab as vocab,
)
from ._cli_utils import configure_logging
from .help import install_help_customizations

install_help_customizations()

app = typer.Typer(
    help=(
        "OMOP Alchemy maintenance utilities.\n\n"
        "PostgreSQL-only commands: reset-sequences, truncate-tables, "
        "foreign-keys, backup-database, restore-database, fulltext."
    ),
    rich_markup_mode="rich",
)

# Subgroups
app.add_typer(config.app, name="config")
app.add_typer(foreign_keys.app, name="foreign-keys")
app.add_typer(indexes.app, name="indexes")
app.add_typer(fulltext.app, name="fulltext")

# Flat root-level commands lifted from each domain module
for _sub in (schema.app, vocab.app, tables.app, backup.app):
    for _cmd in _sub.registered_commands:
        app.registered_commands.append(_cmd)


@app.callback()
def app_callback() -> None:
    configure_logging()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
