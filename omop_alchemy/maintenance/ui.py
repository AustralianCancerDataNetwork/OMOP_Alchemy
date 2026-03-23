from __future__ import annotations

from collections.abc import Iterable

from rich import box
from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from omop_alchemy.cdm.handlers.fulltext import FullTextResult

from .analyze_tables import AnalyzeTableResult
from .ascii import render_banner
from .backup import DatabaseBackupResult, DatabaseRestoreResult
from .backend_support import backend_label
from .create_tables import TableCreationResult
from .data_summary import TableSummaryResult
from .defaults import ConnectionDefaults
from .doctor import DoctorCheck, DoctorRecommendation, DoctorReport
from .foreign_keys import (
    ForeignKeyAction,
    ForeignKeyManagementResult,
    ForeignKeyStatusResult,
    ForeignKeyConstraintViolation,
    ForeignKeyValidationReport,
    ForeignKeyValidationResult,
)
from .info import CommandSupport, MaintenanceInfo
from .indexes import IndexAction, IndexManagementResult
from .load_vocab import VocabularyLoadReport, VocabularyLoadResult
from .reconcile import ReconciliationIssue, SchemaReconciliationReport, TableReconciliationResult
from .reset_sequences import SequenceResetResult
from .tables import TableCategory
from .truncate_tables import TruncateTableResult

console = Console()

STATUS_STYLES = {
    "applied": "green",
    "blocked": "red",
    "drifted": "yellow",
    "limited": "yellow",
    "matched": "green",
    "missing": "red",
    "planned": "cyan",
    "ready": "green",
    "reset": "green",
    "created": "green",
    "loaded": "green",
    "warning": "yellow",
    "info": "cyan",
    "skipped": "yellow",
    "unsupported": "red",
    "failed": "red",
    "passed": "green",
}

CATEGORY_STYLES = {
    TableCategory.CLINICAL: "bright_blue",
    TableCategory.DERIVED: "blue",
    TableCategory.HEALTH_ECONOMIC: "green",
    TableCategory.HEALTH_SYSTEM: "bright_cyan",
    TableCategory.METADATA: "white",
    TableCategory.STRUCTURAL: "magenta",
    TableCategory.UNSTRUCTURED: "bright_magenta",
    TableCategory.VOCABULARY: "yellow",
}


def _bool_label(value: bool) -> Text:
    return Text("yes" if value else "no", style="green" if value else "dim")


def _category_label(category: TableCategory) -> Text:
    return Text(
        category.value.replace("_", " "),
        style=CATEGORY_STYLES[category],
    )


def render_command_header(
    *,
    command_name: str,
    engine_schema: str | None,
    db_schema: str | None,
    vocabulary_included: bool | None,
    mode_label: str,
) -> RenderableType:
    banner = Text(
        render_banner(console.size.width),
        style="bold yellow",
    )
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Command", command_name)
    grid.add_row("Engine", engine_schema or "default ENGINE")
    grid.add_row("DB schema", db_schema or "default search_path")
    if vocabulary_included is not None:
        grid.add_row("Vocabulary", _bool_label(vocabulary_included))
    grid.add_row(
        "Mode",
        Text(
            mode_label,
            style="cyan" if mode_label in {"dry-run", "inspect"} else "green",
        ),
    )

    return Group(
        banner,
        Panel.fit(
            grid,
            title="[bold]OMOP Maintenance[/bold]",
            border_style="blue",
        ),
    )


def render_error(message: str, *, title: str = "Error") -> Panel:
    return Panel.fit(
        Text(message, style="bold red"),
        title=f"[bold red]{title}[/bold red]",
        border_style="red",
    )


def render_connection_defaults(
    defaults: ConnectionDefaults,
    *,
    path: str,
    title: str = "Connection Defaults",
) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("File", path)
    grid.add_row("dotenv", defaults.dotenv or "-")
    grid.add_row("engine_schema", defaults.engine_schema or "-")
    grid.add_row("db_schema", defaults.db_schema or "-")
    grid.add_row("athena_source", defaults.athena_source or "-")
    return Panel.fit(grid, title=f"[bold]{title}[/bold]", border_style="blue")


def _status_text(status: str) -> Text:
    return Text(status.upper(), style=STATUS_STYLES.get(status, "white"))


def _optional_bool_label(value: bool | None) -> Text:
    if value is None:
        return Text("-", style="dim")
    return _bool_label(value)


def _simple_status_table(
    rows: list[tuple[str, ...]],
    headers: list[str],
) -> Table:
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    for header in headers:
        table.add_column(header)
    for row in rows:
        table.add_row(*row)
    return table


def render_info_environment(info: MaintenanceInfo) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Package", f"omop-alchemy {info.package_version}")
    grid.add_row("CLI", info.cli_path or "not on PATH")
    grid.add_row("pg_dump", info.pg_dump_path or "not on PATH")
    grid.add_row("pg_restore", info.pg_restore_path or "not on PATH")
    grid.add_row("psql", info.psql_path or "not on PATH")
    grid.add_row(
        "Defaults file",
        Text(
            info.defaults_file,
            style="green" if info.defaults_exists else "yellow",
        ),
    )
    grid.add_row("dotenv", info.dotenv_path or "-")
    grid.add_row("dotenv exists", _optional_bool_label(info.dotenv_exists))
    grid.add_row("engine_schema", info.engine_schema or "-")
    grid.add_row("db_schema", info.db_schema or "default search_path")
    return Panel.fit(grid, title="[bold]Environment[/bold]", border_style="magenta")


def render_info_database(info: MaintenanceInfo) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Engine URL", info.engine_url or "-")
    grid.add_row("Backend", backend_label(info.backend) if info.backend else "-")
    grid.add_row("Engine created", _bool_label(info.engine_created))
    grid.add_row("Connection ready", _bool_label(info.connection_ready))

    if info.engine_error:
        grid.add_row("Engine detail", Text(info.engine_error, style="yellow"))
    if info.connection_error:
        grid.add_row("Connection detail", Text(info.connection_error, style="yellow"))

    grid.add_row("Managed tables", str(info.managed_table_count))
    if info.existing_table_count is not None:
        grid.add_row("Existing tables", str(info.existing_table_count))
    if info.missing_table_count is not None:
        grid.add_row("Missing tables", str(info.missing_table_count))
    grid.add_row("Vocabulary scope", _bool_label(info.vocabulary_included))

    border_style = "green" if info.connection_ready else "yellow"
    return Panel.fit(grid, title="[bold]Database[/bold]", border_style=border_style)


def render_info_dependencies(info: MaintenanceInfo) -> RenderableType:
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Dependencies[/bold]"
    table.add_column("Dependency", style="bold")
    table.add_column("Installed")
    table.add_column("Version")

    for dependency in info.dependencies:
        table.add_row(
            dependency.name,
            _bool_label(dependency.installed),
            dependency.version or "-",
        )

    return table


def render_backup_result(result: DatabaseBackupResult) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Status", _status_text(result.status))
    grid.add_row("Backend", backend_label(result.backend))
    grid.add_row("Database", result.database_name)
    grid.add_row("Schema", result.schema_name or "all schemas")
    grid.add_row("Format", result.format.value)
    grid.add_row("Output", result.output_path)
    grid.add_row("Tool", result.tool_path)
    grid.add_row("Detail", result.detail)
    return Panel.fit(grid, title="[bold]Backup[/bold]", border_style="green" if result.status == "created" else "cyan")


def render_backup_summary(result: DatabaseBackupResult, *, dry_run: bool) -> Panel:
    restore_hint = (
        f"Restore with `pg_restore -d <target_database> {result.output_path}`."
        if result.format.value == "custom"
        else f"Restore with `psql -d <target_database> -f {result.output_path}`."
    )
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Artifact", result.output_path)
    grid.add_row("Format", result.format.value)
    grid.add_row("Restore", restore_hint)
    grid.add_row(
        "Summary",
        "Backup planned; no dump file was created." if dry_run else "Backup completed; artifact is ready for restore.",
    )
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="cyan" if dry_run else "green")


def render_restore_result(result: DatabaseRestoreResult) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Status", _status_text(result.status))
    grid.add_row("Backend", backend_label(result.backend))
    grid.add_row("Database", result.database_name)
    grid.add_row("Schema", result.schema_name or "all schemas")
    grid.add_row("Format", result.format.value)
    grid.add_row("Input", result.input_path)
    grid.add_row("Tool", result.tool_path)
    grid.add_row("Detail", result.detail)
    return Panel.fit(grid, title="[bold]Restore[/bold]", border_style="green" if result.status == "applied" else "cyan")


def render_restore_summary(result: DatabaseRestoreResult, *, dry_run: bool) -> Panel:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Artifact", result.input_path)
    grid.add_row("Format", result.format.value)
    grid.add_row(
        "Summary",
        "Restore planned; no changes were applied to the target database."
        if dry_run
        else "Restore completed; the target database has been updated from the backup artifact.",
    )
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="cyan" if dry_run else "green")


def render_reconciliation_results(results: Iterable[TableReconciliationResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No ORM-managed tables matched the current selection.", title="No Data", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Issues", justify="right")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.table_name,
            _category_label(result.category),
            str(result.issue_count),
            result.detail,
        )

    return table


def render_reconciliation_issues(issues: Iterable[ReconciliationIssue]) -> RenderableType:
    items = list(issues)
    if not items:
        return Panel.fit("No schema drift detected between ORM metadata and the target database.", title="No Drift", border_style="green")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Differences[/bold]"
    table.add_column("Table", style="bold")
    table.add_column("Component")
    table.add_column("Object")
    table.add_column("Status")
    table.add_column("Expected")
    table.add_column("Actual")
    table.add_column("Detail")

    for issue in items:
        table.add_row(
            issue.table_name,
            issue.component,
            issue.object_name,
            _status_text(issue.status),
            issue.expected or "-",
            issue.actual or "-",
            issue.detail,
        )

    return table


def render_reconciliation_summary(report: SchemaReconciliationReport) -> Panel:
    matched = sum(result.status == "matched" for result in report.table_results)
    drifted = sum(result.status == "drifted" for result in report.table_results)
    missing = sum(result.status == "missing" for result in report.table_results)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Backend", backend_label(report.backend))
    grid.add_row("Tables", str(len(report.table_results)))
    if matched:
        grid.add_row("Matched", str(matched))
    if drifted:
        grid.add_row("Drifted", str(drifted))
    if missing:
        grid.add_row("Missing", str(missing))
    grid.add_row("Issues", str(len(report.issues)))
    grid.add_row(
        "Summary",
        "Schema drift detected." if report.issues else "Database schema matches ORM metadata for the selected scope.",
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="yellow" if report.issues else "green",
    )


def render_info_command_support(support: Iterable[CommandSupport]) -> RenderableType:
    items = list(support)
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Command Compatibility[/bold]"
    table.add_column("Command", style="bold")
    table.add_column("Status")
    table.add_column("Requirement")
    table.add_column("Detail")

    for item in items:
        table.add_row(
            item.command_name,
            _status_text(item.status),
            item.requirement,
            item.detail,
        )

    return table


def render_info_summary(info: MaintenanceInfo) -> Panel:
    ready = sum(item.status == "ready" for item in info.command_support)
    limited = sum(item.status == "limited" for item in info.command_support)
    blocked = sum(item.status == "blocked" for item in info.command_support)
    unsupported = sum(item.status == "unsupported" for item in info.command_support)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Ready", str(ready))
    if limited:
        grid.add_row("Limited", str(limited))
    if blocked:
        grid.add_row("Blocked", str(blocked))
    if unsupported:
        grid.add_row("Unsupported", str(unsupported))
    grid.add_row(
        "Summary",
        "Connection and backend checks completed for maintenance commands.",
    )
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="blue")


def render_doctor_checks(checks: Iterable[DoctorCheck]) -> RenderableType:
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Doctor Checks[/bold]"
    table.add_column("Status")
    table.add_column("Check", style="bold")
    table.add_column("Detail")

    for check in checks:
        table.add_row(
            _status_text(check.status),
            check.name,
            check.detail,
        )

    return table


def render_doctor_recommendations(
    recommendations: Iterable[DoctorRecommendation],
) -> RenderableType:
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Recommended Next Steps[/bold]"
    table.add_column("Status")
    table.add_column("Summary", style="bold")
    table.add_column("Action")

    for recommendation in recommendations:
        table.add_row(
            _status_text(recommendation.status),
            recommendation.summary,
            recommendation.action or "-",
        )

    return table


def render_doctor_summary(report: DoctorReport, *, deep: bool) -> Panel:
    passed = sum(check.status == "passed" for check in report.checks)
    warning = sum(check.status == "warning" for check in report.checks)
    failed = sum(check.status == "failed" for check in report.checks)
    skipped = sum(check.status == "skipped" for check in report.checks)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Checks", str(len(report.checks)))
    grid.add_row("Passed", str(passed))
    if warning:
        grid.add_row("Warnings", str(warning))
    if failed:
        grid.add_row("Failures", str(failed))
    if skipped:
        grid.add_row("Skipped", str(skipped))
    grid.add_row("Depth", "deep" if deep else "standard")
    grid.add_row(
        "Summary",
        (
            "Doctor found one or more maintenance blockers."
            if failed
            else "Doctor completed with warnings."
            if warning
            else "Doctor completed without obvious maintenance blockers."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="red" if failed else "yellow" if warning else "green",
    )


def render_sequence_reset_results(results: Iterable[SequenceResetResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No sequence-backed tables matched the current selection.", title="No Action", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Sequence")
    table.add_column("Next", justify="right")
    table.add_column("Detail")

    for result in items:
        style = STATUS_STYLES.get(result.status, "white")
        table.add_row(
            Text(result.status.upper(), style=style),
            f"{result.table_name}.{result.pk_column_name}",
            _category_label(result.category),
            result.sequence_name or "-",
            str(result.next_value) if result.next_value is not None else "-",
            result.detail,
        )

    return table


def render_sequence_reset_summary(results: Iterable[SequenceResetResult], *, dry_run: bool) -> Panel:
    items = list(results)
    reset_count = sum(result.status in {"planned", "reset"} for result in items)
    skipped_count = sum(result.status == "skipped" for result in items)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Sequences", str(reset_count))
    grid.add_row("Skipped", str(skipped_count))
    grid.add_row(
        "Summary",
        f"{'Planned' if dry_run else 'Applied'} {reset_count} sequence(s); skipped {skipped_count} table(s).",
    )
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="green" if not dry_run else "cyan")


def render_data_summary_results(results: Iterable[TableSummaryResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No OMOP Alchemy tables were found for the current selection.", title="No Data", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Rows", justify="right")
    table.add_column("PK")
    table.add_column("Model")

    for result in items:
        table.add_row(
            result.table_name,
            _category_label(result.category),
            "-" if result.row_count is None else f"{result.row_count:,}",
            ", ".join(result.primary_key_columns),
            result.model_name,
            style="dim" if not result.exists else "",
        )

    return table


def render_data_summary_summary(results: Iterable[TableSummaryResult]) -> Panel:
    items = list(results)
    existing = [result for result in items if result.exists]
    total_rows = sum(result.row_count or 0 for result in existing)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(existing)))
    grid.add_row("Rows", f"{total_rows:,}")
    if len(existing) != len(items):
        grid.add_row("Missing", str(len(items) - len(existing)))
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="blue")


def render_vocab_load_results(
    results: Iterable[VocabularyLoadResult],
) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No Athena vocabulary CSV files matched the current selection.", title="No Data", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Required")
    table.add_column("Rows", justify="right")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.table_name,
            _bool_label(result.required),
            str(result.row_count) if result.row_count is not None else "-",
            result.detail,
        )

    return table


def render_vocab_load_summary(report: VocabularyLoadReport, *, dry_run: bool) -> Panel:
    loaded_count = sum(result.status in {"planned", "loaded"} for result in report.results)
    skipped_count = sum(result.status == "skipped" for result in report.results)
    total_rows = sum(result.row_count or 0 for result in report.results)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Source", report.source_path)
    grid.add_row("Tables", str(loaded_count))
    if total_rows:
        grid.add_row("Rows", f"{total_rows:,}")
    if report.created_table_count:
        grid.add_row("Created tables", str(report.created_table_count))
    if report.sequence_reset_count:
        grid.add_row("Reset sequences", str(report.sequence_reset_count))
    if skipped_count:
        grid.add_row("Skipped", str(skipped_count))
    grid.add_row(
        "Summary",
        (
            "Athena vocabulary load planned; no CSV files were applied."
            if dry_run
            else "Athena vocabulary source loaded into ORM-managed vocabulary tables."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="cyan" if dry_run else "green",
    )


def render_foreign_key_results(results: Iterable[ForeignKeyManagementResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No FK-participating OMOP tables matched the current selection.", title="No Action", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Action")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Outgoing", justify="right")
    table.add_column("Incoming", justify="right")

    for result in items:
        style = STATUS_STYLES.get(result.status, "white")
        table.add_row(
            Text(result.status.upper(), style=style),
            result.action.value,
            result.table_name,
            _category_label(result.category),
            str(result.outgoing_constraint_count),
            str(result.incoming_constraint_count),
        )

    return table


def render_foreign_key_summary(results: Iterable[ForeignKeyManagementResult], *, dry_run: bool) -> Panel:
    items = list(results)
    action = items[0].action.value if items else "manage"
    failed = sum(item.status == "failed" for item in items)
    skipped = sum(item.status == "skipped" for item in items)
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(items)))
    grid.add_row("Outgoing", str(sum(item.outgoing_constraint_count for item in items)))
    grid.add_row("Incoming", str(sum(item.incoming_constraint_count for item in items)))
    if failed:
        grid.add_row("Failed", str(failed))
    if skipped:
        grid.add_row("Skipped", str(skipped))
    grid.add_row(
        "Summary",
        (
            "Strict validation failed; no FK triggers were enabled."
            if failed
            else f"{'Planned' if dry_run else 'Applied'} {action} FK trigger enforcement on {len(items)} table(s)."
        ),
    )
    border_style = "red" if failed else "green" if not dry_run else "cyan"
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style=border_style)


def render_foreign_key_note(action: ForeignKeyAction, *, strict: bool = False) -> Panel:
    if action is ForeignKeyAction.DISABLE:
        body = (
            "PostgreSQL keeps the foreign key constraints defined in metadata. "
            "This command disables the internal RI triggers that enforce them."
        )
    elif strict:
        body = (
            "This command validates all selected foreign key relationships first. "
            "If any violations are found, no RI triggers are re-enabled."
        )
    else:
        body = "This command re-enables PostgreSQL internal RI triggers so foreign key constraints are enforced again."
    return Panel.fit(body, title="[bold]Note[/bold]", border_style="yellow")


def render_foreign_key_status_results(results: Iterable[ForeignKeyStatusResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No FK-participating OMOP tables matched the current selection.", title="No Data", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Disabled", justify="right")
    table.add_column("Enabled", justify="right")
    table.add_column("Outgoing", justify="right")
    table.add_column("Incoming", justify="right")

    for result in items:
        table.add_row(
            result.table_name,
            _category_label(result.category),
            str(result.disabled_trigger_count),
            str(result.enabled_trigger_count),
            str(result.outgoing_constraint_count),
            str(result.incoming_constraint_count),
        )

    return table


def render_foreign_key_status_summary(results: Iterable[ForeignKeyStatusResult]) -> Panel:
    items = list(results)
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(items)))
    grid.add_row("Disabled tables", str(sum(item.disabled_trigger_count > 0 for item in items)))
    grid.add_row("Enabled tables", str(sum(item.enabled_trigger_count > 0 for item in items)))
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="blue")


def render_foreign_key_validation_results(
    results: Iterable[ForeignKeyValidationResult],
) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No FK-participating OMOP tables matched the current selection.", title="No Data", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Bad FKs", justify="right")
    table.add_column("Bad Rows", justify="right")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.table_name,
            _category_label(result.category),
            str(result.violating_constraint_count),
            str(result.violating_row_count),
            result.detail,
        )

    return table


def render_foreign_key_validation_issues(
    violations: Iterable[ForeignKeyConstraintViolation],
) -> RenderableType:
    items = list(violations)
    if not items:
        return Panel.fit(
            "All selected foreign key relationships passed validation.",
            title="No Violations",
            border_style="green",
        )

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.title = "[bold]Violations[/bold]"
    table.add_column("Source", style="bold")
    table.add_column("Constraint")
    table.add_column("Referred table")
    table.add_column("Rows", justify="right")

    for violation in items:
        table.add_row(
            violation.source_table_name,
            violation.constraint_name,
            violation.referred_table_name,
            str(violation.violation_count),
        )

    return table


def render_foreign_key_validation_summary(
    report: ForeignKeyValidationReport,
) -> Panel:
    failed_tables = [
        result
        for result in report.results
        if result.status == "failed"
    ]
    total_constraints = sum(
        result.violating_constraint_count
        for result in report.results
    )
    total_rows = sum(
        result.violating_row_count
        for result in report.results
    )

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(report.results)))
    grid.add_row("Violating tables", str(len(failed_tables)))
    grid.add_row("Violating constraints", str(total_constraints))
    grid.add_row("Violating rows", f"{total_rows:,}")
    grid.add_row(
        "Summary",
        (
            "All selected foreign key relationships passed validation."
            if not failed_tables
            else "Fix the violating rows, then rerun `omop-maint foreign-keys enable --strict`."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="green" if not failed_tables else "red",
    )


def render_table_creation_results(results: Iterable[TableCreationResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No missing OMOP tables were found for the current selection.", title="No Action", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Model")
    table.add_column("Detail")
    for result in items:
        style = STATUS_STYLES.get(result.status, "white")
        table.add_row(
            Text(result.status.upper(), style=style),
            result.table_name,
            _category_label(result.category),
            result.model_name,
            result.detail,
        )
    return table


def render_table_creation_summary(results: Iterable[TableCreationResult], *, dry_run: bool) -> Panel:
    items = list(results)
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(items)))
    grid.add_row("Summary", f"{'Planned' if dry_run else 'Created'} {len(items)} table(s).")
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="green" if not dry_run else "cyan")


def render_index_results(results: Iterable[IndexManagementResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit("No metadata-defined indexes matched the current selection.", title="No Action", border_style="yellow")

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Kind")
    table.add_column("Action")
    table.add_column("Table", style="bold")
    table.add_column("Index")
    table.add_column("Category")
    table.add_column("Columns")
    for result in items:
        style = STATUS_STYLES.get(result.status, "white")
        table.add_row(
            Text(result.status.upper(), style=style),
            result.operation,
            result.action.value,
            result.table_name,
            result.index_name,
            _category_label(result.category),
            ", ".join(result.column_names),
        )
    return table


def render_index_note(action: IndexAction) -> Panel:
    body = (
        "This command drops SQLAlchemy metadata-defined secondary indexes that currently exist in the database. Primary keys and constraints are not removed."
        if action is IndexAction.DISABLE
        else "This command recreates SQLAlchemy metadata-defined secondary indexes that are currently missing from the database and applies PostgreSQL clustering declared in ORM metadata when the backend supports it."
    )
    return Panel.fit(body, title="[bold]Note[/bold]", border_style="yellow")


def render_index_summary(results: Iterable[IndexManagementResult], *, dry_run: bool) -> Panel:
    items = list(results)
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Indexes", str(sum(item.operation == "index" for item in items)))
    cluster_count = sum(item.operation == "cluster" for item in items)
    if cluster_count:
        grid.add_row("Clusters", str(cluster_count))
    skipped = sum(item.status == "skipped" for item in items)
    if skipped:
        grid.add_row("Skipped", str(skipped))
    grid.add_row("Tables", str(len({item.table_name for item in items})))
    grid.add_row(
        "Summary",
        f"{'Planned' if dry_run else 'Applied'} {(items[0].action.value if items else 'manage')} on {len(items)} metadata operation(s).",
    )
    return Panel.fit(grid, title="[bold]Summary[/bold]", border_style="green" if not dry_run else "cyan")


def render_fulltext_results(results: Iterable[FullTextResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit(
            "No PostgreSQL full-text targets matched the current selection.",
            title="No Action",
            border_style="yellow",
        )

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Action")
    table.add_column("Table", style="bold")
    table.add_column("Source")
    table.add_column("Vector")
    table.add_column("Index")
    table.add_column("Rows", justify="right")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.action.value,
            result.table_name,
            result.source_column_name,
            result.vector_column_name,
            result.index_name,
            "-" if result.row_count is None else str(result.row_count),
            result.detail,
        )

    return table


def render_fulltext_summary(
    results: Iterable[FullTextResult],
    *,
    action: str,
    dry_run: bool,
) -> Panel:
    items = list(results)
    affected = sum(item.status in {"planned", "applied"} for item in items)
    populated_rows = sum(item.row_count or 0 for item in items)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Targets", str(affected))
    if action == "populate" and not dry_run:
        grid.add_row("Rows", str(populated_rows))
    grid.add_row(
        "Summary",
        (
            "Full-text changes planned; no PostgreSQL sidecar columns were modified."
            if dry_run
            else "PostgreSQL full-text sidecar management completed."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="cyan" if dry_run else "green",
    )


def render_truncate_results(results: Iterable[TruncateTableResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit(
            "No ORM-managed tables matched the current truncation selection.",
            title="No Action",
            border_style="yellow",
        )

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Rows", justify="right")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.table_name,
            _category_label(result.category),
            "-" if result.row_count is None else f"{result.row_count:,}",
            result.detail,
        )

    return table


def render_truncate_summary(
    results: Iterable[TruncateTableResult],
    *,
    dry_run: bool,
    restart_identities: bool,
    cascade: bool,
) -> Panel:
    items = list(results)
    truncated = [
        result
        for result in items
        if result.status in {"planned", "applied"}
    ]
    total_rows = sum(result.row_count or 0 for result in truncated)
    skipped = sum(result.status == "skipped" for result in items)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(truncated)))
    grid.add_row("Rows", f"{total_rows:,}")
    grid.add_row("Restart identities", "yes" if restart_identities else "no")
    grid.add_row("Cascade", "yes" if cascade else "no")
    if skipped:
        grid.add_row("Skipped", str(skipped))
    grid.add_row(
        "Summary",
        (
            "Table truncation planned; no data was removed."
            if dry_run
            else "Selected tables were truncated."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="cyan" if dry_run else "green",
    )


def render_truncate_note() -> Panel:
    return Panel.fit(
        "This command is destructive. Use `--dry-run` first, and pass `--yes` before applying changes.",
        title="[bold]Note[/bold]",
        border_style="yellow",
    )


def render_analyze_results(results: Iterable[AnalyzeTableResult]) -> RenderableType:
    items = list(results)
    if not items:
        return Panel.fit(
            "No ORM-managed tables matched the current analysis selection.",
            title="No Action",
            border_style="yellow",
        )

    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("Status")
    table.add_column("Table", style="bold")
    table.add_column("Category")
    table.add_column("Operation")
    table.add_column("Detail")

    for result in items:
        table.add_row(
            _status_text(result.status),
            result.table_name,
            _category_label(result.category),
            result.operation,
            result.detail,
        )

    return table


def render_analyze_summary(
    results: Iterable[AnalyzeTableResult],
    *,
    dry_run: bool,
) -> Panel:
    items = list(results)
    analyzed = [
        result
        for result in items
        if result.status in {"planned", "applied"}
    ]
    skipped = sum(result.status == "skipped" for result in items)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()
    grid.add_row("Tables", str(len(analyzed)))
    if analyzed:
        grid.add_row("Operation", analyzed[0].operation)
    if skipped:
        grid.add_row("Skipped", str(skipped))
    grid.add_row(
        "Summary",
        (
            "Table analysis planned; no maintenance statements were run."
            if dry_run
            else "Planner statistics refreshed for the selected tables."
        ),
    )
    return Panel.fit(
        grid,
        title="[bold]Summary[/bold]",
        border_style="cyan" if dry_run else "green",
    )


def render_analyze_note() -> Panel:
    return Panel.fit(
        "This command refreshes database statistics. It does not modify table rows.",
        title="[bold]Note[/bold]",
        border_style="yellow",
    )
