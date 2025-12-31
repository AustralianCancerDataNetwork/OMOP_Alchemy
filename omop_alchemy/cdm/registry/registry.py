import pkgutil
import importlib
import sqlalchemy as sa
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, TypeGuard, Any, TYPE_CHECKING
from sqlalchemy.orm import Session

from .validators import is_type_compatible, normalize_omop_type, CDM_TO_SA
from ..specification import CDM_VERSION, TableSpec, FieldSpec, ModelDescriptor, load_table_specs, load_field_specs
from ..utils import get_logger

logger = get_logger(__name__)

from ..base.typing import ORMTable, DomainSemanticTable
from ..base.mixins import DomainValidationMixin, DomainRule

class SeverityLevel(Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"

DOMAIN_RULES = [
    DomainRule("drug_strength", "drug_concept_id", {"Drug"}),
    DomainRule("drug_strength", "ingredient_concept_id", {"Drug"}),
    DomainRule("drug_strength", "amount_unit_concept_id", {"Unit"}),
    DomainRule("drug_strength", "numerator_unit_concept_id", {"Unit"}),
    DomainRule("drug_strength", "denominator_unit_concept_id", {"Unit"}),
    DomainRule("source_to_concept_map", "source_concept_id", {"Metadata", "Observation", "Condition", "Procedure", "Drug", "Measurement", "Device", "Visit", "Type Concept", "Specimen"}, None),
    DomainRule("source_to_concept_map", "target_concept_id", {"Condition", "Drug", "Procedure", "Measurement", "Observation", "Device", "Visit", "Provider", "Metadata"}, None),
]

def is_domain_semantic_table(
    cls: Type[Any],
) -> TypeGuard[Type["DomainSemanticTable"]]:
    return (
        isinstance(cls, type)
        and isinstance(cls, ORMTable)
        and issubclass(cls, DomainValidationMixin)
    )

@dataclass
class ValidationIssue:
    table: str
    level: SeverityLevel   # "ERROR" | "WARN"
    message: str
    field: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    hint: Optional[str] = None

@dataclass
class ValidationReport:

    def __init__(self, *, cdm_version: str):
        self.cdm_version = cdm_version
        self.issues: list[ValidationIssue] = []
    
    def add(self, issue: ValidationIssue) -> None:
        self.issues.append(issue)
        
    def is_valid(self) -> bool:
        return not self.issues

    def summary(self) -> str:

        by = {SeverityLevel.ERROR: 0, SeverityLevel.WARN: 0, SeverityLevel.INFO: 0}
        for i in self.issues:
            by[i.level] += 1
        return f"CDM v{self.cdm_version}: {by[SeverityLevel.ERROR]} error(s), {by[SeverityLevel.WARN]} warning(s), {by[SeverityLevel.INFO]} info"
    
    def render_text(self) -> str:
        lines = []
        by_table = defaultdict(list)

        for issue in self.issues:
            by_table[issue.table].append(issue)

        for table, issues in sorted(by_table.items()):
            lines.append(f"\n📦 {table}")
            for i in issues:
                icon = "❌" if i.level == SeverityLevel.ERROR else "⚠️"
                hint = f" Hint: {i.hint}" if i.hint else ""
                field = f" (field: {i.field})" if i.field else ""
                lines.append(f"  {icon} {i.message}{field}{hint}")

        return "\n".join(lines)


class CDMModelRegistry:

    def __init__(self, *, cdm_version: str):
        self.cdm_version = cdm_version
        self._table_specs: dict[str, TableSpec] = {}
        self._field_specs: dict[str, dict[str, FieldSpec]] = {}
        self._models: dict[str, ModelDescriptor] = {}

    def load_specs(self, *, table_csv, field_csv) -> None:
        self._table_specs = load_table_specs(table_csv)
        self._field_specs = load_field_specs(field_csv)

    def register_model(self, model: type) -> None:
        desc = ModelDescriptor.from_model(model)
        self._models[desc.table_name] = desc

    def register_models(self, models: list[type]) -> None:
        for m in models:
            self.register_model(m)

    def known_tables(self) -> set[str]:
        return set(self._table_specs.keys())

    def registered_tables(self) -> set[str]:
        return set(self._models.keys())

    def missing_required_tables(self) -> set[str]:
        return {
            t for t, spec in self._table_specs.items()
            if spec.is_required and t not in self._models
        }
    
    def _validate_tables(self, report):
        logger.debug("Validating table registration against CDM specs")
        for t in sorted(self.known_tables() - self.registered_tables()):
            report.add(ValidationIssue(
                t, SeverityLevel.WARN, "TABLE_NOT_REGISTERED",
                hint="Table exists in CDM specs but no ORM model registered."
            ))
        
        for table_name, desc in self._models.items():
            spec = self._table_specs.get(table_name)
            if not spec:
                report.add(ValidationIssue(
                    table_name, SeverityLevel.WARN, "TABLE_NOT_IN_SPECS",
                    hint=f"Registered model has no matching table spec {table_name}."
                ))
                continue

        for table, spec in self._table_specs.items():
            if spec.is_required and table not in self._models:
                report.add(ValidationIssue(
                    table,
                    SeverityLevel.ERROR,
                    "REQUIRED_TABLE_NOT_IMPLEMENTED",
                    hint=f"Required table '{table}' not implemented",
                ))

    def _validate_fields(self, report):
        logger.debug("Validating column presence and nullability")
        for table, field_spec in self._field_specs.items():
            model = self._models.get(table)
            if not model:
                continue
            for field, spec in field_spec.items():
                col = model.columns.get(field)
                if col is None:
                    report.add(ValidationIssue( 
                        table,
                        SeverityLevel.ERROR,
                        "REQUIRED_COLUMN_MISSING",
                        field=field,
                        hint=f"Missing required column '{field}'",
                ))
                continue

            if spec.is_required and col.nullable: # type: ignore
                report.add(ValidationIssue(
                    table,
                    SeverityLevel.ERROR,
                    "REQUIRED_COLUMN_NULLABLE",
                    field=field,
                    hint=f"Column '{field}' should be NOT NULL",
                ))

    def _validate_foreign_keys(self, report):
        logger.debug("Validating foreign key targets against registered tables")
        for model in self._models.values():
            for col, (fk_table, fk_field) in model.foreign_keys.items():
                if fk_table not in self._table_specs:
                    report.add(ValidationIssue(
                        model.table_name,
                        SeverityLevel.ERROR,
                        "FOREIGN_KEY_INVALID_REFERENCE",
                        field=col,
                        hint=f"FK '{col}' references unknown table '{fk_table}'",
                    )) 

    def discover_models(self, package: str) -> None:
        module = importlib.import_module(package)

        for _, modname, _ in pkgutil.walk_packages(
            module.__path__, module.__name__ + "."
        ):
            mod = importlib.import_module(modname)

            for obj in vars(mod).values():
                if getattr(obj, "__abstract__", False):
                    continue
                if (
                    isinstance(obj, type)
                    and hasattr(obj, "__tablename__")
                    and hasattr(obj, "__mapper__")
                ):
                    logger.debug(f"Registering model: {obj.__tablename__}")
                    self.register_model(obj)

    def _validate_domain_rule(
        self,
        rule: DomainRule,
        report: ValidationReport,
        engine: Optional[sa.engine.Engine] = None,
    ) -> None:
        """
        Validate a DomainRule against registered ORM models and CDM specs.
        This validates the *rule definition*, not database contents.
        """

        table = rule.table
        field = rule.field

        model_desc = self._models.get(table)
        if not model_desc:
            report.add(
                ValidationIssue(
                    table=table,
                    level=SeverityLevel.ERROR,
                    message="DOMAIN_RULE_TABLE_NOT_REGISTERED",
                    hint=f"DomainRule refers to unknown table '{table}'",
                )
            )
            return

        if field not in model_desc.columns:
            report.add(
                ValidationIssue(
                    table=table,
                    field=field,
                    level=SeverityLevel.ERROR,
                    message="DOMAIN_RULE_FIELD_NOT_FOUND",
                    hint=f"DomainRule refers to unknown field '{field}' on table '{table}'",
                )
            )
            return

        if engine is not None:
            from ...model.vocabulary.domain import Domain

            with Session(engine) as session:
                known_domains = set([d[0] for d in session.query(Domain.domain_id).all()])
            unknown = rule.allowed_domains - known_domains
            if unknown:
                report.add(
                    ValidationIssue(
                        table=table,
                        field=field,
                        level=SeverityLevel.WARN,
                        message="DOMAIN_RULE_UNKNOWN_DOMAIN",
                        expected=", ".join(sorted(rule.allowed_domains)),
                        actual=", ".join(sorted(unknown)),
                        hint="DomainRule references domains not recognised in CDM",
                    )
                )
        else:
            logger.warning("No engine provided; skipping domain existence check for DomainRule on %s.%s", table, field)

        if rule.allowed_classes:
            report.add(
                ValidationIssue(
                    table=table,
                    field=field,
                    level=SeverityLevel.INFO,
                    message="DOMAIN_RULE_CLASS_CONSTRAINT",
                    expected=", ".join(sorted(rule.allowed_classes)),
                    hint="Concept class constraint declared (not yet enforced)",
                )
            )

    def _validate_domain_semantics(self, report):
        logger.debug("Collecting domain semantic rules from ORM models")
        for model in self._models.values():
            cls = model.model
            if is_domain_semantic_table(cls):
                rules = cls.collect_domain_rules()
                for rule in rules:
                    self._validate_domain_rule(rule, report)

    def _validate_domain_semantics_data(
        self,
        engine,
        report: ValidationReport,
    ) -> None:
        from omop_alchemy.model.vocabulary import Concept
        logger.info("Running domain semantic validation against database")
        with Session(engine) as session:
            for model in self._models.values():
                cls = model.model
                if is_domain_semantic_table(cls):
                    logger.info("Checking domain semantics in table '%s'", model.table_name)    
                    for field, expected in cls.__expected_domains__.items(): 
                        concept_fk = getattr(cls, field)
                        try:
                            rows = (
                                session.query(cls)
                                .join(
                                    Concept,
                                    concept_fk == Concept.concept_id,
                                )
                                .filter(~Concept.domain_id.in_(expected.domains))
                                .limit(10)
                                .all()
                            )
                        except Exception as e:
                            logger.error(
                                "Error querying domain semantics for %s.%s: %s",
                                model.table_name,
                                field,
                                str(e),
                            )
                            continue
                        c = len(rows)
                        if c > 0:
                            logger.warning(
                                "Domain semantic violations detected in table '%s' (showing up to 10)",
                                cls.__tablename__,
                            )
                            report.add(
                                ValidationIssue(
                                        table=cls.__tablename__,
                                        field=field,
                                        message=f"Concept domain not in {expected.domains} ({c} violation(s))",
                                        level=SeverityLevel.WARN
                                    )
                            )
                        else:
                            logger.info(
                                "No domain semantic violations found in table '%s' for field '%s'",
                                cls.__tablename__,
                                field,
                            )
                        
    def _validate_types(self, report):
        logger.debug("Validating column data types against CDM spec")
        for table, field_spec in self._field_specs.items():
            model = self._models.get(table)
            if not model:
                continue

            for field, spec in field_spec.items():

                col = model.columns.get(field)
                if col is None:
                    continue

                expected = normalize_omop_type(spec.data_type) # type: ignore
                allowed = CDM_TO_SA.get(expected)

                if not allowed:
                    report.warning(
                        table,
                        f"Unknown OMOP type '{spec.data_type}' for {field}", # type: ignore
                    )
                    continue

                if not is_type_compatible(spec.data_type, col): 
                    report.add(
                        ValidationIssue(
                            table=table,
                            level=SeverityLevel.ERROR,
                            field=field,
                            expected=expected,
                            actual=str(col.type),
                            hint=f"Column '{field}' type mismatch: expected {expected}, got {type(col.type)}",
                            message=(
                                f"Column '{field}' type mismatch: "
                                f"expected {expected}, got {type(col.type)}"
                            )
                    ))

    def _validate_database_schema(self, engine, report):
        logger.info("Validating database schema against ORM definitions")

        insp = sa.inspect(engine)

        for table_name, model in self._models.items():
            if not insp.has_table(table_name):
                report.add(ValidationIssue(
                    table=table_name,
                    level=SeverityLevel.ERROR,
                    message="TABLE_MISSING_IN_DATABASE",
                    hint="ORM model defined but table not found in database",
                ))
                continue

            db_cols = {c["name"]: c for c in insp.get_columns(table_name)}

            for col_name, orm_col in model.columns.items():
                db_col = db_cols.get(col_name)
                if not db_col:
                    report.add(ValidationIssue(
                        table=table_name,
                        level=SeverityLevel.ERROR,
                        field=col_name,
                        message="COLUMN_MISSING_IN_DATABASE",
                    ))
                    continue

                if orm_col.nullable is False and db_col["nullable"] is True:
                    report.add(ValidationIssue(
                        table=table_name,
                        level=SeverityLevel.ERROR,
                        field=col_name,
                        message="COLUMN_NULLABILITY_MISMATCH",
                        expected="NOT NULL",
                        actual="NULL",
                    ))


    def validate(
            self,
            *,
            engine=None,
            check_types: bool = True,
            check_fks: bool = True,
            check_domain_semantics: bool = False,
    ) -> "ValidationReport":
        
        logger.info(
            "Starting CDM model validation (types=%s, fks=%s, domain_semantics=%s, engine=%s)",
            check_types,
            check_fks,
            check_domain_semantics,
            engine is not None,
        )

        report = ValidationReport(cdm_version=CDM_VERSION)

        self._validate_tables(report)
        self._validate_fields(report)
        if check_fks:
            self._validate_foreign_keys(report)
        if check_types:
            self._validate_types(report)
        if engine is not None:
            self._validate_database_schema(engine, report)
        if check_domain_semantics:
            self._validate_domain_semantics(report)
            if engine is not None:
                self._validate_domain_semantics_data(engine, report)
            else:
                logger.warning("Domain semantic validation requested but no engine provided; skipping data checks")
        return report

