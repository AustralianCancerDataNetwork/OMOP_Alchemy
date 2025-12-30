import pkgutil
import importlib
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Type, TypeGuard, Any, TYPE_CHECKING

from .validators import is_type_compatible, normalize_omop_type, CDM_TO_SA
from ..specification import CDM_VERSION, TableSpec, FieldSpec, ModelDescriptor, load_table_specs, load_field_specs

if TYPE_CHECKING:
    from omop_alchemy.cdm.base import ORMTable, DomainValidationMixin, DomainSemanticTable

class SeverityLevel(Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"

@dataclass(frozen=True)
class DomainRule:
    table: str
    field: str
    allowed_domains: set[str]
    # Optional: concept_class_id constraints
    allowed_classes: Optional[set[str]] = None

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
        and isinstance(cls, "ORMTable")
        and issubclass(cls, "DomainValidationMixin")
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
                    self.register_model(obj)

    def _validate_domain_semantics(self, report):
        for model in self._models.values():
            cls = model.model
            if is_domain_semantic_table(cls):
                rules = cls.collect_domain_rules()
                for rule in rules:
                    report.add_domain_rule(rule)

    def _validate_domain_semantics_data(
        self,
        engine,
        report: ValidationReport,
    ) -> None:
        from sqlalchemy.orm import Session
        from omop_alchemy.model.vocabulary import Concept

        with Session(engine) as session:
            for model in self._models.values():
                cls = model.model
                if is_domain_semantic_table(cls):
                    for field, expected in cls.__expected_domains__.items():
                        concept_fk = getattr(cls, field)

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
                    c = len(rows)
                    report.add(
                        ValidationIssue(
                                table=cls.__tablename__,
                                field=field,
                                message=f"Concept domain not in {expected.domains} ({c} violation(s))",
                                level=SeverityLevel.WARN
                            )
                    )
                        

    def validate(
            self,
            *,
            engine=None,
            check_types: bool = True,
            check_fks: bool = True,
            check_domain_semantics: bool = False,
    ) -> "ValidationReport":
        report = ValidationReport(cdm_version=CDM_VERSION)

        self._validate_tables(report)
        self._validate_fields(report)
        if check_fks:
            self._validate_foreign_keys(report)
        if check_types:
            self._validate_types(report)
        if check_domain_semantics:
            self._validate_domain_semantics(report)
        return report

    def _validate_types(self, report):
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
