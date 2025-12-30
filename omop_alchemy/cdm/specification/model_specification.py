from dataclasses import dataclass
from typing import Optional
import csv
import sqlalchemy as sa

@dataclass(frozen=True)
class TableSpec:
    table_name: str
    schema: str
    is_required: bool
    description: str
    user_guidance: Optional[str] = None

@dataclass(frozen=True)
class FieldSpec:
    table_name: str
    field_name: str
    data_type: str
    is_required: bool
    is_primary_key: bool
    is_foreign_key: bool
    fk_table: str | None
    fk_field: str | None

@dataclass(frozen=True)
class ModelDescriptor:
    model: type
    table_name: str
    columns: dict[str, sa.Column]
    primary_keys: set[str]
    foreign_keys: dict[str, tuple[str, str]]  # col -> (table, field)


    @classmethod
    def from_model(cls, model: type) -> "ModelDescriptor":
        mapper = sa.inspect(model)
        table = mapper.local_table

        fks = {}
        for col in table.columns:
            for fk in col.foreign_keys:
                fks[col.name] = (
                    fk.column.table.name,
                    fk.column.name,
                )

        return cls(
            model=model,
            table_name=table.name,
            columns={c.name: c for c in table.columns},
            primary_keys={c.name for c in table.primary_key.columns},
            foreign_keys=fks,
        )

def load_table_specs(csv_resource) -> dict[str, TableSpec]:
    out = {}
    with csv_resource.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out[row["cdmTableName"].lower()] = TableSpec(
                table_name=row["cdmTableName"].lower(),
                schema=row["schema"],
                is_required=row["isRequired"].lower() == "yes",
                description=row["tableDescription"],
                user_guidance=row.get("userGuidance"),
            )
    return out

def load_field_specs(csv_resource) -> dict[str, dict[str, FieldSpec]]:
    out: dict[str, dict[str, FieldSpec]] = {}
    with csv_resource.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row["cdmTableName"].lower()
            field = row["cdmFieldName"].lower()
            out.setdefault(table, {})[field] = FieldSpec(
                table_name=table,
                field_name=field,
                is_required=row["isRequired"].lower() == "yes",
                data_type=row["cdmDatatype"],
                is_primary_key=row["isPrimaryKey"].lower() == "yes",
                is_foreign_key=row["isForeignKey"].lower() == "yes",
                fk_table=row.get("fkCdmTableName"),
                fk_field=row.get("fkCdmFieldName"),
            )
    return out