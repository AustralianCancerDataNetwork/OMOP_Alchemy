from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
import sqlalchemy as sa

class CreateMaterializedView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

@compiler.compiles(CreateMaterializedView)
def _create_view(element, compiler, **kw):
    compiled = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {element.name} as {compiled}"

class MaterializedViewMixin:
    __mv_name__: str
    __mv_select__: sa.sql.Select

    @classmethod
    def create_mv(cls, bind):
        ddl = CreateMaterializedView(cls.__mv_name__, cls.__mv_select__)
        bind.execute(ddl)

    @classmethod
    def refresh_mv(cls, bind):
        bind.execute(sa.text(f"REFRESH MATERIALIZED VIEW {cls.__mv_name__};"))
        