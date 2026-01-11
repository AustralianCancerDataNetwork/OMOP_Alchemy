import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.base import ReferenceTable, cdm_table, CDMTableBase

@cdm_table
class Domain(Base, ReferenceTable, CDMTableBase):
    __tablename__ = "domain"
    domain_id: so.Mapped[str] = so.mapped_column(sa.String(20), primary_key=True)
    domain_name: so.Mapped[str] = so.mapped_column(sa.String(255), nullable=False)
    domain_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey("concept.concept_id"),nullable=False)

    def __repr__(self):
        return f'<Domain {self.domain_id} - {self.domain_name}>'