import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base

from omop_alchemy.cdm.base import (
    CDMTableBase,
    cdm_table, 
    required_concept_fk,
)

@cdm_table
class Fact_Relationship(CDMTableBase, Base):
    __tablename__ = "fact_relationship"

    domain_concept_id_1: so.Mapped[int] = required_concept_fk()
    fact_id_1: so.Mapped[int] = so.mapped_column(sa.Integer,nullable=False)
    domain_concept_id_2: so.Mapped[int] = required_concept_fk()
    fact_id_2: so.Mapped[int] = so.mapped_column(sa.Integer,nullable=False)
    relationship_concept_id: so.Mapped[int] = required_concept_fk()

    __table_args__ = (
        sa.PrimaryKeyConstraint(
            "domain_concept_id_1",
            "fact_id_1",
            "domain_concept_id_2",
            "fact_id_2",
            "relationship_concept_id",
        ),
    )

    def __repr__(self) -> str:
        return (
            "<FactRelationship "
            f"{self.domain_concept_id_1}:{self.fact_id_1} "
            f"-[{self.relationship_concept_id}]-> "
            f"{self.domain_concept_id_2}:{self.fact_id_2}>"
        )