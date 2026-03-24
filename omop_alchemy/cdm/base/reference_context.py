
from __future__ import annotations
import sqlalchemy.orm as so
from typing import Type, Any

class ReferenceContext:

    """
    `ReferenceContext` 
    
    A helper base class for defining **read-only reference relationships**.

    This class is purely structural: it resolves foreign keys
    into reference tables (Domain, Vocabulary, ConceptClass, etc.)
    with explicit join conditions.

    These relationships are:

    - `viewonly=True`
    - explicitly joined
    - loaded using `selectin` (batched eager loading)
    - defined outside the core table
    - deterministic projections of foreign keys

    They are intended for:

    - inspection
    - analytics
    - debugging
    - view-level navigation
    — **not** for ETL or mutation.
    """

    @classmethod
    def _reference_relationship(
        cls,
        *,
        target: str,
        local_fk: str,
        remote_pk: str,
        uselist: bool = False,
    ):
        
        def _relationship(cls_: Type[Any]) -> Any:
            return so.relationship(
                target,
                primaryjoin=f"{cls_.__name__}.{local_fk} == {target}.{remote_pk}",
                foreign_keys=f"{cls_.__name__}.{local_fk}",
                viewonly=True,
                lazy="selectin",
                uselist=uselist,
            )
        return so.declared_attr(_relationship)
    

        # return so.declared_attr(
        #     lambda cls_: so.relationship(
        #         target,
        #         primaryjoin=lambda: getattr(cls_, local_fk) == getattr(
        #             sa.inspect(cls_).registry._class_registry[target],
        #             remote_pk,
        #         ),
        #         foreign_keys=lambda: getattr(cls_, local_fk),
        #         viewonly=True,
        #         lazy="selectin",
        #         uselist=uselist,
        #     )
        # )