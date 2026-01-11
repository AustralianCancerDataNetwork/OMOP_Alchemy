from sqlalchemy import orm as so
from orm_loader.helpers import get_model_by_tablename

from ..registry import DomainRule

class ExpectedDomain:
    def __init__(self, *domains: str):
        self.domains = set(domains)

class DomainValidationMixin:
    """
    Adds lightweight OMOP domain validation helpers.

    Intended for *View* classes only.
    """
    __expected_domains__: dict[str, ExpectedDomain] = {}

    @classmethod
    def collect_domain_rules(cls) -> list[DomainRule]:
        rules: list[DomainRule] = []
        
        if not hasattr(cls, "__tablename__"):
            raise TypeError(
                f"{cls.__name__} defines domain rules but is not a mapped table"
            )
        
        for field, spec in cls.__expected_domains__.items():
            rules.append(
                DomainRule(
                    table=cls.__tablename__, # type: ignore[attr-defined]
                    field=field,
                    allowed_domains=spec.domains,
                )
            )
        return rules

    def _check_domain(self, field: str) -> bool:
        expected = self.__expected_domains__.get(field)
        if not expected:
            return True

        concept_id = getattr(self, field)
        if concept_id == 0:
            return True  # OMOP allows 0

        session = so.object_session(self)
        if session is None:
            return True  # detached; best-effort
        # need to be able to query concept table but can't import directly here to avoid circular imports
        ConceptCls = get_model_by_tablename("Concept")
        if ConceptCls is None:
            return False
        concept = session.get(ConceptCls, concept_id) # type: ignore
        return concept.domain_id in expected.domains if concept else False


    @property
    def domain_violations(self) -> list[str]:
        issues = []
        for field, expected in self.__expected_domains__.items():
            if not self._check_domain(field):
                issues.append(
                    f"{field} not in domain(s): {sorted(expected.domains)}"
                )
        return issues

    @property
    def is_domain_valid(self) -> bool:
        return not self.domain_violations

