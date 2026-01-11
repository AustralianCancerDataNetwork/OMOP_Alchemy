from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class DomainRule:
    table: str
    field: str
    allowed_domains: set[str]
    allowed_classes: Optional[set[str]] = None
