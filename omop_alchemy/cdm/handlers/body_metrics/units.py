from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from .concept_sets import BodySizeUnitConcepts, default_body_size_unit_concepts

LB_US_TO_KG = 0.45359237
INCH_TO_CM = 2.54


@dataclass(frozen=True)
class BodyUnitConversionRules:
    """
    Unit normalisation for anthropometric measurements.

    The handler keeps these conversions close to the body metrics that consume
    them instead of introducing a premature global unit engine.
    """

    units: BodySizeUnitConcepts
    lb_to_kg: float = LB_US_TO_KG
    inch_to_cm: float = INCH_TO_CM

    @classmethod
    def default(cls) -> "BodyUnitConversionRules":
        return default_body_unit_conversion_rules()

    @property
    def weight_unit_conversion_to_kg(self) -> dict[int, float]:
        return {
            self.units.kg: 1.0,
            self.units.lb: self.lb_to_kg,
        }

    @property
    def height_unit_conversion_to_cm(self) -> dict[int, float]:
        return {
            self.units.cm: 1.0,
            self.units.inch: self.inch_to_cm,
        }

    def normalize_weight_kg(
        self,
        value: Optional[float],
        unit_concept_id: Optional[int],
    ) -> Optional[float]:
        if value is None or unit_concept_id is None:
            return None
        factor = self.weight_unit_conversion_to_kg.get(unit_concept_id)
        return value * factor if factor is not None else None

    def normalize_height_cm(
        self,
        value: Optional[float],
        unit_concept_id: Optional[int],
    ) -> Optional[float]:
        if value is None or unit_concept_id is None:
            return None
        factor = self.height_unit_conversion_to_cm.get(unit_concept_id)
        return value * factor if factor is not None else None


@lru_cache(maxsize=1)
def default_body_unit_conversion_rules() -> BodyUnitConversionRules:
    return BodyUnitConversionRules(units=default_body_size_unit_concepts())
