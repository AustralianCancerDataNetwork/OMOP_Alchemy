from __future__ import annotations

from dataclasses import dataclass

from omop_alchemy.cdm.handlers._semantics import default_semantics_runtime


@dataclass(frozen=True)
class BodySizeMeasurementConcepts:
    weight: int
    height: int


@dataclass(frozen=True)
class BodySizeUnitConcepts:
    kg: int
    lb: int
    cm: int
    inch: int
    m2: int


def default_body_size_measurement_concepts() -> BodySizeMeasurementConcepts:
    runtime = default_semantics_runtime()
    measurements = runtime.measurements_numeric.body_size_measurements
    return BodySizeMeasurementConcepts(
        weight=measurements.weight,
        height=measurements.height,
    )


def default_body_size_unit_concepts() -> BodySizeUnitConcepts:
    runtime = default_semantics_runtime()
    units = runtime.measurements_numeric.body_size_units
    return BodySizeUnitConcepts(
        kg=units.kg,
        lb=units.lb,
        cm=units.cm,
        inch=units.inch,
        m2=units.m2,
    )
