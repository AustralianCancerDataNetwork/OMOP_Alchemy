from __future__ import annotations

from omop_alchemy.cdm.handlers.meds._guards import *  # noqa: F401,F403

from typing import Any

from omop_alchemy.cdm.handlers.timeline.event_timeline import ClinicalEvent, EventMapping
from omop_alchemy.cdm.model.clinical import (
    Condition_Occurrence,
    Device_Exposure,
    Drug_Exposure,
    Measurement,
    Observation,
    Procedure_Occurrence,
)


class MEDSEvent(ClinicalEvent):
    """Extends ClinicalEvent with MEDS row serialisation.

    Concrete subclasses set these class-level attributes to describe their
    OMOP table layout:
        _source_concept_field      name of the source_concept_id column (or None)
        _table_name                OMOP table name written to the "table" extension col
        _unit_source_value_field   raw unit string column (or None)
        _unit_concept_id_field     unit concept_id column (or None)
        _value_source_value_field  SOURCE_CODE fallback column (or None)

    Each concrete subclass is a SQLAlchemy polymorphic view over its OMOP
    table, so instances are obtained via a session query, not constructed
    directly::

        from omop_alchemy.cdm.handlers.meds.code_metadata import build_concept_id_map
        from omop_alchemy.cdm.handlers.meds.meds_event import Condition_MEDS_Event

        code_map, _ = build_concept_id_map(session)

        events = session.scalars(
            sa.select(Condition_MEDS_Event).where(
                Condition_MEDS_Event.person_id == 123
            )
        ).all()

        rows = []
        for event in events:
            rows.extend(event.to_meds_rows(code_map))
        # rows → [{"subject_id": 123, "time": datetime(2021, 3, 4), "code": "SNOMED/73211009",
        #          "numeric_value": None, "text_value": None, "table": "condition_occurrence"}]
    """

    _source_concept_field: str | None = None
    _table_name: str = ""
    _unit_source_value_field: str | None = None
    _unit_concept_id_field: str | None = None
    _value_source_value_field: str | None = None

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _resolve_code(self, concept_id_map: dict[int, str]) -> str | None:
        """TR-03b: source_concept_id > standard concept_id > None (drop row)."""
        if self._source_concept_field:
            src = int(getattr(self, self._source_concept_field, None) or 0)
            if src != 0:
                code = concept_id_map.get(src)
                if code:
                    return code
        cid = self.concept_id
        if cid and cid != 0:
            return concept_id_map.get(int(cid))
        return None

    def _resolve_numeric_text(
        self, concept_id_map: dict[int, str]
    ) -> tuple[float | None, str | None]:
        """TR-06: EventValue → (numeric_value, text_value).

        Concept-valued results follow the meds_etl convention:
          - event has no source concept (src==0) AND value_source_value exists
              → "SOURCE_CODE/{value_source_value}"
          - otherwise
              → "OMOP_CONCEPT_ID/{value_as_concept_id}"
        """
        ev = self.event_value()

        if ev.type == "numeric":
            try:
                return (float(ev.value), None)
            except (TypeError, ValueError):
                return (None, None)

        if ev.type == "string":
            s = str(ev.value).strip() if ev.value is not None else ""
            return (None, s or None)

        if ev.type == "concept" and ev.value and int(ev.value) != 0:
            cid = int(ev.value)
            event_src = (
                int(getattr(self, self._source_concept_field, None) or 0)
                if self._source_concept_field else 0
            )
            if event_src == 0 and self._value_source_value_field:
                sv = getattr(self, self._value_source_value_field, None)
                if sv:
                    return (None, f"SOURCE_CODE/{sv}")
            return (None, f"OMOP_CONCEPT_ID/{cid}")

        return (None, None)

    def _resolve_unit(self, concept_id_map: dict[int, str]) -> str | None:
        """Coalesce unit_source_value, then unit_concept_id lookup."""
        if self._unit_source_value_field:
            uv = getattr(self, self._unit_source_value_field, None)
            if uv:
                return str(uv)
        if self._unit_concept_id_field:
            ucid = int(getattr(self, self._unit_concept_id_field, None) or 0)
            if ucid != 0:
                return concept_id_map.get(ucid)
        return None

    # ------------------------------------------------------------------ #
    # Public interface                                                   #
    # ------------------------------------------------------------------ #

    def to_meds_rows(self, concept_id_map: dict[int, str]) -> list[dict[str, Any]]:
        """Return zero or one MEDS row dicts for this event.

        Returns [] when the event code resolves to None (TR-03c: unmapped
        concept_id=0 rows are silently dropped; the caller counts drops).

        Row keys always present:
            subject_id, time, code, numeric_value, text_value, table

        Row keys present when applicable:
            visit_id  — if visit_occurrence_id is non-null
            unit      — coalesced from unit_source_value / unit_concept_id
            end       — end datetime for interval events (TR-05b)

        Example — a measurement with a numeric value and unit::

            code_map, _ = build_concept_id_map(session)
            event = session.get(Measurement_MEDS_Event, measurement_id)
            event.to_meds_rows(code_map)
            # [{"subject_id": 7, "time": datetime(2022, 6, 1, 9, 0),
            #   "code": "LOINC/2160-0", "numeric_value": 1.1,
            #   "text_value": None, "unit": "mg/dL", "table": "measurement"}]

        Example — an unmapped event (concept_id == 0) returns an empty list::

            event.to_meds_rows({})   # → []
        """
        code = self._resolve_code(concept_id_map)
        if code is None:
            return []

        et = self.event_time
        numeric_value, text_value = self._resolve_numeric_text(concept_id_map)

        row: dict[str, Any] = {
            "subject_id": self.person_id,
            "time": et.start,
            "code": code,
            "numeric_value": numeric_value,
            "text_value": text_value,
            "table": self._table_name or self.__class__.__tablename__,
        }

        visit_id = getattr(self, "visit_occurrence_id", None)
        if visit_id is not None:
            row["visit_id"] = int(visit_id)

        unit = self._resolve_unit(concept_id_map)
        if unit is not None:
            row["unit"] = unit

        if et.end is not None:
            row["end"] = et.end

        return [row]


# ------------------------------------------------------------------ #
# Concrete event classes                                               #
# ------------------------------------------------------------------ #

class Condition_MEDS_Event(Condition_Occurrence, MEDSEvent):
    _mapping = EventMapping(
        concept_field="condition_concept_id",
        start_date_field="condition_start_date",
        start_datetime_field="condition_start_datetime",
        end_date_field="condition_end_date",
        end_datetime_field="condition_end_datetime",
    )
    _source_concept_field = "condition_source_concept_id"
    _table_name = "condition_occurrence"


class Drug_MEDS_Event(Drug_Exposure, MEDSEvent):
    _mapping = EventMapping(
        concept_field="drug_concept_id",
        start_date_field="drug_exposure_start_date",
        start_datetime_field="drug_exposure_start_datetime",
        end_date_field="drug_exposure_end_date",
        end_datetime_field="drug_exposure_end_datetime",
        value_fields=["quantity"],
    )
    _source_concept_field = "drug_source_concept_id"
    _table_name = "drug_exposure"


class Measurement_MEDS_Event(Measurement, MEDSEvent):
    _mapping = EventMapping(
        concept_field="measurement_concept_id",
        start_date_field="measurement_date",
        start_datetime_field="measurement_datetime",
        value_fields=["value_as_concept_id", "value_as_number", "value_as_string"],
    )
    _source_concept_field = "measurement_source_concept_id"
    _table_name = "measurement"
    _unit_source_value_field = "unit_source_value"
    _unit_concept_id_field = "unit_concept_id"
    _value_source_value_field = "value_source_value"


class Observation_MEDS_Event(Observation, MEDSEvent):
    _mapping = EventMapping(
        concept_field="observation_concept_id",
        start_date_field="observation_date",
        start_datetime_field="observation_datetime",
        value_fields=["value_as_concept_id", "value_as_number", "value_as_string"],
    )
    _source_concept_field = "observation_source_concept_id"
    _table_name = "observation"
    _unit_source_value_field = "unit_source_value"
    _value_source_value_field = "value_source_value"


class Procedure_MEDS_Event(Procedure_Occurrence, MEDSEvent):
    _mapping = EventMapping(
        concept_field="procedure_concept_id",
        start_date_field="procedure_date",
        start_datetime_field="procedure_datetime",
        end_date_field="procedure_end_date",
        end_datetime_field="procedure_end_datetime",
    )
    _source_concept_field = "procedure_source_concept_id"
    _table_name = "procedure_occurrence"


class Device_MEDS_Event(Device_Exposure, MEDSEvent):
    _mapping = EventMapping(
        concept_field="device_concept_id",
        start_date_field="device_exposure_start_date",
        start_datetime_field="device_exposure_start_datetime",
        end_date_field="device_exposure_end_date",
        end_datetime_field="device_exposure_end_datetime",
    )
    _source_concept_field = "device_source_concept_id"
    _table_name = "device_exposure"
    _unit_source_value_field = "unit_source_value"
    _unit_concept_id_field = "unit_concept_id"
