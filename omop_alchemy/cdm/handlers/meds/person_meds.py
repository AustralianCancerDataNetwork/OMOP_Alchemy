from __future__ import annotations

from omop_alchemy.cdm.handlers.meds._guards import *  # noqa: F401,F403

import pyarrow as pa
import meds
from datetime import datetime, time
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import object_session

from omop_alchemy.cdm.model.clinical import Person
from omop_alchemy.cdm.model.clinical.death import Death
from omop_alchemy.cdm.model.derived import Observation_Period
from omop_alchemy.cdm.handlers.meds.meds_event import (
    Condition_MEDS_Event,
    Drug_MEDS_Event,
    Measurement_MEDS_Event,
    Observation_MEDS_Event,
    Procedure_MEDS_Event,
    Device_MEDS_Event,
)

def _to_datetime(d: object, *, end: bool = False) -> datetime:
    """Coerce a date or datetime to datetime; use midnight or 23:59:59 for bare dates."""
    if isinstance(d, datetime):
        return d
    return datetime.combine(d, time(23, 59, 59, 999999) if end else time.min)  # type: ignore[arg-type]


class Person_MEDS(Person):
    """Person view that assembles a complete MEDS event stream.

    Mirrors Person_Timeline but serialises to MEDS row dicts and PyArrow
    tables rather than JSON.  Usable independently of MEDSWriter for
    single-patient export or interactive exploration.

    Quick single-patient export::

        from omop_alchemy.cdm.handlers.meds.code_metadata import build_concept_id_map
        from omop_alchemy.cdm.handlers.meds.person_meds import Person_MEDS

        code_map, _ = build_concept_id_map(session)
        person = session.get(Person_MEDS, 42)

        rows = person.meds_rows(code_map)
        # [{"subject_id": 42, "time": datetime(1975, 3, 1), "code": "MEDS_BIRTH", ...},
        #  {"subject_id": 42, "time": datetime(1975, 3, 1), "code": "Gender/8507", ...},
        #  {"subject_id": 42, "time": datetime(2020, 5, 12), "code": "SNOMED/44054006", ...},
        #  ...]  # sorted by time ascending

        table = person.to_meds_table(code_map)
        # PyArrow table aligned to meds.DataSchema, ready for pq.write_table()
    """

    EVENT_TABLES = (
        Condition_MEDS_Event,
        Drug_MEDS_Event,
        Measurement_MEDS_Event,
        Observation_MEDS_Event,
        Procedure_MEDS_Event,
        Device_MEDS_Event,
    )

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    @property
    def _birth_datetime(self) -> datetime:
        """TR-07a: coalesce birth_datetime with year/month/day_of_birth."""
        if self.birth_datetime is not None:
            return _to_datetime(self.birth_datetime)
        return datetime(
            self.year_of_birth,
            self.month_of_birth or 1,
            self.day_of_birth or 1,
        )

    def _observation_period_rows(self) -> list[dict[str, Any]]:
        """TR-07d: one interval row per Observation_Period (code OMOP/OBSERVATION_PERIOD)."""
        session = object_session(self)
        if session is None:
            return []
        periods = session.execute(
            sa.select(Observation_Period).where(
                Observation_Period.person_id == self.person_id
            )
        ).scalars().all()

        rows = []
        for op in periods:
            rows.append({
                "subject_id": self.person_id,
                "time": _to_datetime(op.observation_period_start_date),
                "code": "OMOP/OBSERVATION_PERIOD",
                "numeric_value": None,
                "text_value": None,
                "table": "observation_period",
                "end": _to_datetime(op.observation_period_end_date, end=True),
            })
        return rows

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def demographic_rows(self, concept_id_map: dict[int, str]) -> list[dict[str, Any]]:
        """TR-07: birth, demographic concept, and death events for this person.

        All rows except death are timestamped at birth_datetime.
        Gender, race, and ethnicity are emitted only when their concept_id
        resolves in concept_id_map (unmapped concept_id=0 is silently skipped).
        """
        birth_dt = self._birth_datetime
        rows: list[dict[str, Any]] = []

        rows.append({
            "subject_id": self.person_id,
            "time": birth_dt,
            "code": meds.birth_code,
            "numeric_value": None,
            "text_value": None,
            "table": "person",
        })

        for concept_id in (
            self.gender_concept_id,
            self.race_concept_id,
            self.ethnicity_concept_id,
        ):
            if concept_id and concept_id != 0:
                code = concept_id_map.get(int(concept_id))
                if code:
                    rows.append({
                        "subject_id": self.person_id,
                        "time": birth_dt,
                        "code": code,
                        "numeric_value": None,
                        "text_value": None,
                        "table": "person",
                    })

        session = object_session(self)
        if session is not None:
            death = session.execute(
                sa.select(Death).where(Death.person_id == self.person_id)
            ).scalar_one_or_none()
            if death is not None:
                death_dt = (
                    _to_datetime(death.death_datetime)
                    if death.death_datetime is not None
                    else _to_datetime(death.death_date)
                )
                rows.append({
                    "subject_id": self.person_id,
                    "time": death_dt,
                    "code": meds.death_code,
                    "numeric_value": None,
                    "text_value": None,
                    "table": "death",
                })

        return rows

    def meds_rows(
        self,
        concept_id_map: dict[int, str],
        *,
        include_observation_periods: bool = True,
    ) -> list[dict[str, Any]]:
        """All MEDS event rows for this person, sorted by time ascending.

        Rows with equal times preserve their collection order (demographics
        first, then observation periods, then clinical events by table).

        Example::

            code_map, _ = build_concept_id_map(session)
            rows = session.get(Person_MEDS, person_id).meds_rows(code_map)
            codes = [r["code"] for r in rows]
            # ["MEDS_BIRTH", "Gender/8532", "OMOP/OBSERVATION_PERIOD",
            #  "SNOMED/73211009", "LOINC/2160-0", "MEDS_DEATH"]
        """
        session = object_session(self)
        rows = self.demographic_rows(concept_id_map)

        if include_observation_periods:
            rows.extend(self._observation_period_rows())

        if session is not None:
            for EventCls in self.EVENT_TABLES:
                stmt = sa.select(EventCls).where(
                    EventCls.person_id == self.person_id
                )
                for event in session.execute(stmt).scalars():
                    rows.extend(event.to_meds_rows(concept_id_map))

        rows.sort(key=lambda r: r["time"])
        return rows

    def to_meds_table(
        self,
        concept_id_map: dict[int, str],
        *,
        include_observation_periods: bool = True,
    ) -> pa.Table:
        """Return a DataSchema-aligned PyArrow table for this person.

        Validates against meds.DataSchema before returning.  Extension
        columns (table, end, visit_id, unit) are preserved alongside the
        core MEDS columns.

        Example::

            import pyarrow.parquet as pq

            code_map, _ = build_concept_id_map(session)
            person = session.get(Person_MEDS, 42)
            table = person.to_meds_table(code_map)

            print(table.schema)
            # subject_id: int64
            # time: timestamp[us]
            # code: string
            # numeric_value: float
            # text_value: large_string
            # table: string  (extension)
            # end: timestamp[us]  (extension, nullable)

            pq.write_table(table, "/tmp/patient_42.parquet")
        """
        rows = self.meds_rows(
            concept_id_map,
            include_observation_periods=include_observation_periods,
        )
        if not rows:
            return meds.DataSchema.align(pa.table({
                "subject_id": pa.array([], type=pa.int64()),
                "time": pa.array([], type=pa.timestamp("us")),
                "code": pa.array([], type=pa.string()),
                "numeric_value": pa.array([], type=pa.float32()),
                "text_value": pa.array([], type=pa.large_string()),
            }))

        table = pa.Table.from_pylist(rows)
        return meds.DataSchema.align(table)
