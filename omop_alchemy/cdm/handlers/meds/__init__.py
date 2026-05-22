"""MEDS export handler for OMOP Alchemy.

Converts an OMOP CDM database to a `MEDS`_-compliant dataset using PyArrow
for serialisation.  Requires the optional extras::

    pip install omop_alchemy[meds]

Minimal end-to-end usage::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from omop_alchemy.cdm.handlers.meds import MEDSWriter

    engine = create_engine("postgresql+psycopg2://user:pass@host/omop_db")
    Session = sessionmaker(bind=engine)

    with Session() as session:
        writer = MEDSWriter(session, "/path/to/output")
        result = writer.write()

    print(f"Exported {result.subjects_exported} subjects, "
          f"{result.events_emitted} events across {result.shards_written} shards.")
    if result.drop_counts:
        print("Dropped events per table:", result.drop_counts)

Single-patient interactive usage::

    from omop_alchemy.cdm.handlers.meds import Person_MEDS, build_concept_id_map

    code_map, _ = build_concept_id_map(session)
    person = session.get(Person_MEDS, 12345)
    table = person.to_meds_table(code_map)   # pa.Table, validates DataSchema

.. _MEDS: https://github.com/Medical-Event-Data-Standard/meds
"""
from omop_alchemy.cdm.handlers.meds._guards import *  # noqa: F401,F403

from omop_alchemy.cdm.handlers.meds.code_metadata import (
    build_concept_id_map,
    CUSTOM_CONCEPT_ID_START,
    build_code_metadata,
)
from omop_alchemy.cdm.handlers.meds.meds_event import (
    MEDSEvent,
    Condition_MEDS_Event,
    Drug_MEDS_Event,
    Measurement_MEDS_Event,
    Observation_MEDS_Event,
    Procedure_MEDS_Event,
    Device_MEDS_Event,
)
from omop_alchemy.cdm.handlers.meds.person_meds import Person_MEDS
from omop_alchemy.cdm.handlers.meds.writer import MEDSWriter, WriteResult

__all__ = [
    "build_concept_id_map",
    "CUSTOM_CONCEPT_ID_START",
    "build_code_metadata",
    "MEDSEvent",
    "Condition_MEDS_Event",
    "Drug_MEDS_Event",
    "Measurement_MEDS_Event",
    "Observation_MEDS_Event",
    "Procedure_MEDS_Event",
    "Device_MEDS_Event",
    "Person_MEDS",
    "MEDSWriter",
    "WriteResult",
]
