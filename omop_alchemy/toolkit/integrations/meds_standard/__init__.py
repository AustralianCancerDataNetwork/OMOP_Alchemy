"""Export an OMOP CDM database to a MEDS-compliant dataset.

The `Medical Event Data Standard`_ represents a patient's record as a flat
sequence of coded events.  This module converts a CDM instance into that
form, serialising to sharded Parquet with PyArrow and emitting the code
metadata that makes the export self-describing.

Requires the optional extras::

    pip install omop_alchemy[meds]

Whole-dataset export::

    from omop_alchemy.toolkit.integrations.meds_standard import MEDSWriter

    with Session() as session:
        result = MEDSWriter(session, "/path/to/output").write()

    print(f"Exported {result.subjects_exported} subjects, "
          f"{result.events_emitted} events across {result.shards_written} shards.")

``WriteResult`` also reports ``drop_counts`` per source table.  Rows that
cannot be represented as MEDS events are dropped and counted rather than
silently omitted, so an export can be reconciled against its source.

Single-patient export, for inspection or interactive work::

    from omop_alchemy.toolkit.integrations.meds_standard import (
        Person_MEDS,
        build_concept_id_map,
    )

    code_map, _ = build_concept_id_map(session)
    person = session.get(Person_MEDS, 12345)
    table = person.to_meds_table(code_map)   # pa.Table, validates DataSchema

Conditions, drug exposures, measurements, observations, procedures, and
device exposures each have an event class that maps their CDM columns onto
the MEDS event schema.  Source concepts with no standard mapping are
assigned custom codes numbered from ``CUSTOM_CONCEPT_ID_START``, keeping
them distinguishable from OMOP concept IDs in the exported data.

.. _Medical Event Data Standard: https://github.com/Medical-Event-Data-Standard/meds
"""
