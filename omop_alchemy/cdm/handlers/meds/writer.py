from __future__ import annotations

from omop_alchemy.cdm.handlers.meds._guards import *  # noqa: F401,F403

import hashlib
import json
import logging
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import meds
import sqlalchemy as sa
import sqlalchemy.orm as so

from omop_alchemy.cdm.model.clinical import Person
from omop_alchemy.cdm.handlers.meds.code_metadata import build_concept_id_map, build_code_metadata
from omop_alchemy.cdm.handlers.meds.person_meds import Person_MEDS

try:
    from importlib.metadata import version as _pkg_version
    _OA_VERSION = _pkg_version("omop_alchemy")
except Exception:
    _OA_VERSION = "unknown"

logger = logging.getLogger(__name__)

__all__ = ["MEDSWriter", "WriteResult"]


def _shard_for(subject_id: int, num_shards: int) -> int:
    """Return the shard index for subject_id.

    Uses SHA-256 seeded with the meds_etl canonical seed (213345).  Note: this
    uses SHA-256 rather than Polars' xxhash64 since xxhash is not a declared
    dependency; shard layout will differ from a meds_etl-produced dataset but
    each subject's events remain in exactly one shard.
    """
    digest = hashlib.sha256(struct.pack("<q", subject_id ^ 213345)).digest()
    return int.from_bytes(digest[:8], "little") % num_shards


@dataclass
class WriteResult:
    """Summary returned by MEDSWriter.write().

    Attributes:
        subjects_exported: Number of subjects written to the dataset.
        events_emitted:    Total MEDS rows written across all shards.
        shards_written:    Number of non-empty shard files created.
        codes_written:     Number of rows in codes.parquet.
        drop_counts:       Per-table count of events dropped because their
                           concept_id could not be resolved to a code string.
                           Keys are OMOP table names (e.g. "condition_occurrence").
    """
    subjects_exported: int = 0
    events_emitted: int = 0
    shards_written: int = 0
    codes_written: int = 0
    drop_counts: dict[str, int] = field(default_factory=dict)


class MEDSWriter:
    """Export an OMOP CDM database to a MEDS-compliant dataset on disk.

    Produces the following directory layout::

        {output_dir}/
          data/
            data_0.parquet ... data_{num_shards-1}.parquet
          metadata/
            codes.parquet
            dataset.json

    Each data shard is sorted by ``(subject_id, time, code)`` and validated
    against ``meds.DataSchema`` before writing.  All subjects assigned to a
    shard reside in exactly one shard file.

    Args:
        session:                     Active SQLAlchemy session over an OMOP CDM.
        output_dir:                  Destination directory (created if absent).
        num_shards:                  Number of output shard files (default 10).
        include_observation_periods: Emit ``OMOP/OBSERVATION_PERIOD`` events
                                     with interval end datetimes (default True).
        full_code_metadata:          When True, codes.parquet covers every
                                     concept in the vocabulary; when False
                                     (default) only codes that appear in the
                                     exported data are included.
        batch_size:                  Persons fetched per DB round-trip (default
                                     1000).  Reduce for memory-constrained hosts.
        dataset_name:                Optional free-text name written to
                                     metadata/dataset.json.
        dataset_version:             Optional version string for dataset.json.

    Usage::

        writer = MEDSWriter(session, "/path/to/output")
        result = writer.write()
        print(result.events_emitted, result.drop_counts)
    """

    def __init__(
        self,
        session: so.Session,
        output_dir: str | Path,
        *,
        num_shards: int = 10,
        include_observation_periods: bool = True,
        full_code_metadata: bool = False,
        batch_size: int = 1000,
        dataset_name: str | None = None,
        dataset_version: str | None = None,
    ) -> None:
        self._session = session
        self._output_dir = Path(output_dir)
        self.num_shards = num_shards
        self.include_observation_periods = include_observation_periods
        self.full_code_metadata = full_code_metadata
        self.batch_size = batch_size
        self._dataset_name = dataset_name
        self._dataset_version = dataset_version

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def write(
        self,
        subject_ids: Sequence[int] | None = None,
    ) -> WriteResult:
        """Export subjects to disk and return a summary.

        Args:
            subject_ids: Explicit list of person_ids to export.  Pass ``None``
                         (default) to export every person in the database.

        Returns:
            WriteResult with counts of exported subjects, emitted events,
            shard files written, code metadata rows, and per-table drop counts
            for events whose concept_id could not be resolved.

        Example — full export::

            result = MEDSWriter(session, "/data/meds_export").write()
            # WriteResult(subjects_exported=12400, events_emitted=3820000,
            #             shards_written=10, codes_written=8241,
            #             drop_counts={"measurement": 312})

        Example — subset export for a pilot cohort::

            pilot_ids = session.scalars(sa.select(Person.person_id).limit(100)).all()
            result = MEDSWriter(session, "/tmp/pilot", num_shards=2).write(pilot_ids)
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        (self._output_dir / "data").mkdir(exist_ok=True)
        (self._output_dir / "metadata").mkdir(exist_ok=True)

        logger.info("Building concept ID map …")
        code_map, _ = build_concept_id_map(self._session)
        logger.info("Concept map built: %d entries", len(code_map))

        person_ids = self._resolve_person_ids(subject_ids)
        logger.info("Exporting %d subjects in batches of %d", len(person_ids), self.batch_size)

        all_rows, emitted_codes, drop_counts = self._extract(person_ids, code_map)

        result = WriteResult(
            subjects_exported=len(person_ids),
            events_emitted=len(all_rows),
            drop_counts=drop_counts,
        )

        if all_rows:
            result.shards_written = self._write_shards(all_rows)
            logger.info(
                "Wrote %d events to %d shards", result.events_emitted, result.shards_written
            )
        else:
            logger.info("No events to write")

        if drop_counts:
            for table_name, count in sorted(drop_counts.items()):
                logger.warning(
                    "Dropped %d rows from %s: concept_id not resolved", count, table_name
                )

        codes_table = build_code_metadata(
            self._session,
            emitted_codes if not self.full_code_metadata else None,
            mode="all" if self.full_code_metadata else "emitted",
        )
        pq.write_table(
            codes_table,
            self._output_dir / "metadata" / "codes.parquet",
        )
        result.codes_written = codes_table.num_rows
        logger.info("codes.parquet: %d entries", result.codes_written)

        self._write_dataset_json()
        logger.info("dataset.json written")

        return result

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_person_ids(
        self, subject_ids: Sequence[int] | None
    ) -> list[int]:
        if subject_ids is not None:
            return list(subject_ids)
        rows = self._session.execute(
            sa.select(Person.person_id).order_by(Person.person_id)
        ).all()
        return [int(r[0]) for r in rows]

    def _extract(
        self,
        person_ids: list[int],
        code_map: dict[int, str],
    ) -> tuple[list[dict[str, Any]], set[str], dict[str, int]]:
        """Extract MEDS rows from the database with per-table drop counting.

        Returns:
            (all_rows, emitted_codes, drop_counts)
        """
        all_rows: list[dict[str, Any]] = []
        emitted_codes: set[str] = set()
        drop_counts: dict[str, int] = {}

        for offset in range(0, len(person_ids), self.batch_size):
            batch = person_ids[offset : offset + self.batch_size]
            persons = (
                self._session.execute(
                    sa.select(Person_MEDS).where(Person_MEDS.person_id.in_(batch))
                )
                .scalars()
                .all()
            )
            for person in persons:
                # Demographics and observation periods cannot produce unmapped rows.
                demo = person.demographic_rows(code_map)
                all_rows.extend(demo)
                emitted_codes.update(r["code"] for r in demo)

                if self.include_observation_periods:
                    op = person._observation_period_rows()
                    all_rows.extend(op)
                    emitted_codes.update(r["code"] for r in op)

                # Clinical event tables — track drops per source table.
                for EventCls in Person_MEDS.EVENT_TABLES:
                    stmt = sa.select(EventCls).where(
                        EventCls.person_id == person.person_id
                    )
                    for event in self._session.execute(stmt).scalars():
                        rows = event.to_meds_rows(code_map)
                        if rows:
                            all_rows.extend(rows)
                            emitted_codes.update(r["code"] for r in rows)
                        else:
                            tname = EventCls._table_name
                            drop_counts[tname] = drop_counts.get(tname, 0) + 1

        return all_rows, emitted_codes, drop_counts

    def _write_shards(self, all_rows: list[dict[str, Any]]) -> int:
        table = pa.Table.from_pylist(all_rows)

        shard_values = [
            _shard_for(int(sid), self.num_shards)
            for sid in table.column("subject_id").to_pylist()
        ]
        table = table.append_column("_shard", pa.array(shard_values, type=pa.int32()))

        table = table.sort_by([
            ("_shard", "ascending"),
            ("subject_id", "ascending"),
            ("time", "ascending"),
            ("code", "ascending"),
        ])

        # Recompute shard list post-sort for index-based partitioning.
        sorted_shards = table.column("_shard").to_pylist()
        shard_field_idx = table.schema.get_field_index("_shard")
        shards_written = 0

        for shard_idx in range(self.num_shards):
            indices = [i for i, s in enumerate(sorted_shards) if s == shard_idx]
            if not indices:
                continue

            shard_table = table.take(indices).remove_column(shard_field_idx)
            aligned = meds.DataSchema.align(shard_table)
            meds.DataSchema.validate(aligned)

            out_path = self._output_dir / "data" / f"data_{shard_idx}.parquet"
            pq.write_table(aligned, out_path)
            shards_written += 1

        return shards_written

    def _write_dataset_json(self) -> None:
        # "end", "visit_id", and "unit" are emitted when applicable across all
        # clinical event tables; "table" is always present.
        extension_cols = ["table", "end", "visit_id", "unit"]

        md = meds.DatasetMetadataSchema(  # type: ignore[call-arg]
            dataset_name=self._dataset_name,
            dataset_version=self._dataset_version,
            etl_name="omop_alchemy",
            etl_version=_OA_VERSION,
            meds_version=meds.__version__,
            created_at=datetime.now(timezone.utc).isoformat(),
            other_extension_columns=extension_cols,
        )
        as_dict = md.to_dict()
        meds.DatasetMetadataSchema.validate(as_dict)
        out_path = self._output_dir / "metadata" / "dataset.json"
        out_path.write_text(json.dumps(as_dict), encoding="utf-8")
