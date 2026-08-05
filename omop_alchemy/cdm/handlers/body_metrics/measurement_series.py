from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from functools import cached_property
from typing import ClassVar, Literal, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import object_session

from omop_alchemy.cdm.model import Measurement

DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90
DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365

ReadingSource = Literal["explicit", "window", "person"]


@dataclass(frozen=True)
class MeasurementReading:
    """A resolved numeric measurement reduced to the fields trajectory math needs."""

    measurement_id: int
    date: date
    value: Optional[float]
    unit_concept_id: Optional[int]
    concept_id: int
    source: ReadingSource


def episode_attachment_window(
    episode,
    *,
    days_prior: int = DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
    open_end_fallback_days: int = DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS,
) -> tuple[date, date]:
    """
    Date window used for episode-attributable body measurements.

    Weight and similar time-varying measurements use a bounded episode-relative
    window. Open episodes fall back to a finite post-start window so accidental
    long-running episodes do not absorb a person's entire future history.
    """
    start = episode.episode_start_date
    end = episode.episode_end_date

    window_start = start - timedelta(days=days_prior)
    open_end_bound = start + timedelta(days=open_end_fallback_days)
    window_end = min(end, open_end_bound) if end is not None else open_end_bound

    return window_start, window_end


def reading_from_measurement(
    measurement: Measurement,
    *,
    source: ReadingSource,
) -> MeasurementReading:
    return MeasurementReading(
        measurement_id=measurement.measurement_id,
        date=measurement.measurement_date,
        value=measurement.value_as_number,
        unit_concept_id=measurement.unit_concept_id,
        concept_id=measurement.measurement_concept_id,
        source=source,
    )


def _explicit_episode_measurements(
    episode,
    concept_id_set: set[int],
    *,
    exclude_modifiers: bool,
) -> tuple[list[MeasurementReading], set[int]]:
    seen_ids: set[int] = set()
    readings: list[MeasurementReading] = []

    for event in episode.events:
        if not isinstance(event, Measurement):
            continue
        if event.measurement_concept_id not in concept_id_set:
            continue
        if exclude_modifiers and event.measurement_event_id is not None:
            continue
        readings.append(reading_from_measurement(event, source="explicit"))
        seen_ids.add(event.measurement_id)

    return readings, seen_ids


def resolve_measurement_series(
    episode,
    concept_ids: Sequence[int],
    *,
    exclude_modifiers: bool = True,
) -> list[MeasurementReading]:
    """
    Resolve episode-attributable measurements for the supplied concepts.

    The resolver first honours explicit ``Episode_Event`` links, then adds
    same-person measurements in the episode attachment window that were not
    already seen. Detached episodes simply return explicit links.
    """
    concept_id_set = set(concept_ids)
    readings, seen_ids = _explicit_episode_measurements(
        episode,
        concept_id_set,
        exclude_modifiers=exclude_modifiers,
    )

    session = object_session(episode)
    if session is not None:
        window_start, window_end = episode_attachment_window(episode)
        stmt = select(Measurement).where(
            Measurement.person_id == episode.person_id,
            Measurement.measurement_concept_id.in_(concept_id_set),
            Measurement.measurement_date.between(window_start, window_end),
        )
        if exclude_modifiers:
            stmt = stmt.where(Measurement.measurement_event_id.is_(None))

        for row in session.execute(stmt).scalars():
            if row.measurement_id in seen_ids:
                continue
            readings.append(reading_from_measurement(row, source="window"))
            seen_ids.add(row.measurement_id)

    readings.sort(key=lambda r: r.date)
    return readings


def resolve_person_measurement_series(
    episode,
    concept_ids: Sequence[int],
    *,
    exclude_modifiers: bool = True,
) -> list[MeasurementReading]:
    """
    Resolve all same-person measurements for effectively invariant concepts.

    This is appropriate for adult height, where an out-of-window reading can
    still validly support BMI/BSA for an episode. Do not use it for weight.
    """
    concept_id_set = set(concept_ids)
    readings, seen_ids = _explicit_episode_measurements(
        episode,
        concept_id_set,
        exclude_modifiers=exclude_modifiers,
    )

    session = object_session(episode)
    if session is not None:
        stmt = select(Measurement).where(
            Measurement.person_id == episode.person_id,
            Measurement.measurement_concept_id.in_(concept_id_set),
        )
        if exclude_modifiers:
            stmt = stmt.where(Measurement.measurement_event_id.is_(None))

        for row in session.execute(stmt).scalars():
            if row.measurement_id in seen_ids:
                continue
            readings.append(reading_from_measurement(row, source="person"))
            seen_ids.add(row.measurement_id)

    readings.sort(key=lambda r: r.date)
    return readings


class MeasurementSeriesMixin:
    """
    Mixin for episode views that need one cached measurement concept series.
    """

    _series_concept_ids: ClassVar[Sequence[int]]
    _exclude_modifier_measurements: ClassVar[bool] = True

    @cached_property
    def _series(self) -> list[MeasurementReading]:
        return resolve_measurement_series(
            self,
            self._series_concept_ids,
            exclude_modifiers=self._exclude_modifier_measurements,
        )
