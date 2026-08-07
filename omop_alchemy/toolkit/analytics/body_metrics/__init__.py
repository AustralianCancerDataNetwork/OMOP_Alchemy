"""Weight, height, and BMI as measurement series and trajectories.

Body measurements are recorded repeatedly, in inconsistent units, and
attributed to a patient rather than to any particular episode.  Turning
them into something a clinical calculation can use means resolving which
readings apply, normalising them, and computing change over time.

**Resolving readings.**  ``resolve_measurement_series`` collects the
measurements attributable to an episode, honouring explicit
``Episode_Event`` links first and then admitting same-person readings
within the episode window.  Height behaves differently: an adult height
recorded outside the window is still valid, so
``resolve_person_measurement_series`` collects all same-person readings
for concepts that are effectively invariant.

Each resolved row becomes a ``MeasurementReading`` — value, date, unit,
and the ``ReadingSource`` recording how it was admitted.

**Normalising.**  ``normalize_weight_readings`` and
``normalize_height_readings`` convert a series to kilograms and
centimetres, dropping readings whose units cannot be interpreted rather
than assuming a default.

**Trajectories.**  ``WeightTrajectoryMixin`` gives an episode view
normalised weight and height, BMI, and windowed weight change::

    class MyEpisode(WeightTrajectoryMixin, EpisodeView):
        ...

    episode.bmi
    episode.weight_change(days=180)

``WeightChange`` reports ``pct_change`` as ``None`` whenever the change is
not evaluable — too few readings, or an unusable unit — so callers cannot
mistake an unknown for a zero.

The concepts defining which measurements count as body size, and their
expected units, are declared in this package and overridable per
deployment via ``BodyMetricRules``.

Severity grading of a weight change is a separate concern and lives in
``omop_alchemy.toolkit.analytics.adverse_events``.
"""

from .calculators import BodyMetricRules, default_body_metric_rules
from .concept_sets import (
    BodySizeMeasurementConcepts,
    default_body_size_measurement_concepts,
)
from .measurement_series import (
    MeasurementReading,
    MeasurementSeriesMixin,
    ReadingSource,
    reading_from_measurement,
    resolve_measurement_series,
    resolve_person_measurement_series,
)
from .weight_trajectory import (
    WeightChange,
    WeightTrajectoryMixin,
    WeightTrajectoryPoint,
    normalize_height_readings,
    normalize_weight_readings,
)

__all__ = [
    "BodyMetricRules",
    "BodySizeMeasurementConcepts",
    "MeasurementReading",
    "MeasurementSeriesMixin",
    "ReadingSource",
    "WeightChange",
    "WeightTrajectoryMixin",
    "WeightTrajectoryPoint",
    "default_body_metric_rules",
    "default_body_size_measurement_concepts",
    "normalize_height_readings",
    "normalize_weight_readings",
    "reading_from_measurement",
    "resolve_measurement_series",
    "resolve_person_measurement_series",
]
