# analytics

Clinical-domain logic, one subpackage per domain. Where `core` and `episodes` are
deliberately domain-agnostic, the subpackages here encode what a value *means* in a
particular clinical context: which concepts constitute a treatment, what counts as a
significant change, how severity is graded. Each domain owns its own concept sets, kept
beside the code that uses them, and domains may depend on each other where the clinical
logic genuinely composes.

## oncology

Cancer treatment and disease episodes. `OncologyEpisode` is the main entry point — it
classifies itself from its episode concepts and exposes the facts it contains:

```python
from omop_alchemy.toolkit.analytics.oncology import OncologyEpisode

episode = session.get(OncologyEpisode, episode_id)
episode.structural_modality   # OncologyModality.SACT, .RADIOTHERAPY, .SURGERY, ...
episode.is_treatment_cycle
episode.rt_dose_summaries_by_site
episode.sact_dose_summary
```

Episode traversal resolves to oncology-aware fact classes — `OncologyDrugExposure` and
`OncologyProcedure` extend their CDM counterparts with questions such as `is_sact` and
`is_radiotherapy` — and `OncologyEpisodeEvent` carries resolution diagnostics the same
way `episodes.handling`'s `ResolvedEpisodeEvent` does, since it extends it.

::: omop_alchemy.toolkit.analytics.oncology

## body_metrics

Weight, height, and BMI as measurement series and trajectories. `WeightTrajectoryMixin`
gives an episode view normalised weight and height, BMI, and windowed weight change:

```python
from omop_alchemy.toolkit.analytics.body_metrics import WeightTrajectoryMixin

class MyEpisode(WeightTrajectoryMixin, EpisodeView):
    ...

episode.baseline_bmi
episode.pct_change_over(days=180)
```

`WeightChange.pct_change` is `None` whenever the change is not evaluable — too few
readings, or an unusable unit — so callers cannot mistake an unknown for a zero.

::: omop_alchemy.toolkit.analytics.body_metrics

## adverse_events

Grades clinical severity against published criteria, kept separate from the
measurement code in `body_metrics` so the standard being applied is always explicit.

```python
from omop_alchemy.toolkit.analytics.adverse_events import critical_weight_loss_grade

grade = critical_weight_loss_grade(pct_change=-12.0, bmi=21.4)
```

`critical_weight_loss_grade` uses the Martin et al. BMI-adjusted matrix where BMI is
available and falls back to CTCAE-style percent-loss bins where it is not.

::: omop_alchemy.toolkit.analytics.adverse_events
