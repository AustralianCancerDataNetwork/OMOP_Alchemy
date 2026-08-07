"""Cancer treatment and disease episodes, and the dosing they carry.

Oncology care is structured: a disease episode spans a cancer course, a
treatment regimen sits beneath it, and cycles sit beneath the regimen.
This package provides that structure as navigable objects and answers the
questions that follow from it.

``OncologyEpisode`` is the main entry point.  It classifies itself from
its episode concepts — whether it is a treatment cycle, and which
``OncologyModality`` it represents (systemic therapy, radiotherapy,
surgery, or diagnostic staging) — and exposes the facts it contains::

    from omop_alchemy.toolkit.analytics.oncology import OncologyEpisode

    episode = session.get(OncologyEpisode, episode_id)
    episode.structural_modality
    episode.is_treatment_cycle

Episode traversal resolves to oncology-aware fact classes.
``OncologyDrugExposure`` and ``OncologyProcedure`` extend their CDM
counterparts with domain questions such as whether a row represents
systemic therapy or radiotherapy, so traversal from an episode yields
objects that can answer them.  Non-oncology targets resolve to the base
CDM classes as usual.

**Dosing.**  Radiotherapy and systemic therapy are dosed in
fundamentally different terms and are summarised separately.  RT
summaries group by treatment site; systemic therapy summaries group by
drug or ingredient.  Both report whether the dose is evaluable, because
source units and quantities are frequently not comparable across agents
or regimens — a summary that cannot be interpreted as a dose says so
rather than presenting a total that looks authoritative.

**Concept sets.**  Which concepts constitute systemic therapy,
radiotherapy, cancer-indicating surgery, diagnostic staging, and each
episode type are declared in this package and resolved against the
vocabulary tables on first use, then cached.  ``clear_concept_set_cache``
discards that cache when the vocabulary changes underneath a long-running
session.

Governed default concept sets come from ``omop-semantics``, an optional
dependency.  Install ``omop-alchemy[semantics]`` to use them; supply your
own concept sets otherwise.
"""
