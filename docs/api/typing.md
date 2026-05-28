# Typing

OMOP Alchemy exposes a set of **Protocols and typed containers** for code that needs to
interact with CDM classes without coupling to specific ORM implementations.

These live in two modules:

| Module | Contents |
|--------|---------|
| `omop_alchemy.cdm.base.typing` | Runtime-checkable Protocols for structural checking |
| `omop_alchemy.cdm.model.typing` | Typed row containers |

---

## Protocols (`cdm.base.typing`)

### `HasConceptId`

Satisfied by any object with an integer `concept_id` attribute.

::: omop_alchemy.cdm.base.typing.HasConceptId

---

### `HasPersonId`

Satisfied by any object with an integer `person_id` attribute.

::: omop_alchemy.cdm.base.typing.HasPersonId

---

### `HasEpisodeId`

Satisfied by any object with an integer `episode_id` attribute.

::: omop_alchemy.cdm.base.typing.HasEpisodeId

---

### `DomainSemanticTable`

Structural protocol for CDM ORM classes that participate in domain validation. A class
satisfies this protocol if it has `__tablename__`, `__mapper__`, `__expected_domains__`,
and a `collect_domain_rules()` classmethod.

::: omop_alchemy.cdm.base.typing.DomainSemanticTable

---

### `ClinicalEvent` (Protocol)

Minimal protocol for ORM rows that represent a clinical event — a concept, a person, a
start date, and an optional end date. Used as the structural contract for domain-level
utilities that operate across multiple CDM tables.

!!! note
    The concrete mixin of the same name lives in
    `omop_alchemy.cdm.handlers.timeline.event_timeline`. The Protocol here is the
    structural interface; the mixin there is the implementation.

::: omop_alchemy.cdm.base.typing.ClinicalEvent

---

### `ConceptResolver`

Protocol for objects that can look up whether a set of concept IDs are standard.

::: omop_alchemy.cdm.base.typing.ConceptResolver

---

## Typed row containers (`cdm.model.typing`)

### `ConceptRow`

A frozen dataclass representing the core fields of a concept lookup row. Used where a
lightweight, hashable concept record is preferable to a full ORM object.

::: omop_alchemy.cdm.model.typing.ConceptRow
