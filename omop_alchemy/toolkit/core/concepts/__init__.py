"""Resolve free-text terms and source codes to OMOP concept IDs.

Source data rarely arrives with concept IDs attached.  This module turns a
declarative description of *which* concepts are eligible into a runtime
resolver that maps incoming text to those concepts, applying the same
normalisation on both sides so that matching is predictable.

Three pieces make up the workflow:

``LookupSpec``
    Declares which concepts belong in a lookup — by vocabulary, domain,
    concept class, or explicit ancestry — and which text fields are
    indexed.

``LookupIndex``
    The materialised table of normalised text keys to concept IDs that a
    spec produces against the vocabulary tables.

``ConceptResolver``
    Wraps an index and resolves terms at runtime, applying the same
    normalisation used to build the keys.

``make_concept_resolver`` bundles all three for the common case::

    from omop_alchemy.toolkit.core.concepts import make_concept_resolver

    resolver = make_concept_resolver(session, spec)
    concept_id = resolver.resolve("Adenocarcinoma of lung")

Normalisation is composable.  ``compose_normalizers`` chains individual
rules — ``normalize_default`` for whitespace and casing, ``strip_uicc``
and ``make_stage`` for staging text, ``site_to_NOS`` for site
generalisation — so that a resolver's matching behaviour is stated
explicitly rather than implied.

Building an index queries the vocabulary tables, so resolvers are worth
reusing.  ``ConceptResolverRegistry`` constructs each resolver on first
access and caches it for the registry's lifetime.
"""

from .lookup import (
    ConceptResolver,
    LookupIndex,
    LookupSpec,
    OMOPConceptSource,
    make_concept_resolver,
)
from .normalizers import (
    compose_normalizers,
    make_stage,
    normalize_default,
    site_to_NOS,
    strip_uicc,
)
from .registry import ConceptResolverRegistry

__all__ = [
    "ConceptResolver",
    "ConceptResolverRegistry",
    "LookupIndex",
    "LookupSpec",
    "OMOPConceptSource",
    "compose_normalizers",
    "make_concept_resolver",
    "make_stage",
    "normalize_default",
    "site_to_NOS",
    "strip_uicc",
]
