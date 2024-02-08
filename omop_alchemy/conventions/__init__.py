from .concept_enumerators import Modality, EpisodeConcepts, ModifierFields, ConditionModifiers
from .vocab_lookups import VocabLookup

tnm_lookup = VocabLookup(parent=ConditionModifiers.tnm)
grading_lookup = VocabLookup(domain="Measurement", concept_class="Staging/Grading", code_filter='grade')
mets_lookup = VocabLookup(parent=ConditionModifiers.mets)


__all__ = [Modality, EpisodeConcepts, ModifierFields, VocabLookup, ConditionModifiers,
           tnm_lookup, grading_lookup, mets_lookup]