# this module holds all global namespace vocabulary lookups as equivalent to singleton instances
from collections import defaultdict
import sqlalchemy as sa
import sqlalchemy.orm as so
import sqlalchemy.sql.sqltypes as sss
import sqlalchemy.sql.expression as exp
import re

from oa_configurator import oa_config

from ..db import Base
from ..model.vocabulary import Concept, Concept_Relationship, Concept_Ancestor
from .concept_enumerators import ConceptEnum, TStageConcepts, NStageConcepts, MStageConcepts, GroupStageConcepts

class VocabLookup:
    # base class for custom vocabulary lookups

    # correction parameter holds an ordered list of callable corrections 
    # - try match the raw input string first 
    # - then apply corrections in order and return the first match 
    # - examples of correction functions would be stripping punctuation, 
    #   spelling correction functions

    def __init__(self, 
                 unknown=0,           # TODO: tbd do we want to define behaviours when mapping is not found?
                 parent=None,         # used when you want to pull all child concepts under a given parent into the lookup
                 domain=None,         # otherwise we are grabbing by specification of domain
                 concept_class=None,  # and optionally concept_class
                 code_filter=None,    # last option - apply a string filter (this is required for grade as distinct from stage)
                 correction=[],       # correction parameter holds an ordered list of callable corrections 
                                      # - try match the raw input string first 
                                      # - then apply corrections in order and return the first match 
                                      # - examples of correction functions would be stripping punctuation, 
                                      #   spelling correction functions
                 standard_only=True): # for when you want to toggle between grabbing children from standard concepts strictly or not
        self._unknown = unknown.value if isinstance(unknown, ConceptEnum) else unknown
        self._lookup = defaultdict(self.return_unknown)
        self._domain = domain
        self._concept_class = concept_class
        self._standard_only = standard_only
        self._code_filter = code_filter
        # parent parameter is the high-level concept under which you want to pull
        # in all available matches - e.g. TNM stages, which can grab all concepts 
        # that fall under the parent concept from the concept_relationship table
        self._parent = parent.value if isinstance(parent, ConceptEnum) else parent
        self._correction = correction
        with so.Session(oa_config.engine) as session:
            # TBD: question - do we need to provide support for combining parent 
            # definition with domain def? is this a likely use-case? it won't fail 
            # for now, but perhaps check?
            if parent is not None:
                self.get_lookup(session)
            if domain is not None:
                self.get_domain_lookup(session)
        
        # TODO: consider generalisable creation of custom maps to host 
        # manual mappings of local concepts to OMOP concepts as well?

    
    def get_domain_lookup(self, session):
        # returns a default dictionary that contains all
        # concepts under a given domain for rapid lookups
        
        q = session.query(Concept)
        if self._domain:
            q = q.filter(Concept.domain_id==self._domain)
        if self._concept_class:
            q = q.filter(Concept.concept_class_id==self._concept_class)
        if self._standard_only:
            q = q.filter(Concept.standard_concept=='S')
        if self._code_filter:
            q = q.filter(Concept.concept_code.ilike(f'%{self._code_filter}%'))
        concepts = q.all()
        for row in concepts:
            self._lookup[row.concept_name.lower().strip()] = row.concept_id
            self._lookup[row.concept_code.lower().strip()] = row.concept_id
    
    def get_standard_hierarchy(self, session):
        children = session.query(Concept_Ancestor
                                ).options(so.joinedload(Concept_Ancestor.descendant)
                                ).filter(Concept_Ancestor.ancestor_concept_id == self._parent).distinct().all()
        return [c.descendant for c in children]
        
    def get_all_hierarchy(self, this_level, concepts, session):
        # TODO: check if we want to do this thru Concept_Ancestor strictly
        # if confirmed we only want to be doing for standard concepts?
        # this is iterative and slow way of doing it to arbitrary depths
        # otherwise...but good if you want to include non-standard 
        # children - maybe useful in condition_concept_id?

        if len(this_level) == 0:
            return concepts
        children = session.query(Concept
                                ).join(Concept_Relationship, Concept_Relationship.concept_id_2==Concept.concept_id
                                ).filter(Concept_Relationship.concept_id_1.in_(this_level)
                                ).filter(Concept_Relationship.relationship_id=='Subsumes').distinct().all()
        next_level = tuple([c.concept_id for c in children if c not in concepts])
        concepts += children
        concepts = self.get_all_hierarchy(next_level, concepts, session)
        return concepts

    def get_lookup(self, session):
        # returns a default dictionary that contains all
        # concepts under a given parent concept and the
        # appropriate unknown value for the target context
        if not self._standard_only:
            concepts = self.get_all_hierarchy(tuple([self._parent]), [], session)
        else:
            concepts = self.get_standard_hierarchy(session)
        for c in concepts:
            self._lookup[c.concept_name.lower()] = c.concept_id
            self._lookup[c.concept_code.lower()] = c.concept_id

    def return_unknown(self):
        return self._unknown

    def lookup_exact(self, term):
        if term == None:
            term = ''
        return self._lookup[term.lower().strip()]

    def lookup(self, term):
        if term == None:
            term = ''
        value = self._lookup[term.lower().strip()]
        if self._correction is not None:
            for c in self._correction:
                if value != self._unknown:
                    break
                value = self._lookup[c(term).lower().strip()]
        return value
    
    def __contains__(self, item):
        if isinstance(item, int):
            return item in self._lookup.values()
        if isinstance(item, str):
            return item in self._lookup.keys() and self.lookup(item) != self.return_unknown()

from .concept_enumerators import ConditionModifiers


def remove_brackets(val):
    return val.split('(')[0]

def make_stage(val):
    val = val.lower()
    roman_lookup = [('-iii', '-3'), ('-iv', '-4'), ('-ii', '-2'), ('-i', '-1'), ('nos', '')]
    for replacement in roman_lookup:
        val = val.replace(*replacement)
    return val

class StagingLookup(VocabLookup):

    def get_children(self, session, concepts):
        children = session.query(Concept_Ancestor
                                ).options(so.joinedload(Concept_Ancestor.descendant)
                                ).filter(Concept_Ancestor.ancestor_concept_id.in_(concepts)).distinct().all()
        return [c.descendant.concept_id for c in children]

    def __init__(self):
        super().__init__(parent=ConditionModifiers.tnm, correction=[remove_brackets, make_stage])
        self.clinical_stage_concepts = [v for k, v in self._lookup.items() if k[0] == 'c' and v != 0]
        self.path_stage_concepts = [v for k, v in self._lookup.items() if k[0] == 'p' and v != 0]

        with so.Session(oa_config.engine) as session:
            self.t_stage_concepts = self.get_children(session, TStageConcepts.member_values())
            self.n_stage_concepts = self.get_children(session, NStageConcepts.member_values())
            self.m_stage_concepts = self.get_children(session, MStageConcepts.member_values())
            self.group_stage_concepts = self.get_children(session, GroupStageConcepts.member_values())
    
    def get_standard_hierarchy(self, session):
        children = session.query(Concept_Ancestor
                                ).options(so.joinedload(Concept_Ancestor.descendant)
                                ).filter(Concept_Ancestor.ancestor_concept_id == self._parent).distinct().all()
        return [c.descendant for c in children]
        
tnm_lookup = StagingLookup()
grading_lookup = VocabLookup(domain="Measurement", concept_class="Staging/Grading", code_filter='grade')
mets_lookup = VocabLookup(parent=ConditionModifiers.mets)
gender_lookup = VocabLookup(domain="Gender")
race_lookup = VocabLookup(domain="Race")
ethnicity_lookup = VocabLookup(domain="Ethnicity")

# class HierarchicalLookup():
#     # this class holds an ordered list of standard vocabularies and 
#     # will try match them in order of priority, restricted
#     # to the target domain

#     def __init__(self, domain, vocab_list, unknown=Unknown.generic.value, corrections=None):
#         self._unknown = unknown
#         self._lookup_list = [StandardVocabLookup(v, domain, unknown, corrections) for v in vocab_list]

#     def lookup(self, term):
#         value = self._unknown
#         for l in self._lookup_list:
#             if value != self._unknown:
#                 break
#             value = l.lookup_exact(term)
#         for l in self._lookup_list:
#             if value != self._unknown:
#                 break
#             value = l.lookup(term)
#         return value

# def remove_slash(term):
#     return term.replace('/', '')

# def insert_slash(term):
#     try:
#         return f'{term[:-1]}/{term[-1]}'
#     except:
#         return ''

# def regexp_find_icd(term):
#     # match full ICD10 code of form C00.00
#     return re.search('[a-zA-Z]\d{1,2}\.\d{1,2}|$', term).group()

# def regexp_icd_group(term):
#     # match higher (less specific) ICD term when full term 
#     # isn't possible e.g. C92
#     return re.search('[a-zA-Z]\d{1,2}|$', term).group()

# class ConditionLookup(VocabLookup):
    
#     # a condition lookup object can be used to map a combination of 
#     # fields into a single target.
#     # it was originally created so that morph and topog could
#     # be combined to lookup condition, using the object 
#     # CTL.condition_lookup, but could be used validly to lookup
#     # any n:1 concept lookup by populating a similar control
#     # schema table.

#     def __init__(self, unknown, object_lookup, source, target, vocabulary):
#         super().__init__(unknown)
#         self._correction = None
#         get_custom_lookup(object_lookup, source, target, vocabulary, self._lookup)
    

# def get_custom_lookup(ObjectLookup, source, target, vocabulary, lookup):
    
#     with db_session(control_engine) as ctl_sess:
#         object_lookup = dataframe_from_query(ctl_sess.query(ObjectLookup)).fillna('0')
#         object_lookup['source']=object_lookup.apply(lambda x: '-'.join(list(x[source])), axis=1)
#         object_lookup['target']=object_lookup.apply(lambda x: '-'.join(list(x[target])), axis=1)

#         concept_filter = tuple(object_lookup.target.dropna().unique())
#         with db_session(target_engine) as targ_sess:
#             concept_lookup = dataframe_from_query(targ_sess.query(CDM.concept)
#                                                   .filter(CDM.concept.concept_code.in_(concept_filter))
#                                                   .filter(CDM.concept.vocabulary_id==vocabulary))
#         object_lookup = object_lookup.merge(concept_lookup, left_on='target', right_on='concept_code')
#         for k, v in zip(object_lookup.source, object_lookup.concept_id):
#             lookup[k.lower()]=v

# class CoordinatedCondition(HierarchicalLookup):
#     def __init__(self):
#         super().__init__(domain='Condition', 
#                          vocab_list=['ICDO3'], 
#                          unknown=Unknown.condition,
#                          corrections=[remove_slash, regexp_find_icd, 
#                                       regexp_icd_group, insert_slash])

# class GenderLookup(VocabLookup):
#     def __init__(self):
#         super().__init__(unknown=Unknown.gender, domain='Gender')
#                          #parent=SNOMED_hierarchy.sex)  

# class LanguageLookup(VocabLookup):
#     def __init__(self):
#         super().__init__(unknown=Unknown.generic, 
#                          parent=SNOMED_hierarchy.language,
#                          table='prompt', 
#                          context='admin.language_spoken_pro_id') 
#         self._correction = [self.append_language]

#     def append_language(self, term):
#         return term.lower().strip() + ' language'

# class RaceLookup(VocabLookup):

#     def __init__(self):
#         super().__init__(unknown=Unknown.generic, 
#                          parent=None, 
#                          table='prompt', 
#                          context='race.pro_id')

# #try:
# cava_log.log('Loading custom concept lookup objects', 'debug')
# lookup_language = LanguageLookup()
# lookup_race = RaceLookup()
# lookup_condition = ConditionLookup(unknown=Unknown.cancer, object_lookup=CTL.condition_lookup, source=['morph', 'topog'], target=['condition'], vocabulary='ICDO3') # Unknown histology of unknown primary site
# #    lookup_observation = ConditionLookup('Observation')
# #    lookup_coordinated = CoordinatedCondition()
# lookup_icd = HierarchicalLookup('Condition', ['ICD10', 'ICD9CM', 'ICDO3'], Unknown.condition, [remove_slash, insert_slash, regexp_find_icd, regexp_icd_group])
# lookup_laterality = MappingLookup('medical', 'paired_organ')
# lookup_stage = MappingLookup('tnmstage', ['t_stage', 'n_stage', 'm_stage', 'stage'])
# lookup_grade = MappingLookup('medical', 'hist_grade')
# lookup_drugs = MappingLookup('drug', 'drug')
# lookup_units = MappingLookup('drug', 'unit')
# lookup_route = MappingLookup('drug', 'route')
# lookup_eviq = MappingLookup('eviq', ['component', 'regimen'])

# cava_log.log('Custom concept lookup loading complete', 'debug')
# # except Exception as e:
# #     cava_log.log('Unable to load custom concept lookup objects', 'error')
# #     cava_log.log(f'{e}', 'error')