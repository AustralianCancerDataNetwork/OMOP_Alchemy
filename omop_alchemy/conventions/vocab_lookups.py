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
from .concept_enumerators import ConceptEnum, TStageConcepts, NStageConcepts, MStageConcepts, GroupStageConcepts, Unknown, CancerProcedureTypes


def flatten_lookup(l):
    concepts = []
    for k, v in l.items():
        if isinstance(v, int):
            concepts.append(v)
        elif type(v) == defaultdict:
            concepts.extend(flatten_lookup(v))
    return concepts

class MappingLookup:
    """ 
    simple version of the vocab lookup, which relies on knowing which custom map you are 
    looking for, and its context and providing very fast lookup functionality if so, as it 
    attempts no correction and no iteration through options and instead exposes the lookup 
    dict directly

    if no context relevant to that col, just uses single 'all' context

    To create a new mapping lookup object, define the following:

    - Appropriate value for unknown in this context - if not specified, generic unknown value is selected
    - Custom mappings in CTL.custom_vocabulary_maps - these should be specified in cava_app/vocabs/custom_vocabularies/custom_vocabulary_maps.csv and loaded via the SetupCustomMaps task

    """

    def return_unknown(self):
        """
        Set unknown value that will be used in this specific context at instantiation time
        """
        if self._unknown:
            return self._unknown.value
    
    def lookup(self, val, col=None, context='all', null_override=False):
        """
        Returns mapped value - if not found returns relevant unknown value
        """
        if val:
            value = self._lookup[col or self._column][context][val.strip().lower()]
            for c in self._correction:
                if value != self._unknown.value:
                    break
                value = self._lookup[col or self._column][context][c(val).strip().lower()]
            if null_override and value == self._unknown.value:
                return
            return value
        else: 
            if null_override:
                return
            return self.return_unknown()

    def __init__(self, table, column, control_schema_object, engine, unknown=Unknown.generic, corrections=[]):
        self._unknown = unknown
        self._lookup = defaultdict(lambda: defaultdict(lambda: defaultdict(self.return_unknown)))
        self._correction = corrections
        if type(column) == str:
            self._column=column
            column=[column]
        else:
            self._column = None
        column_filter = [control_schema_object.column.ilike(c) for c in column]
        with so.Session(engine) as ctl_sess:
            concepts = ctl_sess.query(control_schema_object
                                        ).filter(control_schema_object.table.ilike(table)
                                        ).filter(sa.or_(*column_filter))
        for c in concepts:
            self._lookup[c.column][c.context or 'all'][c.value.strip().lower()] = c.concept_id


    @property
    def all_concepts(self):
        return flatten_lookup(self._lookup)


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
                 vocabulary_id=None,  # and optionally vocabulary
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
        self._vocabulary_id = vocabulary_id
        self._standard_only = standard_only
        self._code_filter = code_filter
        # parent parameter is the high-level concept under which you want to pull
        # in all available matches - e.g. TNM stages, which can grab all concepts 
        # that fall under the parent concept from the concept_relationship table
        if not isinstance(parent, list):
            parent = [parent] if parent is not None else []
        self._parent = [p.value if isinstance(p, ConceptEnum) else p for p in parent]
        self._correction = correction
        with so.Session(oa_config.engine) as session:
            # TBD: question - do we need to provide support for combining parent 
            # definition with domain def? is this a likely use-case? it won't fail 
            # for now, but perhaps check?
            if len(parent) > 0:
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
            class_filter = sa.or_(*[Concept.concept_class_id==c for c in self._concept_class])
            q = q.filter(class_filter)
        if self._vocabulary_id:
            vocab_filter = sa.or_(*[Concept.vocabulary_id==v for v in self._vocabulary_id])
            q = q.filter(vocab_filter)
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
                                ).filter(Concept_Ancestor.ancestor_concept_id.in_(self._parent)
                                ).distinct().all()
        return [c.descendant for c in children if c.descendant is not None]
        
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
            concepts = self.get_all_hierarchy(tuple(self._parent), [], session)
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

    @property
    def all_concepts(self):
        return list(set(self._lookup.values()))

from .concept_enumerators import ConditionModifiers

def remove_brackets(val):
    return val.split('(')[0]

def make_stage(val):
    val = val.lower()
    roman_lookup = [('-iii', '-3'), ('-iv', '-4'), ('-ii', '-2'), ('-i', '-1'), ('nos', '')]
    for replacement in roman_lookup:
        val = val.replace(*replacement)
    return val

def site_to_NOS(icdo_topog):
    split_topog = icdo_topog.split('.')
    if '.' not in icdo_topog:
        return f'{icdo_topog}.9'
    # a couple of codes have a third decimal point?
    elif len(split_topog[-1]) > 2:
        return ''.join(split_topog[:-1] + ['.', split_topog[-1][:2]])
    return icdo_topog

def strip_uicc(code):
    return code.lower().replace('ajcc', 'ajcc/uicc')

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
    

    # def get_standard_hierarchy(self, session):
    #     children = session.query(Concept_Ancestor
    #                             ).options(so.joinedload(Concept_Ancestor.descendant)
    #                             ).filter(Concept_Ancestor.ancestor_concept_id.in_(self._parent)
    #                             ).distinct().all()
    #     return [c.descendant for c in children if c.descendant is not None]

    # def get_standard_hierarchy(self, session):
    #     children = session.query(Concept_Ancestor
    #                             ).options(so.joinedload(Concept_Ancestor.descendant)
    #                             ).filter(Concept_Ancestor.ancestor_concept_id == self._parent).distinct().all()
    #     return [c.descendant for c in children]
        
tnm_lookup = StagingLookup()
grading_lookup = VocabLookup(domain="Measurement", concept_class=["Staging/Grading"], code_filter='grade')
mets_lookup = VocabLookup(parent=ConditionModifiers.mets)
stage_edition_lookup = VocabLookup(parent=ConditionModifiers.tnm, correction=[strip_uicc])
gender_lookup = VocabLookup(domain="Gender")
unit_lookup = VocabLookup(domain="Unit")
race_lookup = VocabLookup(domain="Race")
ethnicity_lookup = VocabLookup(domain="Ethnicity")
icdo_condition_lookup = VocabLookup(domain='Condition', concept_class=['ICDO Condition'], standard_only=False, correction=[site_to_NOS])
icd10_condition_lookup = VocabLookup(domain='Condition', concept_class=['ICD10 Hierarchy', 'ICD10 code'], standard_only=False, correction=[site_to_NOS])
relaxed_condition_lookup = VocabLookup(domain='Condition', vocabulary_id=['ICD10', 'ICD10CM', 'ICD9CM'], standard_only=False)
radiotherapy_procedures = VocabLookup(parent=CancerProcedureTypes.rt_procedure)

class CustomLookups():

    # todo: refactor the control schema so that it can be merged with OMOP_Alchemy and not have to pass around the engines and objects in such a silly way

    def __init__(self, engine, control_schema_object):
        
        self._engine = engine

        self.lookup_laterality = MappingLookup('medical', 'paired_organ', control_schema_object, self._engine)
        self.lookup_condition = MappingLookup('medical', 'combined_condition', control_schema_object, self._engine, corrections=[site_to_NOS])
        self.lookup_stage = MappingLookup('tnmstage', ['t_stage', 'n_stage', 'm_stage', 'stage'], control_schema_object, self._engine)
        self.lookup_grade = MappingLookup('medical', 'hist_grade', control_schema_object, self._engine)
        self.lookup_drugs = MappingLookup('drug', 'drug', control_schema_object, self._engine)
        self.lookup_units = MappingLookup('drug', 'unit', control_schema_object, self._engine)
        self.lookup_route = MappingLookup('drug', 'route', control_schema_object, self._engine)
        self.lookup_mets = MappingLookup('medical', 'dist_mets', control_schema_object, self._engine)
        self.lookup_eviq = MappingLookup('eviq', ['component', 'regimen'], control_schema_object, self._engine, unknown=Unknown.therapeutic_regimen)
        self.lookup_cob = MappingLookup('admin', 'birth_place', control_schema_object, self._engine, unknown=Unknown.cob)
        self.lookup_lang = MappingLookup('prompt', 'text', control_schema_object, self._engine)
        self.lookup_stafftype = MappingLookup('staff', ['provider_type'], control_schema_object, self._engine,  unknown=None)
        self.lookup_radiotherapy = MappingLookup('radiotherapy', ['rt_procedure', 'rt_site', 'rt_parameter'], control_schema_object, self._engine, unknown=None)
        self.lookup_surg = MappingLookup('patcplan', 'cpact_name', control_schema_object, self._engine, unknown=None)
        self.intent = MappingLookup('treatment', 'intent', control_schema_object, self._engine, unknown=None)