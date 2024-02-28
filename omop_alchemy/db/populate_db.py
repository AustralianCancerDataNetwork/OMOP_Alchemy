import csv
from pathlib import Path
from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.orm as so
import sqlalchemy.sql.sqltypes as sss

from oa_configurator import oa_config, logger
from ..model.clinical import Person, Condition_Occurrence, Measurement, Observation
from ..model.health_system import Care_Site, Location, Provider
from ..model.vocabulary import Concept, Vocabulary, Concept_Class, Domain, Relationship, \
                                Concept_Relationship, Concept_Ancestor

# TODO: insert some validation and checks to make sure folk know how and why to do this (and the limits for demo purposes)

to_load_vocabulary = {'folder': 'ohdsi_vocabs',
                      'VOCABULARY.csv': Vocabulary, 
                      'CONCEPT.csv': Concept, 
                      'CONCEPT_CLASS.csv': Concept_Class, 
                      'DOMAIN.csv': Domain,
                      'RELATIONSHIP.csv': Relationship,
                      'CONCEPT_RELATIONSHIP.csv': Concept_Relationship,
                      'CONCEPT_ANCESTOR.csv': Concept_Ancestor}

to_load_vocab_skip_relationships = {'folder': 'ohdsi_vocabs',
                                    'VOCABULARY.csv': Vocabulary, 
                                    'CONCEPT.csv': Concept, 
                                    'CONCEPT_CLASS.csv': Concept_Class, 
                                    'DOMAIN.csv': Domain,
                                    'RELATIONSHIP.csv': Relationship}

to_load_just_concept_relationships = {'CONCEPT_RELATIONSHIP.csv': Concept_Relationship,
                                      'CONCEPT_ANCESTOR.csv': Concept_Ancestor}

to_load_health_system = {'folder': 'demo_data',
                         'CARE_SITE.csv': Care_Site,
                         'LOCATION.csv': Location,
                         'PROVIDER.csv': Provider}

to_load_clinical = {'folder': 'demo_data',
                    'PERSON.csv': Person,
                    'CONDITION_OCCURRENCE.csv': Condition_Occurrence,
                    'MEASUREMENT.csv': Measurement,
                    'OBSERVATION.csv': Observation,
                    'CONCEPT.csv': Concept}

# flexible loading of ohdsi vocab files downloaded to the path /data/ohdsi_vocabs

def datetime_conversion(dt, fmt):
    if dt != '':
        return datetime.strptime(dt, fmt)
    
def convert_date_col(dt):
    return datetime_conversion(dt, '%Y%m%d')
    
def convert_time_col(dt):
    return datetime_conversion(dt, '%H%M%S')

def convert_datetime_col(dt):
    return datetime_conversion(dt, '%Y%m%d%H%M%S')

def callable_pass(s):
    return s

def convert_int(i):
    try:
        return int(i)
    except:
        return 0
    
def convert_dec(i):
    try:
        return sss.Decimal(i)
    except:
        return 0

type_map = {sss.BigInteger: convert_int, 
            sss.Integer: convert_int, 
            sss.Numeric: convert_dec, 
            sss.DateTime: convert_datetime_col, 
            sss.Time: convert_time_col, 
            sss.String: callable_pass, 
            sss.Date: convert_date_col}

def get_type_lookup(interface):
    return {c.key: type_map[type(c.type)] for c in interface.__table__._columns}


def data_load_prep():
    # check if database is not sqlite and try turn off foreign keys if possible
    ...

def after_data_load():
    # turn back on foreign keys if they've been turned off
    ...

def populate_db_from_file(filepath, interface, session):
    logger.debug(filepath)
    try: 
        with open(filepath, 'r') as file:
            reader = csv.DictReader(file, delimiter='\t')
            field_map = get_type_lookup(interface)
            
            for row in reader:
                record = {field:field_map[field](data) for field, data in row.items() if field in field_map}
                o = interface(**record)
                session.add(o)

            logger.debug(f'complete load for: {filepath}')
    except Exception as e:
        logger.error(e)
        logger.debug(f'Error loading data file {filepath}. Have you unzipped it in the correct location ({oa_config.data_path})?')


def populate_db_from_dict(to_load):
    with so.Session(oa_config.engine) as sess:
        folder = Path(oa_config.data_path) / to_load['folder']
        logger.debug(folder)
        for ohdsi_file, interface in to_load.items():
            if interface != folder.name:
                populate_db_from_file(folder / ohdsi_file, interface, sess)
        sess.commit()
