import csv
from pathlib import Path
from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.orm as so
import sqlalchemy.sql.sqltypes as sss
import pandas as pd

from oa_configurator import oa_config, logger
from ..model.clinical import Person, Condition_Occurrence, Measurement, Observation
from ..model.health_system import Care_Site, Location, Provider
from ..model.vocabulary import Concept, Vocabulary, Concept_Class, Domain, Relationship, \
                                Concept_Relationship, Concept_Ancestor

# TODO: insert some validation and checks to make sure folk know how and why to do this (and the limits for demo purposes)

to_load_vocabulary = {'folder': 'ohdsi_vocabs',
                      'chunkable': False,
                      'files': {'VOCABULARY.csv': Vocabulary, 
                                'CONCEPT.csv': Concept, 
                                'CONCEPT_CLASS.csv': Concept_Class, 
                                'DOMAIN.csv': Domain,
                                'RELATIONSHIP.csv': Relationship,
                                'CONCEPT_RELATIONSHIP.csv': Concept_Relationship,
                                'CONCEPT_ANCESTOR.csv': Concept_Ancestor}}

to_load_vocab_skip_relationships = {'folder': 'ohdsi_vocabs',
                                    'chunkable': False,
                                    'files':{'VOCABULARY.csv': Vocabulary, 
                                             'CONCEPT.csv': Concept, 
                                             'CONCEPT_CLASS.csv': Concept_Class, 
                                             'DOMAIN.csv': Domain,
                                             'RELATIONSHIP.csv': Relationship}}

to_load_just_concept_relationships = {'folder': 'ohdsi_vocabs',
                                     'chunkable': True,
                                     'files':{'CONCEPT_RELATIONSHIP.csv': Concept_Relationship}}

to_load_just_ancestry = {'folder': 'ohdsi_vocabs',
                         'chunkable': True,
                         'files':{'CONCEPT_ANCESTOR.csv': Concept_Ancestor}}
                         

to_load_health_system = {'folder': 'demo_data',
                         'chunkable': False,
                         'files':{'CARE_SITE.csv': Care_Site,
                                  'LOCATION.csv': Location,
                                  'PROVIDER.csv': Provider}}

to_load_clinical = {'folder': 'demo_data',
                    'chunkable': False,
                    'files':{'PERSON.csv': Person,
                             'CONDITION_OCCURRENCE.csv': Condition_Occurrence,
                             'MEASUREMENT.csv': Measurement,
                             'OBSERVATION.csv': Observation,
                             'CONCEPT.csv': Concept}}

# flexible loading of ohdsi vocab files downloaded to the path /data/ohdsi_vocabs

def datetime_conversion(dt, fmt):
    if dt != '':
        return datetime.strptime(dt, fmt)
    
def convert_date_col(dt):
    try:
       return datetime_conversion(dt, '%Y%m%d')
    except:
        return datetime_conversion(dt, '%Y-%m-%d')

def convert_time_col(dt):
    return datetime_conversion(dt, '%H%M%S')

def convert_datetime_col(dt):
    try:
        return datetime_conversion(dt, '%Y%m%d%H%M%S')
    except:
        try:
            return datetime_conversion(dt, '%Y-%m-%d %H:%M:%S')
        except: # workaround for excel annoying behaviour
            return datetime_conversion(dt, '%Y-%m-%d %H:%M:%S.%f')
        
def callable_pass(s):
    return s

def convert_int(i):
    try:
        return int(i)
    except:
        return None
    
def convert_dec(i):
    try:
        return sss.Decimal(i)
    except:
        return None
    
def convert_bool(i):
    return bool(i)

type_map = {sss.BigInteger: convert_int, 
            sss.Integer: convert_int, 
            sss.Numeric: convert_dec, 
            sss.DateTime: convert_datetime_col, 
            sss.Time: convert_time_col, 
            sss.String: callable_pass, 
            sss.Date: convert_date_col,
            sss.Boolean: convert_bool,
            sss.Enum: callable_pass}

def create_enum_lookup(enum_lookup):
    def f(strval):
        return enum_lookup._object_lookup[strval]
    return f

def get_type_lookup(interface):
    return {c.key: type_map[type(c.type)] if type(c.type) != sss.Enum else create_enum_lookup(c.type) for c in interface.__table__._columns}

def data_load_prep():
    # check if database is not sqlite and try turn off foreign keys if possible
    ...

def after_data_load():
    # turn back on foreign keys if they've been turned off
    ...


def populate_chunks_from_file(filepath, interface, sess):
    logger.debug(filepath)
    try: 
        field_map = get_type_lookup(interface)
        base_tables = [b for b in interface.__bases__ if hasattr(b, '__tablename__')]
        if len(base_tables) > 0:
            for b in base_tables:
                field_map.update(get_type_lookup(b))

        for chunk in pd.read_csv(filepath, chunksize=10000, delimiter='\t', low_memory=False):
            rows = chunk.to_dict(orient='records')
            records = map(lambda x: {field:field_map[field](data) for field, data in x.items() if field in field_map}, rows)
            objects = map(lambda x: interface(**x), records)
            sess.add_all(objects)
            sess.commit()
            logger.debug(f'chunk loaded for: {filepath}')
        
        logger.debug(f'complete load for: {filepath}')
    except Exception as e:
        logger.error(e)
        logger.debug(f'Error loading data file {filepath}. Have you unzipped it in the correct location ({oa_config.data_path})?')


def populate_db_from_file(filepath, interface, session):
    logger.debug(filepath)
    try: 
        with open(filepath, 'r') as file:
            reader = csv.DictReader(file, delimiter='\t')
            field_map = get_type_lookup(interface)
            base_tables = [b for b in interface.__bases__ if hasattr(b, '__tablename__')]
            if len(base_tables) > 0:
                for b in base_tables:
                    field_map.update(get_type_lookup(b))

            for row in reader:
                record = {field:field_map[field](data) for field, data in row.items() if field in field_map}
                o = interface(**record)
                session.add(o)

            logger.debug(f'complete load for: {filepath}')
    except Exception as e:
        logger.error(e)
        logger.debug(f'Error loading data file {filepath}. Have you unzipped it in the correct location ({oa_config.data_path})?')


def rapid_load(path, target):
    rapid_load_script = {'before_load': 'SET session_replication_role = replica;', 'load_script': [], 'not_loaded': []}
    for f in path.iterdir(): 
        try:
            if f.name in target['files']:
                with open(f, 'r') as file:
                    reader = csv.DictReader(file, delimiter='\t')
                    for row in reader:
                        header = row
                        break
                rapid_load_script['load_script'].append({'file': f.name, 
                                                         'table': f.stem.lower(), 
                                                         'sep': '\t', 
                                                         'columns': list(header.keys())})
        except Exception as e:
            rapid_load_script['not_loaded'].append({'file': f.name, 
                                                    'error': str(e)[:500]})
    return rapid_load_script

def populate_db_from_dict(to_load):
    with so.Session(oa_config.engine) as sess:
        folder = Path(oa_config.data_path) / to_load['folder']
        logger.debug(folder)
        for ohdsi_file, interface in to_load['files'].items():
            if interface != folder.name and (folder / ohdsi_file).exists():
                if to_load['chunkable']:
                    populate_chunks_from_file(folder / ohdsi_file, interface, sess)
                else:
                    populate_db_from_file(folder / ohdsi_file, interface, sess)
        sess.commit()
