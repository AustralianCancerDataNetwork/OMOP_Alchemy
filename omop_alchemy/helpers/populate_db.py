import csv
from pathlib import Path
from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.orm as so
import sqlalchemy.sql.sqltypes as sss

from ..db.config import engine, config
from ..tables.clinical import Person, Condition_Occurrence, Measurement
from ..tables.health_system import Care_Site, Location, Provider
from ..tables.vocabulary import Concept, Vocabulary, Concept_Class, Domain, Relationship, \
                                Concept_Relationship, Concept_Ancestor

# TODO: insert some validation and checks to make sure folk know how and why to do this (and the limits for demo purposes)

to_load_vocabulary = {'folder': 'ohdsi_vocabs',
                      'VOCABULARY.csv': Vocabulary, 
                      'CONCEPT.csv': Concept, 
                      'CONCEPT_CLASS.csv': Concept_Class, 
                      'DOMAIN.csv': Domain,
                      'RELATIONSHIP.csv': Relationship}
                      #'CONCEPT_RELATIONSHIP.csv': Concept_Relationship,
                      #'CONCEPT_ANCESTOR.csv': Concept_Ancestor}

to_load_health_system = {'folder': 'demo_data',
                         'CARE_SITE.csv': Care_Site,
                         'LOCATION.csv': Location,
                         'PROVIDER.csv': Provider}

to_load_clinical = {'folder': 'demo_data',
                    'PERSON.csv': Person,
                    'CONDITION_OCCURRENCE.csv': Condition_Occurrence,
                    'MEASUREMENT.csv': Measurement}#,
                    #'CONCEPT.csv': Concept}

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
        return Decimal(i)
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

def populate_demo_db(to_load):
    with so.Session(engine) as sess:
        folder = Path(config.VOCAB_PATH) / to_load['folder']
        print(folder)
        for ohdsi_file, interface in to_load.items():
            if interface != folder.name:
                try:
                    with open(folder / ohdsi_file, 'r') as file:
                        reader = csv.DictReader(file, delimiter='\t')
                        field_map = get_type_lookup(interface)
                        
                        for row in reader:
                            record = {field:field_map[field](data) for field, data in row.items() if field in field_map}
                            o = interface(**record)
                            sess.add(o)

                        print(f'complete load for: {ohdsi_file}')
                except Exception as e:
                    print(f'Error loading data file {ohdsi_file}. Have you unzipped it in the correct location ({config.VOCAB_PATH})?')
        sess.commit()


def populate_clinical_demo_data():
    with so.Session(engine) as sess:

        gender_concepts = [(8532, 'F', 'FEMALE'), (8507, 'M', 'MALE')]
        ages = [{'year_of_birth': 2000}, 
                {'year_of_birth': 2000, 'day_of_birth': 12, 'month_of_birth': 3},
                {'birth_datetime': datetime(2000, 4, 2)}, 
                {'birth_datetime': datetime.now()},
                {'birth_datetime': datetime(2000, 4, 2), 'death_datetime': datetime(2008, 4, 2)}]

        for n in range(15, 20):
            p = Person(person_id=n, 
                    gender_concept_id=gender_concepts[n%2][0],
                    ethnicity_concept_id=0,
                    race_concept_id=0,
                    **ages[n%5])
            sess.add(p)
        sess.commit()

