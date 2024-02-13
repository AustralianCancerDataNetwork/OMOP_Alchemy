# need to import at least these objects to make sure all tables are added to the metadata properly by the time we want to use them
from .db import Base
from .tables.vocabulary import Concept
from .tables.clinical import Person
from .tables.onco_ext import episode
from .tables.health_system import Provider
from .helpers.create_db import create_db 

# this script will create a new sqlite db file with the tables to reflect the specified objects in 
# clinical and reference directories. needs to be made more friendly for re-create / update etc, but 
# will do for now :)

from .db import oa_config, logger
create_db(Base, oa_config.engine)

from .conventions import ConditionModifiers
from .conventions.vocab_lookups import tnm_lookup, grading_lookup, mets_lookup, gender_lookup, race_lookup, ethnicity_lookup, VocabLookup


for table in [Person]:
    Person.set_validators()