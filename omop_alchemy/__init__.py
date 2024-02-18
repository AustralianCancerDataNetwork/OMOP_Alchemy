# need to import at least these objects to make sure all tables are added to the metadata properly by the time we want to use them
from .db import Base
from .tables.vocabulary import Concept
from .tables.clinical import Person
from .tables.onco_ext import episode
from .tables.health_system import Provider
from .helpers.create_db import create_db 

# grabbing environment variables for path of configuration files - review files .example_dotenv and 
# oa_system_config_demo.yaml to see what items need to be setup in your local environment

from dotenv import load_dotenv
import os

load_dotenv()

message = ""
try:
    os.environ['OA_CONFIG']
except:
    message = 'Please set OA_CONFIG environment variable to allow loading of db configuration details'

# weird way to do this, but necessary if we are to be able to use jupyter - can't handle exiting from
# an exception block for some reason...
if len(message) > 0:
    raise SystemExit(message)

# this script will create a new sqlite db file with the tables to reflect the specified objects in 
# clinical and reference directories. needs to be made more friendly for re-create / update etc, but 
# will do for now :)

from .db import oa_config, logger
create_db(Base, oa_config.engine)

from .conventions import ConditionModifiers
from .conventions.vocab_lookups import tnm_lookup, grading_lookup, mets_lookup, gender_lookup, race_lookup, ethnicity_lookup, VocabLookup


for table in [Person]:
    Person.set_validators()