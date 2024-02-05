# need to import at least these objects to make sure all tables are added to the metadata properly by the time we want to use them
from .db import Base
from .tables.clinical import Person
from .tables.vocabulary import Concept
from .tables.onco_ext import episode