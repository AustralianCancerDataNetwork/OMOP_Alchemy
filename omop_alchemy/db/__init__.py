import sqlalchemy.orm as so
from .create_db import create_db

Base = so.declarative_base()
__all__ = [Base, create_db]