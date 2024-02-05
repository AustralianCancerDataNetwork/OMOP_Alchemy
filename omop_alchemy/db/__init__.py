import sqlalchemy.orm as so
from .config import config, engine

Base = so.declarative_base()

__all__ = [Base, config, engine]