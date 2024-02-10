import sqlalchemy.orm as so
from .config import oa_config, Config

Base = so.declarative_base()

__all__ = [Base, oa_config, Config]