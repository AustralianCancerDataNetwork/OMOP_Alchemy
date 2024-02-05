from pathlib import Path
import sqlalchemy as sa

# TODO make this more flexible so that you aren't stuck with sqlite demo only

class Config(object):

    home_dir = Path(__file__).parent.parent
    data_path = home_dir / 'resources' 
    db_dir = data_path / 'demo.db'
    vocab_load = data_path 

    vocab_load.mkdir(parents=True, exist_ok=True)

    VOCAB_PATH = f'{vocab_load.resolve()}'
    SQLALCHEMY_DATABASE_URI = f'sqlite:///{db_dir.resolve()}'
    SECRET_KEY='dev'

config = Config()
engine = sa.create_engine(config.SQLALCHEMY_DATABASE_URI, echo=True)
