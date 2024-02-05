import sqlalchemy as sa

# TODO make this nicer so that it can check if you've already set it up 
# and take input args to drop and recreate

def create_db(Base, engine):
    Base.metadata.create_all(engine)