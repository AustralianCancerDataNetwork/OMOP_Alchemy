import pytest
from omop_alchemy.db import Config

def test_config_create():
    assert Config() is not None


# @pytest.fixture
# def db():
#     ...
#    engine = create_engine(
#        "sqlite:///:memory:", connect_args={"check_same_thread": False}
#    )
#    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#    Base.metadata.create_all(bind=engine)
#    return TestSession()