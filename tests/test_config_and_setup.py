import pytest
# from omop_alchemy.db import Config

# def test_config_create():
#     assert Config() is not None

# def test_config_contents():
# # TODO: figure out how to override config file to make sure we can reflect settings correctly
#     ...

# def test_schema_creation():
#     ...


# @pytest.fixture
# def db():
# #    create sqlite temp db that can be used to run tests
#    engine = create_engine(
#        "sqlite:///:memory:", connect_args={"check_same_thread": False}
#    )
#    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#    Base.metadata.create_all(bind=engine)
#    return TestSession()