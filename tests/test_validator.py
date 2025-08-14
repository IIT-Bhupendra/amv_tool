import pytest
from mongo_validator import MongoValidator

def test_validator_initialization():
    validator = MongoValidator(
        connection_string="mongodb://localhost:27017",
        database="test_db"
    )
    assert validator.rules is not None
    assert 'collections' in validator.rules