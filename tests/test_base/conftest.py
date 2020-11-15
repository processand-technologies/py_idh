import pytest


_samples = {}

@pytest.fixture(scope="session")
def samples():
    return _samples