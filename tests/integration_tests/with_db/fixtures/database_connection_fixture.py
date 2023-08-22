import pytest
import pyexasol
from tests.utils.parameters import db_params


@pytest.fixture(scope="session")
def pyexasol_connection() -> pyexasol.ExaConnection:
    conn = pyexasol.connect(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password)
    return conn

