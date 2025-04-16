import pytest
from utils.db_connector import get_connection

@pytest.fixture(scope="session")
def db_connection():
    conn = get_connection("regcor_refine_db")
    yield conn
    conn.close()
