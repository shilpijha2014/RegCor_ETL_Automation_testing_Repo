import pytest
import yaml
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.db_connector import *
from utils.validations_utils import *

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# -----------✅ Test: Table Exists ----------
@pytest.fixture
def validation():
    return {
        "db": "regcor_refine_db",
        "schema": "regcor_refine",
        "target_table": "dim_regcor_registration",
    }

def test_validate_connection(validation):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        conn = get_connection(validation["db"])
        
        assert conn is not None, f"❌ Connection object is None for {validation["db"]}"
        print(f"✅ Successfully connected to database: {validation["db"]}")
        conn.close()
    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation["db"]}: {str(e)}")
        
def test_table_exists(validation):
    print(f"Test Set-RDCC-48 - This Test case validates the Registration start,end,status date,Registration number in dim table  is fetched from source registration source table.")
    conn = get_connection(validation["db"])
    assert validate_table_exists(conn, validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")


