from psycopg2._psycopg import connection
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

@pytest.fixture
def validation():
    return {
        "db": "regcor_refine_db",
        "schema": "regcor_refine",
        "target_table": "dim_regcor_country",
        "source_table": "country",
    }

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC-46 - This Test case validates the Registration start,end,status date,Registration number in dim table is fetched from source registration source table.")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['db']}"
        print(f"✅ Successfully connected to database: {validation['db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    assert validate_table_exists( db_connection,validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")
    
# Test Case - RDCC-58 - This Test set contains test cases for Dim Country table.
def test_TS_RDCC_58_TC_RDCC_59_country_code(db_connection: connection | None,validation: dict[str, str]):
 
    cursor = db_connection.cursor()
    print("Test Case - RDCC-47 - This Test case validates the Registration_id in dim_regcor_registration is correctly mapped with id in source registration table .")
    print("Checking if a column contains NULL values in a given table and schema.")