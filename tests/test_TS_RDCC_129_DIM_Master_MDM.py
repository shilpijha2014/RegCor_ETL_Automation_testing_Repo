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
        "source_db": "regcor_refine_db",
        "source_schema": "regcor_refine",
        "source_table": "dim_rdm_regcor_master_table",   
        "target_db": "regcor_refine_db" ,
        "target_schema": "regcor_refine",        
        "target_table": "dim_regcor_product_family_item_configuration"
    }

#This Test set includes test cases of DIM master RDM.

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 107 - This Test set includes test cases of DIM Manufacturer.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

