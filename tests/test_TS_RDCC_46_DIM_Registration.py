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
        "target_table": "dim_regcor_registration",
        "source_table": "registration",
        "target_column": "registration_id",
        "source_column": "id"
    }

def test_validate_connection(db_connection, validation):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC-46 - This Test case validates the Registration start,end,status date,Registration number in dim table is fetched from source registration source table.")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['db']}"
        print(f"✅ Successfully connected to database: {validation['db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['db']}: {str(e)}")

def test_table_exists(db_connection,validation):
    
    assert validate_table_exists( db_connection,validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")
    
# Test Case - RDCC-47 - This Test case validates the Registration_id in  dim_regcor_registration is correctly mapped with id in source registration table .
def test_TC_RDCC_47_1_regitration_id_null_values(db_connection,validation: dict[str, str]):
    
    cursor = db_connection.cursor()
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['schema']}"."{validation['target_table']}" 
        WHERE "{validation['target_column']}" IS NULL;
    """
    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains NO NULL values!\n")
    
def test_TC_RDCC_47_2_and_3_regitration_id_col_data_completeness(db_connection,validation):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key=validation["source_column"],
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key=validation["target_column"]
    )
    assert passed, message
    print(message)

def test_TC_RDCC_48_1_registration_name_null_values(db_connection,validation: dict[str, str]):

    print("Checking if a column contains NULL values in a given table and schema.")
    null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"registration_name")

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name"
        f"contains NO NULL values!\n")

def test_TC_RDCC_47_2_and_3_registration_name_col_data_completeness(db_connection,validation):

    passed, missing_count, message = check_col_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="name__v",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_name'
    )
    assert passed, message


