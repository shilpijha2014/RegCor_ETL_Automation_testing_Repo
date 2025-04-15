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
        "target_table": "fact_regcor_drug_substance_registration",
        "source_table": "registration",
        "target_column": "registration_id",
        "source_column":"id"
    }

# -----------✅ Test: Table Exists ----------

def test_table_exists(validation):
    print(f"Test Set-RDCC-18 - This Test set contains test cases for Fact_regcor_drug_substance_registration.")
    conn = get_connection(validation["db"])
    assert validate_table_exists(conn, validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

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

# fact_regcor_drug_product_registration.Registration_id column Validation
def test_TC_RDCC_19_1_regitration_id_null_values(validation: dict[str, str]):
    conn = get_connection(validation["db"])
    cursor = conn.cursor()
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


def test_TC_RDCC_19_2_regitration_id_data_completeness_Test(validation):
    conn = get_connection(validation["db"])
    print("""Checking all registration_ids in the fact table(target) exist in the 
          registration(source) table and no records are not missed and correctly 
          mapped with source table.""")
    result = check_data_completeness(
        conn, 
        validation['schema'],validation["source_table"],validation["source_column"],
        validation["schema"],validation["target_table"],validation["target_column"]
        )
    print(result)
    conn.close()

# fact_regcor_drug_product_registration.Registration_name column Validation
def test_TC_RDCC_20_1_regitration_name_column_null_values(validation: dict[str, str]):
    print("TC-RDCC-20-This Test case validates the registration_name in fact table  is correctly mapped with Name__v from source rim_ref.registration source table.")
    conn = get_connection(validation["db"])
    cursor = conn.cursor()
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['schema']}"."{validation['target_table']}" 
        WHERE registration_name IS NULL;
    """

    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name "
        f"contains NO NULL values!\n")

def test_TC_RDCC_20_2_3_regitration_name_data_completeness_Test(validation):
    conn = get_connection(validation["db"])

    result = check_data_completeness(
        conn, 
        validation['schema'],validation["source_table"],"name__v",
        validation["schema"],validation["target_table"],"registration_name"
        )
    print(result)
    conn.close()

# fact_regcor_drug_product_registration.Registration_state column Validation
def test_TC_RDCC_20_1_regitration_state_column_null_values(validation: dict[str, str]):
 
    conn = get_connection(validation["db"])
    cursor = conn.cursor()
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['schema']}"."{validation['target_table']}" 
        WHERE registration_state  IS NULL;
    """

    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name "
        f"contains NO NULL values!\n")

#This query checks if the registration_state values in the fact_regcor_drug_substance_registration 
# table have corresponding labels in the objectlifecyclestate_ref table.
    
def test_TC_RDCC_20_2_3_regitration_name_data_completeness_Test(validation):
    conn = get_connection(validation["db"])

    result = check_data_completeness(
        conn, 
        validation["schema"],validation["source_table"],"name__v",
        validation["schema"],validation["target_table"],"registration_name"
        )
    print(result)
    conn.close()

