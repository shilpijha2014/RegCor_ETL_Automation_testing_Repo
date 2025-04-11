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
    conn = get_connection(validation["db"])
    assert validate_table_exists(conn, validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists")

def test_validate_connection(validation):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        conn = get_connection(validation["db"])
        print("\nTest Case : TC487 : This Test set contains test cases for Fact_regcor_drug_substance_registration\n")

        assert conn is not None, f"❌ Connection object is None for {validation["db"]}"
        print(f"✅ Successfully connected to database: {validation["db"]}")
        conn.close()
    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation["db"]}: {str(e)}")

# fact_regcor_drug_product_registration.Registration_id column Validation
def test_regitration_id_null_values(validation: dict[str, str]):
    """
    Checks if a column contains NULL values in a given table and schema.

    Args:
        validation (dict): A dictionary with keys:
            - "db": Database name (defined in db_config.yaml)
            - "schema": Schema name
            - "target_table": Table name
            - "target_column": Column to check
    Raises:
        AssertionError: If NULL values are found in the specified column.
    """
    conn = get_connection(validation["db"])
    cursor = conn.cursor()

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


def test_data_completeness_reg_id(validation):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_data_completeness(
        conn, conn,
        validation['schema'],validation["source_table"],validation["source_column"],
        validation["schema"],validation["target_table"],validation["target_column"]
        )
    assert passed, message

    
