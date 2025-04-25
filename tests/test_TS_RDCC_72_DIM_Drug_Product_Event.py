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
        "target_table": "stg_dim_regcor_drug_product_event",
    }
# This Test set contains test cases for Dim drug Product event.

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 72 - This Test set contains test cases for Dim drug product event.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    print(f"TS-RDCC-69-This Test set contains test cases for Dim drug substance event")
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

def test_TS_RDCC_72_TC_RDCC_73_data_completeness(db_connection: connection | None,validation: dict[str, str]): 
    print(f"Test Case RDCC 73 : This Test case validates the dp_materiel_number,event_id in  dim_regcor_drug_product_event is correctly fetched with concept_code,relation_code_to in source dim_rdm_regcor_master.\n")
    print(f"\nTest 2 : Identify records in the source table that are missing in the dim_regcor_drug_substance_event table.\n")
    try:

        query = f"""SELECT 
                s.ds_material_number, 
                s.event_id 
                FROM {validation['target_schema']}.{validation['target_table']} s
                LEFT JOIN (
                SELECT 
                    concept_code AS dp_material_number, 
                    relation_code_to AS event_id 
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE 
                    vocabulary_name = 'DP Flavor'
                    AND relation_name = 'RIM Event'
                GROUP BY 
                    concept_code, relation_code_to
                ) src
                ON s.dp_material_number = src.dp_material_number 
                AND s.event_id = src.event_id
                WHERE src.dp_material_number IS NULL 
                OR src.event_id IS NULL;
                """
        
        test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
        

        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)
