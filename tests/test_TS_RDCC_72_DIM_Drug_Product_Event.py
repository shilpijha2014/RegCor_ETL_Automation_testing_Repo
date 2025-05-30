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
        "target_table": "association_regcor_dp_event",
    }
# This Test set contains test cases for Dim drug Product event.

def test_TS_RDCC_72_TC_RDCC_73_data_completeness(db_connection: connection | None,validation: dict[str, str]): 
    """
    Test to validate that a connection to the database can be established.
    """

    print(f"TS-RDCC-69-This Test set contains test cases for Dim drug product event")
    try:
               
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")


    print(f"Test Case RDCC 73 : This Test case validates the dp_materiel_number,event_id in  dim_regcor_drug_product_event is correctly fetched with concept_code,relation_code_to in source dim_rdm_regcor_master.\n")
    print(f"Identify records in the association_regcor_dp_event table that are missing in the source table (dim_rdm_regcor_master_table):")

    query = f"""SELECT 
    s.dp_material_number, 
    s.event_id 
    FROM {validation['target_schema']}.{validation['target_table']} s
    except 
        SELECT 
            src.dp_material_number, 
            src.event_id from (
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
"""
    
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"\nTest 2 : Identify records in the source table that are missing in the dim_regcor_drug_substance_event table.\n")
    try:

        query = f"""SELECT 
    src.dp_material_number, 
    src.event_id 
FROM (
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
except
SELECT 
    s.dp_material_number, 
    s.event_id 
FROM {validation['target_schema']}.{validation['target_table']} s
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

def test_TS_RDCC_72_TC_RDCC_74_primary_key_check(db_connection: connection | None,validation: dict[str, str]): 
    print(f"TC-RDCC-74- This Test case validates the Duplicates,Null checks  Primary key column dp_material_number,event_id in DIM table.\n")
# -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['dp_material_number','event_id']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

    columns_to_check = ["dp_material_number", "event_id"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

