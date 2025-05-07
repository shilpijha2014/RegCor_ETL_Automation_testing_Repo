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
        "target_table": "stg_dim_regcor_drug_substance"
    }

#This Test set includes tests of DIM Drug Substance.

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 78 - This Test set includes tests of DIM Drug Substance.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    print(f"TS-RDCC-69-This Test set contains test cases for Dim drug substance event")
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

def test_TS_RDCC_78_TC_RDCC_79_data_completeness(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-79-This Test case validates ds_materiel_number coulmn  data in dim drug substance table is correctly fetched from dim_rdm_regcor_master_table for DS Flavour Vocabulary name.\n")
    print(f"Test 1 : Identify ds_material_number in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    success, count, msg = validate_target_to_source_with_filter(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['ds_material_number'],
        src_filter=f"{validation['source_schema']}.{validation['source_table']}.vocabulary_name='DS Flavor'",
        tgt_filter=""
    )

    assert success, msg

    result, count, msg = validate_source_to_target_with_filter(
       connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['ds_material_number'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
        vocabulary_name='DS Flavor'""",
        tgt_filter=""
    )
    print(msg)
    assert result, msg

def test_TS_RDCC_78_TC_RDCC_79_null_check(db_connection, validation):
    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = ["ds_material_number"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_80_process_identifier(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify process_identfier in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.process_identifier 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT 
    MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) AS process_identifier
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
    group by concept_code"""
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

    query =f"""SELECT 
        MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) 
        AS process_identifier
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        group by concept_code 
    except 
    SELECT tgt.process_identifier 
    FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TS_RDCC_78_TC_RDCC_79_null_check(db_connection, validation):
    print("\nIdentify there is no Null values for process_identifier in dim table.\n")
    columns_to_check = ["process_identifier"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message












