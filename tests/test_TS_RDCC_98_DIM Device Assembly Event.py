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
        "target_table": "stg_dim_regcor_device_assembly"
    }

#This Test set includes tests of DIM Drug Assembly Table.

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

def test_TS_RDCC_98_TC_RDCC_99_da_material_number_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-99-This Test case validates da_materiel_number coulmn  data in dim device assembly table is correctly fetched from dim_rdm_regcor_master_table for DA Flavour Vocabulary name.\n")
    print(f"Test 1 : Identify da_material_number in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    success, count, msg = validate_target_to_source_with_filter(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['da_material_number'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.vocabulary_name='DA Flavor'""",
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
        tgt_cols=['da_material_number'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
        vocabulary_name='DA Flavor' """,
        tgt_filter=""
    )
    print(msg)
    assert result, msg

    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = ["da_material_number"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message


def test_TS_RDCC_98_TC_RDCC_100_da_material_number_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-100-This Test case validates the device assembly manufacturer in dim device assembly is correctly retrive property_value using property_name as “Manufacturer”  for DA flavour as vocabulary name.\n")
    print(f"Test 1 : Identify device_assembly_manufacturer in the dim_regcor_drug_assembly table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f"""SELECT tgt.device_assembly_manufacturer 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT 
    MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS device_assembly_manufacturer
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DA Flavor'
        group by concept_code
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

    query =f"""SELECT 
    MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS device_assembly_manufacturer
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DA Flavor'
        group by concept_code 
    except 
    SELECT tgt.device_assembly_manufacturer 
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


def test_TS_RDCC_98_TC_RDCC_101_da_manufacturing_line_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-100-This Test case validates the da manufacturing line in dim device assembly is correctly retriving  property_value using property_name as “Manufacturing Line”  from dim_rdm_regcor_master_table for DA flavour as vocabulary name\n")
    print(f"Test 1 : Identify da_manufacturing_line in the dim_regcor_device_assembly table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f"""SELECT tgt.da_manufacturing_line 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT 
    MAX(CASE WHEN property_name = 'Manufacturing Line' THEN property_value END) AS da_manufacturing_line
        FROM  {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DA Flavor'
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
    MAX(CASE WHEN property_name = 'Manufacturing Line' THEN property_value END) AS da_manufacturing_line
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'DA Flavor'
    group by concept_code 
    except 
    SELECT tgt.da_manufacturing_line 
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


