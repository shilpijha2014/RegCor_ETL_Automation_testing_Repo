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
        "target_table": "stg_dim_regcor_manufacturer"
    }

#This Test set includes tests of DIM Drug Assembly Table.

def test_TS_RDCC_107_TC_RDCC_108_manufacturer_validation(db_connection: connection | None,validation: dict[str, str]): 
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 107 - This Test set includes test cases of DIM Manufacturer.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")
 
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

    print(f"RDCC-108-This Test case validates manufacturer coulmn  data in dim manufacturer table is correctly fetched from dim_rdm_regcor_master_table by filtering Vocabulary name with 'RIM Manufacturers\n")
    print(f"Test 1 : Identify manufacturer in the dim_regcor_manufacturer table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    success, count, msg = validate_target_to_source_with_filter(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['manufacturer '],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.vocabulary_name= 'RIM Manufacturers'""",
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
        tgt_cols=['manufacturer'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
        vocabulary_name= 'RIM Manufacturers'""",
        tgt_filter=""
    )
    print(msg)
    assert result, msg

def test_TS_RDCC_107_TC_RDCC_109_manufacturer_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-109-This Test case validates the  manufacturer_id in dim manufatcurer is correctly retrive property_value using property_name as “RIM_Manufacturer_id”  with filter condition 'RIM Manufacturers' as vocabulary name from dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify manufacturer_id in the dim_regcor_manufactuer table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f""" SELECT tgt.manufacturer,tgt.manufacturer_id 
        FROM {validation['target_schema']}.{validation['target_table']} tgt
        except
        SELECT drrmt.concept_code,
        MAX(CASE WHEN property_name = 'RIM Manufacturer ID' THEN property_value END) AS manufacturer_id
            FROM {validation['source_schema']}.{validation['source_table']} drrmt
            WHERE vocabulary_name = 'RIM Manufacturers'
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

    query =f"""
        SELECT drrmt.concept_code,
            MAX(CASE WHEN property_name = 'RIM Manufacturer ID' THEN property_value END) AS manufacturer_id
                FROM {validation['source_schema']}.{validation['source_table']} drrmt
                WHERE vocabulary_name = 'RIM Manufacturers'
                group by concept_code 
            except 
            SELECT tgt.manufacturer, tgt.manufacturer_id 
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


def test_TS_RDCC_107_TC_RDCC_110_registered_manufacturer_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-110-This Test case validates the  registered_manufacturer in dim manufacturer is correctly retrive property_value using property_name as “Registered Manufacturer”  with filter condition 'RIM Manufacturers' as vocabulary name from dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify registered_manufacturer in the dim_regcor_manufactuer table that are missing in the source table (dim_rdm_regcor_master_table):   \n")

    query =f""" SELECT tgt.manufacturer,tgt.registered_manufacturer 
        FROM {validation['target_schema']}.{validation['target_table']} tgt
        except
        SELECT drrmt.concept_code,
        MAX(CASE WHEN property_name = 'Registered Manufacturer' THEN property_value END) AS registered_manufacturer
            FROM {validation['source_schema']}.{validation['source_table']} drrmt
            WHERE vocabulary_name = 'RIM Manufacturers'
            group by concept_code """

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

    query =f"""
        SELECT concept_code,
            MAX(CASE WHEN property_name = 'Registered Manufacturer' THEN property_value END) AS registered_manufacturer
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'RIM Manufacturers'
                group by concept_code 
            except 
            SELECT tgt.manufacturer,tgt.registered_manufacturer  
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


def test_TS_RDCC_107_TC_RDCC_111_sap_plant_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-111-This Test case validates the  sap_plant_id in dim manufatcurer is correctly retrive property_value using property_name as “SAP Plant Code”  with filter condition 'RIM Manufacturers' as vocabulary name from dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify sap_plant_code in the dim_regcor_manufactuer table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f""" SELECT tgt.manufacturer, tgt.sap_plant_code 
            FROM {validation['target_schema']}.{validation['target_table']} tgt
            except
            SELECT concept_code,
            MAX(CASE WHEN property_name = 'SAP Plant Code' THEN property_value END) AS sap_plant_code
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'RIM Manufacturers'
                group by concept_code """

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

    query =f"""
                SELECT concept_code,
                MAX(CASE WHEN property_name = 'SAP Plant Code' THEN property_value END) AS sap_plant_code
                    FROM {validation['source_schema']}.{validation['source_table']}
                    WHERE vocabulary_name = 'RIM Manufacturers'
                    group by concept_code 
                except 
                SELECT tgt.manufacturer,tgt.sap_plant_code 
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


def test_TS_RDCC_107_TC_RDCC_112_PK_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Duplicates,Null checks of Primary key column manufacturer in DIM table .\n")
    # -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['manufacturer']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

    print(f"\nIdentify there is no Null values for {primary_keys} in dim table.\n")
    columns_to_check = ["manufacturer"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message


