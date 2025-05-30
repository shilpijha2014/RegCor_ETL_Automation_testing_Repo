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
        "target_table": "association_regcor_dp_ccs"
    }

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 199 - This test set contains of test cases of Association_regcor_dp_ccs table.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

def test_TS_RDCC_199_TC_RDCC_200_dp_material_container_closure_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"\nThis test case validates that dp_materiel_number and container_closure_system in Association_regcor_dp_ccs are correctly fetched using concept_code and relation_code_to from the source table dim_rdm_regcor_master, with the filters Vocabulary_name set to 'DP Flavour' and relation_name set to 'Container Closure System'.\n")
    print(f"Test 1 : Identify records in the association_regcor_dp_ccs table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    query =f"""SELECT  
    s.dp_material_number,
    s.container_closure_system 
            FROM {validation['target_schema']}.{validation['target_table']} s
            LEFT JOIN (
                SELECT 
                    concept_code AS dp_material_number, 
                    relation_code_to AS container_closure_system 
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE 
                    vocabulary_name = 'DP Flavor'
                    AND relation_name = 'Container Closure System'
                GROUP BY 
                    concept_code, relation_code_to
            ) src
            ON s.dp_material_number = src.dp_material_number 
            AND s.container_closure_system = src.container_closure_system
            WHERE src.dp_material_number IS NULL 
            OR src.container_closure_system IS NULL;"""

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

    print(f"\n2)Identify records in the source table that are missing in the association_regcor_dp_ccs table:")

    query =f"""SELECT 
    src.dp_material_number, 
    src.container_closure_system 
FROM (
    SELECT 
        concept_code AS dp_material_number, 
        relation_code_to AS container_closure_system 
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE 
        vocabulary_name = 'DP Flavor'
        AND relation_name = 'Container Closure System'
    GROUP BY 
        concept_code, relation_code_to
) src
LEFT JOIN {validation['target_schema']}.{validation['target_table']} s
ON s.dp_material_number = src.dp_material_number 
   AND s.container_closure_system = src.container_closure_system
WHERE s.dp_material_number IS NULL 
   OR s.container_closure_system IS NULL;
 """
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

def test_TS_RDCC_199_TC_RDCC_201_PK_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case validates the duplicates and null checks for the primary key columns dp_material_number and container_closure_system in Association_assembly_dp_ccs.\n")
    # -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['container_closure_system'  ,'dp_material_number']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

    print("\nIdentify there is no Null values for {primary_keys} in dim table.\n")
    columns_to_check = primary_keys
    result, count,msg = check_all_columns_null_combination(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    try:
        if count == 0:
            message = f"✅ No rows found where all of the columns {columns_to_check} are NULL."
            logging.info(message)
            test = True
        else:
            message = f"❌ {count} row(s) found where all of the columns ({columns_to_check}) are NULL."
            logging.error(message)
            test = False

    except Exception as e:
        message =  f"❌ Error checking NULL combinations: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TS_RDCC_202_TC_RDCC_207_No_Source_Value_Transformation_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case validates that columns in the Association_dp_ccs table are transformed to no_source_values when null values are present in the source table.\n")
    query =f"""
        WITH source_data AS (
    SELECT 
        COALESCE(concept_code, 'No_Source_Value') AS dp_material_number, 
        COALESCE(relation_code_to, 'No_Source_Value') AS container_closure_system 
    FROM 
        {validation['source_schema']}.{validation['source_table']}
    WHERE 
        vocabulary_name = 'DP Flavor' 
        AND relation_name = 'Container Closure System' 
    GROUP BY 
        concept_code, 
        relation_code_to
        ),
        target_data AS (
            SELECT 
                dp_material_number, 
                container_closure_system 
            FROM 
                {validation['target_schema']}.{validation['target_table']}
        )
        -- Compare source and target data
        SELECT 
            s.dp_material_number AS source_dp_material_number,
            t.dp_material_number AS target_dp_material_number,
            s.container_closure_system AS source_container_closure_system,
            t.container_closure_system AS target_container_closure_system
        FROM 
            source_data s
        LEFT JOIN 
            target_data t
        ON 
            s.dp_material_number = t.dp_material_number
            AND s.container_closure_system = t.container_closure_system
        WHERE 
            t.dp_material_number IS NULL 
            OR t.container_closure_system IS NULL"""
    
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ No_Source_Value_Transformation check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ No_Source_Value_Transformation failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during No_Source_Value_Transformation completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)



