# from psycopg2._psycopg import connection
# import pytest
# import yaml
# import sys
# import os
# import logging

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from utils.db_connector import *
# from utils.validations_utils import *

# # Setup logging
# logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# @pytest.fixture
# def validation():
#     return {
#         "source_db": "regcor_refine_db",
#         "source_schema": "regcor_refine",
#         "source_table": "dim_rdm_regcor_master_table",   
#         "target_db": "regcor_refine_db" ,
#         "target_schema": "regcor_refine",        
#         "target_table": "stg_dim_regcor_container_closure_system"
#     }

# #This Test set includes test cases of DIM master RDM.

# def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 202 - This test set contains of test cases of dim_regcor_container_closure_system table.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

# def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")

# def test_TS_RDCC_202_TC_RDCC_203_container_closure_system_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"\nThis test case verifies that container_closure_system_name and container_closure_system_description in dim_regcor_container_closure_system are correctly retrieved using concept_code and concept_name from the source table dim_rdm_regcor_master, with the filter Vocabulary_name set to 'Container Closure System'\n")
#     print(f"Test 1 : Identify records in the dim_regcor_container_closure_system table that are missing in the source table (dim_rdm_regcor_master_table):\n")
#     query =f"""SELECT  
#                 s.container_closure_system_name,
#                     s.container_closure_system_description 
#                 FROM {validation['target_schema']}.{validation['target_table']} s
#                 LEFT JOIN (
#                     SELECT 
#                         concept_code AS container_closure_system_name, 
#                         concept_name AS container_closure_system_description 
#                     FROM {validation['source_schema']}.{validation['source_table']}
#                     WHERE 
#                         vocabulary_name = 'Container Closure System'
#                     GROUP BY 
#                         concept_code,concept_name
#                 ) src
#                 ON s.container_closure_system_name = src.container_closure_system_name 
#                 AND s.container_closure_system_description = src.container_closure_system_description
#                 WHERE src.container_closure_system_name IS NULL 
#                 OR src.container_closure_system_description IS NULL"""

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during target-to-source completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     print(f"\n2)Identify records in the source table that are missing in the dim_regcor_container_closure_system table:")

#     query =f"""SELECT 
#     src.container_closure_system_name, 
#     src.container_closure_system_description 
#         FROM (
#             SELECT 
#                 concept_code AS container_closure_system_name, 
#                 relation_code_to AS container_closure_system_description 
#             FROM {validation['source_schema']}.{validation['source_table']}
#             WHERE 
#                 vocabulary_name = 'Container Closure System'
#             GROUP BY 
#                 concept_code, relation_code_to
#         ) src
#         LEFT JOIN {validation['target_schema']}.{validation['target_table']} s
#         ON s.container_closure_system_name = src.container_closure_system_name 
#         AND s.container_closure_system_description = src.container_closure_system_description
#         WHERE src.container_closure_system_name IS NULL 
#         and src.container_closure_system_description IS NULL;
#  """
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Source-to-Target check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_202_TC_RDCC_204_PK_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates the duplicates and null checks for the primary key columns container_closure_system_name and container_closure_system_description in dim_regcor_container_closure_system.\n")
#     # -- Check for duplicates in primary keys 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['container_closure_system_name'  ,'container_closure_system_description']
#     result = check_primary_key_duplicates(
#     connection=db_connection,
#     schema_name=validation['target_schema'],
#     table_name=validation["target_table"],
#     primary_keys=primary_keys)
#     assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
#     print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

#     print("\nIdentify there is no Null values for {primary_keys} in dim table.\n")
#     columns_to_check = primary_keys
#     result, count,msg = check_all_columns_null_combination(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     try:
#         if count == 0:
#             message = f"✅ No rows found where all of the columns {columns_to_check} are NULL."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ {count} row(s) found where all of the columns ({columns_to_check}) are NULL."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message =  f"❌ Error checking NULL combinations: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_202_TC_RDCC_208_No_Source_Value_Transformation_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case ensures that the columns in the dim_product_family_item_configuration table are transformed to no_source_values when null values are present in the source table.\n")
#     query =f"""
#         WITH source_data AS (select 
# distinct 
# coalesce(concept_code, 
# 'No_Source_Value') 
# as container_closure_system_name, 
# coalesce(concept_name, 
# 'No_Source_Value') 
# as container_closure_system_description 
# from 
# {validation['source_schema']}.{validation['source_table']}
# where 
# vocabulary_name = 'Container Closure System'),
# target_data AS (
#     SELECT 
#        container_closure_system_name,container_closure_system_description
#     FROM 
#         {validation['target_schema']}.{validation['target_table']}
# )
# SELECT 
# s.container_closure_system_name AS source_container_closure_system_name,
#     t.container_closure_system_name AS targetcontainer_closure_system_name,
#     s.container_closure_system_description as source_container_closure_system_description,
# t.container_closure_system_description as target_container_closure_system_description
# FROM 
#     source_data s
# LEFT JOIN 
#     target_data t
# ON 
#     s.container_closure_system_name = t.container_closure_system_name
#     and s.container_closure_system_description=t.container_closure_system_description
# where 
# s.container_closure_system_description!=t.container_closure_system_description
# or s.container_closure_system_name!=t.container_closure_system_name"""
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ No_Source_Value_Transformation check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ No_Source_Value_Transformation failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during No_Source_Value_Transformation completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)



