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
#         "target_table": "association_regcor_ds_event",
#         "vocabulary_name": "DS Flavor",
#     }
# # This Test set contains test cases for Dim drug substance event.

# def test_TS_RDCC_69_TC_RDCC_70_Drug_Substance_validation(db_connection: connection | None,validation: dict[str, str]): 
   
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 64 - This Test set contains test cases for Drug Substance Event Association.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

   
#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")

#     print(f"This Test case validates the ds_materiel_number,event_id in dim_regcor_drug_substance_event is correctly fetched with concept_code,relation_code_to in source dim_rdm_regcor_master.\n")
#     print(f"Test 1 : Identify records in the dim_regcor_drug_substance_event table that are missing in the source table (dim_rdm_regcor_master_table):\n")
#     query = f"""SELECT 
#                     s.ds_material_number, 
#                     s.event_id 
#                     FROM {validation['target_schema']}.{validation['target_table']} s
#                 except 
#                 select src.ds_material_number,src.event_id
#                 from(
#                     SELECT 
#                         concept_code AS ds_material_number, 
#                         relation_code_to AS event_id 
#                     FROM {validation['source_schema']}.{validation['source_table']}
#                     WHERE 
#                         vocabulary_name = 'DS Flavor'
#                         AND relation_name = 'RIM Event'
#                     GROUP BY 
#                         concept_code, relation_code_to
#                 ) src
#                 """
    
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

#     print(f"\nTest 2 : Identify records in the source table that are missing in the dim_regcor_drug_substance_event table.\n")
#     query = f"""SELECT 
#     src.ds_material_number, 
#     src.event_id 
#             FROM (
#                 SELECT 
#                     concept_code AS ds_material_number, 
#                     relation_code_to AS event_id 
#                 FROM {validation['source_schema']}.{validation['source_table']}
#                 WHERE 
#                     vocabulary_name = 'DS Flavor'
#                     AND relation_name = 'RIM Event'
#                 GROUP BY 
#                     concept_code, relation_code_to
#             ) src
#             except
#             SELECT 
#                 s.ds_material_number, 
#                 s.event_id 
#             FROM {validation['target_schema']}.{validation['target_table']}  s
# """
    
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_69_TC_RDCC_71_Primary_Key_Validation(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis Test case validates the Duplicates,Null checks of Primary key column event_id in DIM table. \n")
# # -- Check for duplicates in country_code 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['ds_material_number,event_id']
#     result = check_primary_key_duplicates(
#     connection=db_connection,
#     schema_name=validation['target_schema'],
#     table_name=validation["target_table"],
#     primary_keys=primary_keys)
#     assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
#     print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"event_id")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.event_id"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.event_id"
#         f" contains NO NULL values!\n")


