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
#         "source_table": "t_ompj_proj",   
#         "target_db": "regcor_refine_db" ,
#         "target_schema": "regcor_refine",        
#         "target_table": "dim_rdm_regcor_master_table"
#     }

# #This Test set includes test cases of DIM master RDM.

# def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 129 - This Test set includes test cases of DIM Master MDM table.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

# def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")

# def test_TS_RDCC_129_TC_RDCC_130_product_configuration_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"RDCC-130-This test case validates that the project_name column data in the Master Table RDM DIM table is correctly fetched from t_ompj_proj by filtering PJ_PJ_SEQ with 'Project id'.\n")
#     print(f"Test 1 : Identify project_name in the dim_rdm_regcor_master_table table that are missing in the source table (dim_rdm_regcor_master_table):\n")
#     query =f"""select distinct dm.project_name from  {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct project."PJ_PJ_NAME" 
#                 from {validation['source_schema']}.{validation['source_table']} project
#                 where project."PJ_PJ_NAME" = 'RegCoR' and project."PJ_PJ_SEQ"='1332'"""

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

#     query =f"""select distinct project."PJ_PJ_NAME" 
#                 from {validation['source_schema']}.{validation['source_table']} project
#                 where project."PJ_PJ_NAME" = 'RegCoR' and project."PJ_PJ_SEQ"='1332'
#                 except
#                 select distinct dm.project_name 
#                 from  {validation['target_schema']}.{validation['target_table']} dm
# """
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

#     print(f"\nIdentify there is Null values for project_name in {validation['target_table']} table.\n")
#     columns_to_check = ["project_name "]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message 

# def test_TS_RDCC_129_TC_RDCC_131_vocabulary_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"RDCC-131-This test case validates that the vocabulary_name  column data in the Master Table RDM DIM table is correctly fetched from t_ompj_proj by filtering PJ_PJ_SEQ with 'Project id'.\n")
#     print(f"Test 1 : Identify vocabulary_name  in the dim_rdm_regcor_master_table table that are missing in the source table (dim_rdm_regcor_master_table):\n")
#     query =f"""select distinct dm.vocabulary_name from  
#                 {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select vocabulary."VO_VO_NAME" from 
#                 {validation['source_schema']}.t_omvo_voc vocabulary,{validation['source_schema']}.{validation['source_table']} project
#                 where project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" and project."PJ_PJ_NAME" = 'RegCoR'"""

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

#     query =f"""Select vocabulary."VO_VO_NAME" as source_vocabulary_name 
#     from {validation['source_schema']}.t_omvo_voc vocabulary,
#     {validation['source_schema']}.{validation['source_table']} project
#     where project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#     and project."PJ_PJ_NAME" = 'RegCoR'
#     except
#     select distinct dm.vocabulary_name from  
#     {validation['target_schema']}.{validation['target_table']} dm
# """
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

#     print(f"\nIdentify there is Null values for vocabulary_name in {validation['target_table']} table.\n")
#     columns_to_check = ["vocabulary_name  "]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message 


# def test_TS_RDCC_129_TC_RDCC_132_concept_code_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case validates that the concept_code column data in the Master Table RDM DIM is correctly fetched from t_omco_cncpt by filtering PJ_PJ_SEQ with 'Project id'.\n""")
#     print(f"Test 1 : Identify concept_code in the dim_rdm_regcor_master_table table that are missing in the source table (t_omco_cncpt):\n")
#     query =f"""select distinct dm.concept_code from 
#             {validation['target_schema']}.{validation['target_table']} dm
#             except
#             select concept."CO_CO_CD" as source_concept_code 
#             from {validation['source_schema']}.t_omco_cncpt concept,
#             {validation['source_schema']}.t_omvo_voc vocabulary,
#             {validation['source_schema']}.{validation['source_table']} project
#             where
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ"
#             and vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             and project."PJ_PJ_NAME" = 'RegCoR'"""

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

#     query =f"""select concept."CO_CO_CD" as source_concept_code 
#             from {validation['source_schema']}.t_omco_cncpt concept,
#             {validation['source_schema']}.t_omvo_voc vocabulary,
#             {validation['source_schema']}.{validation['source_table']} project
#             where
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ"
#             and vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             and project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.concept_code from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

#     print(f"\nIdentify there is Null values for concept_code in {validation['target_table']} table.\n")
#     columns_to_check = ["concept_code"]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message 


# def test_TS_RDCC_129_TC_RDCC_133_concept_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case validates that the concept_name column data in the Master Table RDM DIM is correctly fetched from t_omco_cncpt by filtering PJ_PJ_SEQ with 'Project id'.\n""")
#     print(f"Test 1 : Identify concept_name in the dim_rdm_regcor_master_table table that are missing in the source table (t_omco_cncpt):\n")
#     query =f"""select distinct dm.concept_name from 
#             {validation['target_schema']}.{validation['target_table']} dm
#             except
#             select concept."CO_NAME" as concept_name 
#             from {validation['source_schema']}.t_omco_cncpt concept,
#             {validation['source_schema']}.t_omvo_voc vocabulary,
#             {validation['source_schema']}.{validation['source_table']} project
#             where
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ"
#             and vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             and project."PJ_PJ_NAME" = 'RegCoR'"""

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

#     query =f"""select concept."CO_NAME" as source_concept_code 
#             from {validation['source_schema']}.t_omco_cncpt concept,
#             {validation['source_schema']}.t_omvo_voc vocabulary,
#             {validation['source_schema']}.{validation['source_table']} project
#             where
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ"
#             and vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             and project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.concept_name from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

#     print(f"\nIdentify there is Null values for concept_name in {validation['target_table']} table.\n")
#     columns_to_check = ["concept_name"]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message 

# def test_TS_RDCC_129_TC_RDCC_134_property_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""his test case validates that the property_name column data in the Master Table RDM DIM is correctly fetched from t_ompr_prop by filtering PJ_PJ_SEQ with 'Project id'.\n""")
#     print(f"Test 1 : Identify property_name in the dim_rdm_regcor_master_table table that are missing in the source table (t_ompr_prop):\n")
#     query =f"""select distinct dm.property_name from  
#                 {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct property."PR_PR_NAME" as property_name from 
#                 {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'
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

#     query =f"""select distinct property."PR_PR_NAME" as property_name from 
#                 {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.property_name from  
#                 {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_135_property_value_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case validates that the property_value column data in the Master Table RDM DIM table is correctly fetched from t_omcp_cncpt_prop by filtering PJ_PJ_SEQ with 'Project id'.\n""")
#     print(f"Test 1 : Identify property_value in the dim_rdm_regcor_master_table table that are missing in the source table (t_omcp_cncpt_prop):\n")
#     query =f"""select distinct dm.property_value from {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct concept_property."CP_VAL" as property_value from 
#                  {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'
# """

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

#     query =f"""select distinct concept_property."CP_VAL" as property_value from 
#                  {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.property_value from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_136_Domain_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case validates that the domain column data in the Master Table RDM DIM table is correctly fetched from t_omdo_dmn by filtering PJ_PJ_SEQ with 'Project id'.\n""")
#     print(f"Test 1 : Identify domain in the dim_rdm_regcor_master_table table that are missing in the source table (t_omdo_dmn):\n")
#     query =f"""select distinct dm."domain" from  {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct domain."DO_DO_NAME" as domain_name 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'"""

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

#     query =f"""select distinct domain."DO_DO_NAME" as domain_name 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 where project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm."domain" from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_137_relation_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case ensures that the relation_name column data in the Master Table RDM DIM is accurately retrieved from t_omre_rel by filtering PJ_PJ_SEQ with the 'Project ID'.\n""")
#     print(f"Test 1 : Identify relation_name in the dim_rdm_regcor_master_table table that are missing in the source table (t_omre_rel)::\n")
#     query =f"""select distinct dm.relation_name from  
#                 {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct relation."RE_RE_NAME" as relation_name 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#                 cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omre_rel" relation on 
#                 relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#                 where project."PJ_PJ_NAME" = 'RegCoR'"""

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

#     print("\nIdentify source_relation_name in the source table that are missing in the dim_rdm_regcor_master_table table:")

#     query =f"""select distinct relation."RE_RE_NAME" as relation_name 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#                 cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omre_rel" relation on 
#                 relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#                 where project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.relation_name from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_138_relation_code_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case ensures that the relation_code column data in the Master Table RDM DIM is accurately retrieved from t_omre_rel by filtering PJ_PJ_SEQ with the 'Project ID'.\n""")
#     print(f"Test 1 : Identify relation_code in the dim_rdm_regcor_master_table table that are missing in the source table (t_omre_rel)::\n")
#     query =f"""select distinct dm.relation_code_to 
#             from {validation['target_schema']}.{validation['target_table']} dm
#             except
#             select distinct concept_to."CO_CO_CD" as relation_code_to 
#             from {validation['source_schema']}.{validation['source_table']} project 
#             join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#             join {validation['source_schema']}."t_omco_cncpt" concept on 
#             vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             join {validation['source_schema']}."t_omdo_dmn" domain on 
#             domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#             left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#             concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_ompr_prop" property on 
#             property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#             left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#             cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_omco_cncpt" concept_to on 
#             cocor."CR_TO_CO_SEQ" = concept_to."CO_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_omre_rel" relation on 
#             relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#             where project."PJ_PJ_NAME" = 'RegCoR'
# """

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

#     print("\nIdentify relation_code in the source table that are missing in the dim_rdm_regcor_master_table table:")

#     query =f"""select distinct concept_to."CO_CO_CD" as relation_code_to 
#             from {validation['source_schema']}.{validation['source_table']} project 
#             join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#             project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#             join {validation['source_schema']}."t_omco_cncpt" concept on 
#             vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#             join {validation['source_schema']}."t_omdo_dmn" domain on 
#             domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#             left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#             concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_ompr_prop" property on 
#             property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#             left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#             cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_omco_cncpt" concept_to on 
#             cocor."CR_TO_CO_SEQ" = concept_to."CO_CO_SEQ" 
#             left outer join {validation['source_schema']}."t_omre_rel" relation on 
#             relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#             where project."PJ_PJ_NAME" = 'RegCoR'
#             except
#             select distinct dm.relation_code_to from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_139_relation_value_to_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"""This test case ensures that the relation_value_to column data in the Master Table RDM DIM is accurately retrieved from t_omco_cncpt by filtering PJ_PJ_SEQ with the 'Project ID'.\n""")
#     print(f"Test 1 : Identify relation_value_to in the dim_rdm_regcor_master_table table that are missing in the source table (t_omco_cncpt):\n")
#     query =f"""select distinct dm.relation_value_to 
#                 from {validation['target_schema']}.{validation['target_table']} dm
#                 except
#                 select distinct concept_to."CO_NAME" as relation_value_to 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#                 cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omco_cncpt" concept_to on 
#                 cocor."CR_TO_CO_SEQ" = concept_to."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omre_rel" relation on 
#                 relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#                 where project."PJ_PJ_NAME" = 'RegCoR'
# """

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

#     print("\nIdentify source_relation_value_to in the source table that are missing in the dim_rdm_regcor_master_table table:")

#     query =f"""select distinct concept_to."CO_NAME" as relation_value_to 
#                 from {validation['source_schema']}.{validation['source_table']} project 
#                 join {validation['source_schema']}."t_omvo_voc" vocabulary on 
#                 project."PJ_PJ_SEQ" = vocabulary."VO_PJ_SEQ" 
#                 join {validation['source_schema']}."t_omco_cncpt" concept on 
#                 vocabulary."VO_VO_SEQ" = concept."CO_VO_SEQ" 
#                 join {validation['source_schema']}."t_omdo_dmn" domain on 
#                 domain."DO_DO_SEQ" = concept."CO_DO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omcp_cncpt_prop" concept_property on 
#                 concept."CO_CO_SEQ" = concept_property."CP_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_ompr_prop" property on 
#                 property."PR_PR_SEQ" = concept_property."CP_PR_SEQ"
#                 left outer join {validation['source_schema']}."t_omcr_cncpt_rel" cocor on 
#                 cocor."CR_FROM_CO_SEQ" = concept."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omco_cncpt" concept_to on 
#                 cocor."CR_TO_CO_SEQ" = concept_to."CO_CO_SEQ" 
#                 left outer join {validation['source_schema']}."t_omre_rel" relation on 
#                 relation."RE_RE_SEQ" = cocor."CR_RE_SEQ" 
#                 where project."PJ_PJ_NAME" = 'RegCoR'
#                 except
#             select distinct dm.relation_value_to from  
#             {validation['target_schema']}.{validation['target_table']} dm
# """
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

# def test_TS_RDCC_129_TC_RDCC_140_PK_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates the duplicates and null checks of the primary key columns in the DIM RDM Master Table.\n")
#     # -- Check for duplicates in primary keys 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['concept_code' ,'property_name','relation_name' ,'relation_value_to']
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


