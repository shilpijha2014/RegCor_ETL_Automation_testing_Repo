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
#         "source_table": "country",   
#         "target_db": "regcor_refine_db" ,
#         "target_schema": "regcor_refine",        
#         "target_table": "stg_dim_regcor_country",
#     }

# # Test Case - RDCC-58 - This Test set contains test cases for Dim Country table.
# def test_TS_RDCC_58_TC_RDCC_59_country_code_check(db_connection: connection | None,validation: dict[str, str]):
 
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 58 - This Test set contains test cases for Dim Country table.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

   
#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")
    
#     print(f"\nTest Case - RDCC-59 - This Test case validates the Country_code in  dim_regcor_country is correctly fetched with country_code_rim,cnrty_code in source country and ref_mcsad_mcs_cntry with active status__v.\n")
#     print("Check for null values\n")
#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains NO NULL values!\n")
    
#     print(f"\nIdentify country_code values present in the source  but missing in dim_regcor_country.\n")
#     query =f"""SELECT a.country_code
#                 FROM (
#                     SELECT COALESCE(r.country_code__rim, g.cntry_cd) AS country_code
#                     FROM {validation['source_schema']}.{validation['source_table']} r
#                     FULL OUTER join {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                     ON r.country_code__rim = g.cntry_cd
#                     WHERE r.status__v::text != '{{inactive__v}}'
#                 ) a
#                 WHERE a.country_code IS NOT null and a.country_code not in ('EU','EDQ')
#                 EXCEPT
#                 SELECT country_code
#                 FROM {validation['target_schema']}.{validation['target_table']}
#                             """
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

#     query =f"""SELECT country_code
#                 FROM {validation['target_schema']}.{validation['target_table']}
#             except
#             SELECT a.country_code
#                 FROM (
#                     SELECT COALESCE(r.country_code__rim, g.cntry_cd) AS country_code
#                     FROM {validation['source_schema']}.{validation['source_table']} r
#                     FULL OUTER join {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                     ON r.country_code__rim = g.cntry_cd
#                     WHERE r.status__v::text != '{{inactive__v}}'
#                 ) a
#                 WHERE a.country_code IS NOT null and a.country_code not in ('EU','EDQ')"""
    
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


# def test_TS_RDCC_58_TC_RDCC_60_country_name_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-60 - This Test case validates the Country_name in  dim_regcor_country is correctly fetched with name__V in source country table with active status__v.\n")

#     query = f"""SELECT a.country_code,a.name__V
#             FROM (
#                 SELECT COALESCE(r.country_code__rim, g.cntry_cd) AS country_code,
#                 name__v
#                 FROM {validation['source_schema']}.{validation['source_table']} r
#                 FULL OUTER join {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                 ON r.country_code__rim = g.cntry_cd
#                 WHERE r.status__v::text != '{{inactive__v}}'
#             ) a
#             WHERE a.country_code IS NOT null and a.country_code not in ('EU','EDQ')
#             EXCEPT
#             SELECT country_code,country_name
#             FROM {validation['target_schema']}.{validation['target_table']};"""
    
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

#     query = f"""SELECT country_code,country_name
#                 FROM {validation['target_schema']}.{validation['target_table']}
#                 Except
#                 SELECT a.country_code,a.name__v
#                 FROM (
#                     SELECT COALESCE(r.country_code__rim, g.cntry_cd) AS country_code,
#                     name__v
#                     FROM {validation['source_schema']}.{validation['source_table']} r
#                     FULL OUTER join {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                     ON r.country_code__rim = g.cntry_cd
#                     WHERE r.status__v::text != '{{inactive__v}}'
#                 ) a
#         """ 
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


# def test_TS_RDCC_58_TC_RDCC_61_country_flag_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-61 - This Test case validates EU_Country_flag in DIM country table as it sets to True if country is European country.\n")

#     query = f"""SELECT 
#     DISTINCT a.country_code,
#     a.country_name
#         FROM 
#             (
#                 SELECT 
#                     COALESCE(r.country_code__rim, g.cntry_cd) AS country_code, 
#                     r.name__v AS country_name, 
#                     r.region__v 
#                 FROM 
#                     {validation['source_schema']}.{validation['source_table']} r 
#                 FULL OUTER JOIN 
#                     {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                 ON 
#                     r.country_code__rim = g.cntry_cd 
#                 WHERE 
#                     r.status__v = '{{active__v}}'
#             ) a 
#         LEFT OUTER JOIN 
#             {validation['source_schema']}.region b 
#         ON 
#             a.region__v = b.id 
#         WHERE 
#             a.country_code IS NOT NULL 
#             and b.name__v = 'European Union' and a.country_name not in ('European Union','European Directorate for the Quality of Medicines')
#         EXCEPT
#         SELECT DISTINCT country_code,
#             country_name 
#         FROM 
#             {validation['target_schema']}.{validation['target_table']} 
#         WHERE 
#             eu_country_flag = true;
# """
    
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "European Flag Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ EU Flag check passed: All EU countries in {validation['target_table']} are getting marked correctly ."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ EU Flag check Failed: All EU countries in {validation['target_table']} are not getting marked correctly ."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during EU flag validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     query = f"""SELECT DISTINCT country_code,
#             country_name 
#             FROM 
#                 {validation['target_schema']}.{validation['target_table']} 
#             WHERE 
#                 eu_country_flag = true
#                 except
#                 SELECT 
#                     DISTINCT a.country_code,
#                     a.country_name
#                         FROM 
#                             (
#                                 SELECT 
#                                     COALESCE(r.country_code__rim, g.cntry_cd) AS country_code, 
#                                     r.name__v AS country_name, 
#                                     r.region__v 
#                                 FROM 
#                                     {validation['source_schema']}.{validation['source_table']} r 
#                                 FULL OUTER JOIN 
#                                     {validation['source_schema']}.ref_mcsad_mcs_cntry g 
#                                 ON 
#                                     r.country_code__rim = g.cntry_cd 
#                                 WHERE 
#                                     r.status__v = '{{active__v}}'
#                             ) a 
#                         LEFT OUTER JOIN 
#                             {validation['source_schema']}.region b 
#                         ON 
#                             a.region__v = b.id 
#                         WHERE 
#                             a.country_code IS NOT NULL 
#                             and b.name__v = 'European Union' and a.country_name not in 
#                             ('European Union','European Directorate for the Quality of Medicines')
#                 """ 
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ EU Flag check passed: All EU countries in {validation['target_table']} are getting marked correctly ."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ EU Flag check Failed: All EU countries in {validation['target_table']} are not getting marked correctly ."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during  EU Flag check: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

    
# def test_TS_RDCC_58_TC_RDCC_62_Primary_key_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-62 - This Test case validates the Duplicates,Null checks  of Primary key column country_code in DIM table and with source tables.\n")
#     # -- Check for duplicates in country_code 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['country_code']
#     result = check_primary_key_duplicates(
#     connection=db_connection,
#     schema_name=validation['target_schema'],
#     table_name=validation["target_table"],
#     primary_keys=primary_keys)
#     assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
#     print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains NO NULL values!\n")
    
# def test_TS_RDCC_58_TC_RDCC_63_filter_condition_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-63 - This Test case validates the Filter condition on active status to fetch country details from source table.\n")

#     query = f"""SELECT 
#     drc.country_code, 
#     drc.country_name 
#     FROM 
#         {validation['target_schema']}.{validation['target_table']} drc 
#     EXCEPT
#     SELECT 
#         c.country_code__rim, 
#         c.Name__v 
#     FROM 
#         {validation['source_schema']}.{validation['source_table']} c 
#     WHERE 
#     c.status__v::text = '{{active__v}}';
#         """ 
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

#     query = f"""SELECT 
#     c.country_code__rim, 
#     c.Name__v 
#     FROM 
#         {validation['source_schema']}.{validation['source_table']} c 
#     WHERE 
#         c.status__v::text = '{{active__v}}' and c.id is  null 
#         and name__v not in('European Union','European Directorate for the Quality of Medicines')
#     EXCEPT
#     SELECT 
#         drc.country_code, 
#         drc.country_name 
#     FROM 
#     {validation['target_schema']}.{validation['target_table']} drc""" 
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

     

