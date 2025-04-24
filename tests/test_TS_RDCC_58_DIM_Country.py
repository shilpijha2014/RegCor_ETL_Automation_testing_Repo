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
#         "source_table": "",   
#         "target_db": "regcor_refine_db" ,
#         "target_schema": "regcor_refine",        
#         "target_table": "dim_regcor_country",
#     }

# def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 58 - This Test set contains test cases for Dim Country table.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

# def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")
    
# # Test Case - RDCC-58 - This Test set contains test cases for Dim Country table.
# def test_TS_RDCC_58_TC_RDCC_59_country_code_null_value_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-59 - This Test case validates the Country_code in  dim_regcor_country is correctly fetched with country_code_rim,cnrty_code in source country and ref_mcsad_mcs_cntry with active status__v.\n")
#     print(f"\nIdentify country_code values present in the source  but missing in dim_regcor_country.\n")

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains NO NULL values!\n")
    
# # def test_TS_RDCC_58_TC_RDCC_59_country_code_data_completeness(db_connection: connection | None,validation: dict[str, str]):
# #     cursor = db_connection.cursor()
# #     success, count, message = check_data_completeness_with_full_join(
# #     connection=db_connection,
# #     src_schema1=validation['source_schema'],
# #     src_table1="country",
# #     src_col1="country_code__rim",
# #     src_schema2=validation['source_schema'],
# #     src_table2="ref_mcsad_mcs_cntry",
# #     src_col2="cntry_cd",
# #     tgt_schema=validation['target_schema'],
# #     tgt_table="dim_regcor_country",
# #     tgt_col="country_code",
# #     join_condition=f"r.status__v::text != '{{inactive__v}}'"
# #     )

# #     assert success, message


# def test_TS_RDCC_58_TC_RDCC_60_country_name_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-60 - This Test case validates the Country_name in  dim_regcor_country is correctly fetched with name__V in source country table with active status__v.\n")

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_name")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_name"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_name"
#         f" contains NO NULL values!\n")

#     success, count, msg = validate_source_to_target_with_filter(
#         connection=db_connection,
#         src_schema=validation['source_schema'],
#         src_table="country",
#         tgt_schema=validation['target_schema'],
#         tgt_table=validation['target_table'],
#         src_cols=['name__v'],
#         tgt_cols=['country_name'],
#         src_filter=f"status__v::text != '{{inactive__v}}'",
#         tgt_filter=""
#     )

#     print(msg)
#     assert success, msg

#     success, count, msg = validate_target_to_source_with_filter(
#         connection=db_connection,
#         src_schema=validation['source_schema'],
#         src_table="country",
#         tgt_schema=validation['target_schema'],
#         tgt_table=validation['target_table'],
#         src_cols=['name__v'],
#         tgt_cols=['country_name'],
#         src_filter=f"status__v::text != '{{inactive__v}}'",
#         tgt_filter=""
#     )

#     assert success, msg

#     result, count, msg = validate_target_to_source_with_filter(
#     connection=db_connection,
#     src_schema=validation['source_schema'],
#     src_table="country",
#     tgt_schema=validation["target_schema"],
#     tgt_table="dim_regcor_country",
#     src_cols=['name__v'],
#     tgt_cols=['country_name'],
#     src_filter=f"status__v::text != '{{inactive__v}}'",
#     tgt_filter=""
#     )
#     print(msg)
#     assert result, msg

# def test_TS_RDCC_58_TC_RDCC_61_country_flag_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-61 - This Test case validates EU_Country_flag in DIM country table as it sets to True if country is European country.\n")

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_name")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_name"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_name"
#         f" contains NO NULL values!\n")
    
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
#     assert result, f"❌ Duplicate values found in customers table for keys {primary_keys}!"

#     null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")

#     assert null_count == 0, (
#         f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.country_code"
#         f" contains NO NULL values!\n")
    
# def test_TS_RDCC_58_TC_RDCC_62_filter_condition_check(db_connection: connection | None,validation: dict[str, str]):
 
#     print(f"\nTest Case - RDCC-62 - This Test case validates the Filter condition on active status to fetch country details from source table.\n")

#     result, count, msg = validate_target_to_source_with_filter(
#     connection=db_connection,
#     src_schema=validation['source_schema'],
#     src_table="country",
#     tgt_schema=validation["target_schema"],
#     tgt_table="dim_regcor_country",
#     src_cols=['country_code__rim','name__v'],
#     tgt_cols=['country_code','country_name'],
#     src_filter=f"status__v::text != '{{inactive__v}}'",
#     tgt_filter=""
#     )
#     print(msg)
#     assert result, msg

#     result, count, msg = validate_source_to_target_with_filter(
#     connection=db_connection,
#     src_schema=validation['source_schema'],
#     src_table="country",
#     tgt_schema=validation["target_schema"],
#     tgt_table="dim_regcor_country",
#     src_cols=['country_code__rim','name__v'],
#     tgt_cols=['country_code','country_name'],
#     src_filter=f"status__v::text != '{{inactive__v}}'",
#     tgt_filter=""
#     )
#     print(msg)
#     assert result, msg

















     

