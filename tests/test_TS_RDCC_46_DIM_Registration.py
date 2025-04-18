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
        "db": "regcor_refine_db",
        "schema": "regcor_refine",
        "target_table": "dim_regcor_registration",
        "source_table": "registration",
        "target_column": "registration_id",
        "source_column": "id"
    }

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC-46 - This Test case validates the Registration start,end,status date,Registration number in dim table is fetched from source registration source table.")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['db']}"
        print(f"✅ Successfully connected to database: {validation['db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    assert validate_table_exists( db_connection,validation["schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")
    
# Test Case - RDCC-47 - This Test case validates the Registration_id in dim_regcor_registration is correctly mapped with id in source registration table .
def test_TC_RDCC_47_1_regitration_id_null_values(db_connection: connection | None,validation: dict[str, str]):
    
    cursor = db_connection.cursor()
    print("Test Case - RDCC-47 - This Test case validates the Registration_id in dim_regcor_registration is correctly mapped with id in source registration table .")
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['schema']}"."{validation['target_table']}" 
        WHERE "{validation['target_column']}" IS NULL;
    """
    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_47_2_3_registration_id_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key=validation["source_column"],
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key=validation["target_column"]
    )
    assert passed, message
    print(message)

def test_TS_46_TC_RDCC_48_1_registration_name_null_values(db_connection: connection | None,validation: dict[str, str]):
    print("Test Case - RDCC-48 - This Test case validates the Registration_name in dim_regcor_registration is correctly mapped with id in source registration table .")
    print("Checking if a column contains NULL values in a given table and schema.")
    null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"registration_name")

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.registration_name"
        f"contains NO NULL values!\n")

def test_TC_RDCC_48_2_and_3_registration_name_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    passed, missing_count, message = check_col_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="name__v",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_name'
    )
    assert passed, message

def test_TC_RDCC_49_col_key1_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration start,end,status date,Registration number in dim table  is fetched from source registration source table.")
    passed, missing_count, message = check_col_key_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="id",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_id',
        src_col='registration_start_date__rim',
        tgt_col = 'registration_start_date'
    )
    assert passed, message

def test_TC_RDCC_49_col_key2_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration start,end,status date,Registration number in dim table  is fetched from source registration source table.")
    passed, missing_count, message = check_col_key_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="id",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_id',
        src_col='registration_end_date__rim',
        tgt_col = 'registration_end_date'
    )
    assert passed, message

def test_TC_RDCC_49_col_key3_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration start,end,status date,Registration number in dim table  is fetched from source registration source table.")
    passed, missing_count, message = check_col_key_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="id",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_id',
        src_col='registration_status_date__rim',
        tgt_col = 'registration_status_date'
    )
    assert passed, message

def test_TC_RDCC_49_col_key4_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration start,end,status date,Registration number in dim table  is fetched from source registration source table.")
    passed, missing_count, message = check_col_key_data_completeness(
        connection=db_connection,
        src_schema=validation["schema"],
        src_table=validation["source_table"],
        src_key="id",
        tgt_schema=validation["schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_id',
        src_col='registration_number__rim',        
        tgt_col = 'registration_number'
    )
    assert passed, message

# def test_TS_RDCC_46_TC_RDCC_50_Product_family_name_null_check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nTC_RDCC-50 : This Test case validates the product_family in dim_regcor_registration table  is correctly mapped with Name__v from source product  table.")
#     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"product_family")
#     assert null_count == 0, (
#         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'product_family"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'product_family"
#         f"contains NO NULL values!\n")


# def test_TS_46_TC_RDCC_50_2_and_3_product_family_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
#     conn = get_connection(validation["db"])
#     passed, missing_count, message = check_col_data_completeness(
#         db_connection,
#         src_schema=validation["schema"],
#         src_table="product",
#         src_key='name__V',
#         tgt_schema=validation["schema"],
#         tgt_table=validation["target_table"],
#         tgt_key='product_family'
#     )
#     assert passed, message
#     print(message)

# def test_TS_RDCC_46_TC_RDCC_51_Application_id_null_check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nTC_RDCC-51 : This Test case validates the  application_id in DIM table is correctly mapped  with id in application and not null.")
#     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"application_id")
#     assert null_count == 0, (
#         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'application_id'"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'application_id'"
#         f"contains NO NULL values!\n")

# def test_TS_46_TC_RDCC_51_2_and_3_Application_id_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
#     conn = get_connection(validation["db"])
#     passed, missing_count, message = check_col_data_completeness(
#         db_connection,
#         src_schema=validation["schema"],
#         src_table="Application",
#         src_key='id',
#         tgt_schema=validation["schema"],
#         tgt_table=validation["target_table"],
#         tgt_key='application_id'
#     )
#     assert passed, message
#     print(message)

# def test_TS_RDCC_46_TC_RDCC_52_Application_name_null_check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nTC_RDCC-52 : This Test case validates the application_name in DIM table is correctly mapped  with id in application and not null.")
#     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"application_name")
#     assert null_count == 0, (
#         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'application_name'"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'application_name'"
#         f"contains NO NULL values!\n")
    
# def test_TS_46_TC_RDCC_52_2_and_3_Application_name_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
#     conn = get_connection(validation["db"])
#     passed, missing_count, message = check_col_data_completeness(
#         db_connection,
#         src_schema=validation["schema"],
#         src_table="Application",
#         src_key='name__v',
#         tgt_schema=validation["schema"],
#         tgt_table=validation["target_table"],
#         tgt_key='application_name'
#     )
#     assert passed, message
#     print(message)

# def test_TS_RDCC_46_TC_RDCC_53_country_code_null_check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nTC_RDCC-53 : This Test case validates the country_code  in DIM table is correctly fetched from country_code_rim in country table.")
#     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"country_code")
#     assert null_count == 0, (
#         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'country_code'"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'country_code'"
#         f"contains NO NULL values!\n")
    
# def test_TS_46_TC_RDCC_53_2_and_3_country_code_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
#     conn = get_connection(validation["db"])
#     passed, missing_count, message = check_col_data_completeness(
#         db_connection,
#         src_schema=validation["schema"],
#         src_table="country",
#         src_key='country_code__rim',
#         tgt_schema=validation["schema"],
#         tgt_table=validation["target_table"],
#         tgt_key='country_code'
#     )
#     assert passed, message
#     print(message)



    





