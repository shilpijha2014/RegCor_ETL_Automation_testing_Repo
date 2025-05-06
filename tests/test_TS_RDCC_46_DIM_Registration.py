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
        "source_schema": "regcor_refine",
        "target_schema": "regcor_refine",
        "target_table": "stg_dim_regcor_registration",
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
    
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")
    
# Test Case - RDCC-47 - This Test case validates the Registration_id in dim_regcor_registration is correctly mapped with id in source registration table .
def test_TC_RDCC_47_1_registration_id_null_values(db_connection: connection | None,validation: dict[str, str]):
 
    cursor = db_connection.cursor()
    print("Test Case - RDCC-47 - This Test case validates the Registration_id in dim_regcor_registration is correctly mapped with id in source registration table .")
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['target_schema']}"."{validation['target_table']}" 
        WHERE "{validation['target_column']}" IS NULL;
    """
    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_47_2_3_registration_id_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table=validation["source_table"],
        src_key=validation["source_column"],
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key=validation["target_column"]
    )
    assert passed, message


def test_TS_46_TC_RDCC_48_1_registration_name_null_values(db_connection: connection | None,validation: dict[str, str]):
    print("Test Case - RDCC-48 - This Test case validates the Registration_name in dim_regcor_registration is correctly mapped with id in source registration table .")
    print("Checking if a column contains NULL values in a given table and schema.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"registration_name")

    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.registration_name"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.registration_name"
        f"contains NO NULL values!\n")

def test_TC_RDCC_48_2_and_3_registration_name_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    passed, missing_count, message = check_col_data_completeness(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation["source_table"],
        src_key="name__v",
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_name'
    )
    assert passed, message

def test_TC_RDCC_49_col_registration_start_date_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration start date in dim table  is fetched from source registration source table.")
    query = f"""SELECT r.registration_start_date__rim 
        FROM {validation['source_schema']}.{validation['source_table']} r
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} drr ON r.id = drr.registration_id
        WHERE drr.registration_id IS null  and r.clinical_study_number__v is null""" 
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TC_RDCC_49_col_registration_end_date_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration end date in dim table  is fetched from source registration source table.")
    query = f"""SELECT r.registration_end_date__rim 
        FROM {validation['source_schema']}.{validation['source_table']} r
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} drr ON r.id = drr.registration_id
        WHERE drr.registration_id IS null  and r.clinical_study_number__v is null""" 
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TC_RDCC_49_col_registration_status_date_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the status date in dim table  is fetched from source registration source table.")
    query = f"""SELECT r.registration_status_date__rim 
        FROM {validation['source_schema']}.{validation['source_table']} r
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} drr ON r.id = drr.registration_id
        WHERE drr.registration_id IS null  and r.clinical_study_number__v is null""" 
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TC_RDCC_49_col_registration_number_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("This Test case validates the Registration number in dim table  is fetched from source registration source table.")
    query = f"""SELECT r.registration_status_date__rim 
        FROM {validation['source_schema']}.{validation['source_table']} r
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} drr ON r.id = drr.registration_id
        WHERE drr.registration_id IS null  and r.clinical_study_number__v is null""" 
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)
# Commenting it as per now as Nulls would be allowed for the field
# def test_TS_RDCC_46_TC_RDCC_50_Product_family_name_null_check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nTC_RDCC-50 : This Test case validates the product_family in dim_regcor_registration table  is correctly mapped with Name__v from source product  table.")
#     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"product_family")
#     assert null_count == 0, (
#         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'product_family"
#         f" contains {null_count} NULL values!\n"
#     )
#     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'product_family"
#         f"contains NO NULL values!\n")


def test_TS_46_TC_RDCC_50_2_and_3_product_family_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    passed, missing_count, message = check_col_data_completeness_src_to_tgt(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="product",
        src_key='name__V',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='product_family'
    )
    passed, missing_count, message = check_col_data_completeness_tgt_to_src(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="product",
        src_key='name__V',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='product_family'
    )

    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_51_Application_id_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-51 : This Test case validates the  application_id in DIM table is correctly mapped  with id in application and not null.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"application_id")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'application_id'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_id'"
        f"contains NO NULL values!\n")

def test_TS_46_TC_RDCC_51_2_and_3_Application_id_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="Application",
        src_key='id',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='application_id'
    )
    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_52_Application_name_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-52 : This Test case validates the application_name in DIM table is correctly mapped  with id in application and not null.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"application_name")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_name'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_name'"
        f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_52_2_and_3_Application_name_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="Application",
        src_key='name__v',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='application_name'
    )
    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_53_country_code_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-53 : This Test case validates the country_code  in DIM table is correctly fetched from country_code_rim in country table.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code'"
        f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_53_2_and_3_country_code_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="country",
        src_key='country_code__rim',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='country_code'
    )
    assert passed, message
    print(message)

# # Commenting it as per now as Nulls would be allowed for the field
# # def test_TS_RDCC_46_TC_RDCC_54_Drug_Product_name_null_check(db_connection: connection | None,validation: dict[str, str]):
# #     print("\nTC_RDCC-54 : This Test case validates the drug_product_name in DIM table is correctly mapped with name__v in drug_product and not null.")
# #     null_count = check_null_values(db_connection,validation["schema"],validation["target_table"],"drug_product_name")
# #     assert null_count == 0, (
# #         f"\n❌ {validation['db']}.{validation['schema']}.{validation['target_table']}.'drug_product_name'"
# #         f" contains {null_count} NULL values!\n"
# #     )
# #     print(f"\n{validation['db']}.{validation['schema']}.{validation['target_table']}.'drug_product_name'"
# #         f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_54_2_and_3_drug_product_name_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="drug_product",
        src_key='name__v',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='drug_product_name'
    )
    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_55_item_code__c_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-55 : This Test case validates the family_item_code in  DIM registration is correctly mapped with item_code__c in source product_Detail table .")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"family_item_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'family_item_code'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'family_item_code'"
        f"contains NO NULL values!\n")
    
def test_TS_46_TC_RDCC_55_2_and_3_family_item_code_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness_src_to_tgt(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="product_detail",
        src_key='item_code__c',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='family_item_code'
    )
    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_56_registration_state__c_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-56 : This Test case validates the registration_state in dim_regcor_registration table  is correctly mapped with lifecycle_state__V from objectlifecyclestate source table.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"family_item_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_state'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_state'"
        f"contains NO NULL values!\n")

def test_TS_46_TC_RDCC_56_2_and_3_registration_state_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    conn = get_connection(validation["db"])
    passed, missing_count, message = check_col_data_completeness(
        db_connection,
        src_schema=validation["source_schema"],
        src_table="objectlifecyclestate_ref",
        src_key='lifecyclestate_label',
        tgt_schema=validation["target_schema"],
        tgt_table=validation["target_table"],
        tgt_key='registration_state'
    )
    assert passed, message
    print(message)

def test_TS_RDCC_46_TC_RDCC_57_primay_key_validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Duplicates,Null checks and refrential integrity of Primary key columns registration_id ,application_id,country_code,family_item_Code in DIM table and with source tables.\n")
    
    # -- Check for duplicates in registration_id 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['registration_id','application_id', 'country_code', 'family_item_code ']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in customers table for keys {primary_keys}!"


    print(f"\n2. Check for Null Values:")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"registration_id")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_id' "
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_id' "
        f"contains NO NULL values!\n")

    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"application_id")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_id' "
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_id' "
        f"contains NO NULL values!\n")

    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code' "
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code' "
        f"contains NO NULL values!\n")

# def test_TS_RDCC_46_TC_RDCC_128_No_source_value_transformation(db_connection: connection | None,validation: dict[str, str]): TBD
#     # This test case validates the coulmns in dim registration is being transformed as no_source_values in case of null present in source table.
