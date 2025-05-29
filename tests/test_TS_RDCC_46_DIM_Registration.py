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

    query =f"""SELECT  f.registration_id,f.registration_name
            FROM {validation['target_schema']}.{validation['target_table']} f
            except
            select r.id,r.name__v from {validation['source_schema']}.{validation['source_table']} r 
            where r.clinical_study_number__v is null
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

    print(f"\n2)Identify records in the source table that are missing in the dim_regcor_container_closure_system table:")

    query =f"""SELECT  f.registration_id,f.registration_name
            FROM {validation['target_schema']}.{validation['target_table']} f
            except
            SELECT  f.registration_id,f.registration_name
            FROM {validation['target_schema']}.{validation['target_table']} f"""
    
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

def test_TC_RDCC_49_Reg_start_end_status_date_val(db_connection: connection | None,validation: dict[str, str]):
    print("This test case ensures that the registration_start_date,registration_end_date, registration_status_date , and registration_number  in the dim_regcor_registration table are correctly fetched from the source registration table.\n")
    print("Identify registration start date,end_date,status_date,registration_number table that are missing in the source table (registration): '.")
    query =f"""SELECT drr.registration_id, drr.registration_start_date ,drr.registration_end_date,drr.registration_status_date,drr.registration_number
                FROM {validation['target_schema']}.{validation['target_table']} drr
                except 
                select r.id,r.registration_start_date__rim,r.registration_end_date__rim,r.registration_status_date__rim,r.registration_number__rim 
                from {validation['source_schema']}.{validation['source_table']} r where r.clinical_study_number__v is null
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

    print(f"\n2) Identify registration start date,end_date,status_date,registration_number in the source table that are missing in the target table (dim_regcor_registration):")

    query =f"""select r.id,r.registration_start_date__rim,r.registration_end_date__rim,r.registration_status_date__rim,r.registration_number__rim 
                from {validation['source_schema']}.{validation['source_table']} r 
                where r.clinical_study_number__v is null
                except 
                SELECT drr.registration_id, drr.registration_start_date ,drr.registration_end_date,drr.registration_status_date,drr.registration_number
                FROM {validation['target_schema']}.{validation['target_table']} drr"""
    
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

def test_TS_46_TC_RDCC_50_product_family_col_data_completeness(db_connection: connection | None,validation: dict[str, str]):

    print("This test case ensures that the product_family in the dim_regcor_registration table is accurately mapped to the Name__v from the source product table.\n")
    print("Identify product_family in the dim_regcor_registration table that are missing in the source table (product): '.")
    query =f"""SELECT f.registration_id, f.product_family
                FROM {validation['target_schema']}.{validation['target_table']} f 
                Except
                select r.id, coalesce(p.name__v, 
                'No_Source_Value' ) as product_family 
                from 
                {validation['source_schema']}.{validation['source_table']} r 
                left join regcor_refine.product p on 
                p.id = r.product_family__v 
                and r.clinical_study_number__v is null
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

    print(f"\n2) Identify product_family in the source table that are missing in the dim_regcor_registration table:")
    query =f"""select r.id, coalesce(p.name__v, 
                'No_Source_Value' ) as product_family 
                from 
                {validation['source_schema']}.{validation['source_table']} r 
                left join {validation['source_schema']}.product p on 
                p.id = r.product_family__v 
                where r.clinical_study_number__v is null
                except
                SELECT f.registration_id, f.product_family
                FROM {validation['target_schema']}.{validation['target_table']} f"""
    
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

    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"product_family")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'product_family'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'product_family'"
        f"contains NO NULL values!\n")

def test_TS_RDCC_46_TC_RDCC_51_Application_id_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-51 : This Test case validates the  application_id in DIM table is correctly mapped  with id in application and not null.")

    print("Identify application_id in the dim_regcor_registration table that are missing in the source table (product): '.")
    query =f"""SELECT DISTINCT f.application_id
                FROM {validation['target_schema']}.{validation['target_table']} f 
                where f.application_id !='No_Source_Value'
                Except
                Select a.id 
                from {validation['source_schema']}.application a ,
                {validation['source_schema']}.{validation['source_table']} r  
                where a.id = r.application__rim
                and r.clinical_study_number__v is null
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

    print(f"\n2) Identify application_id in the source table that are missing in the dim_regcor_registration table:")
    query =f""" Select a.id 
                from {validation['source_schema']}.application a ,
                {validation['source_schema']}.{validation['source_table']} r  
                where a.id = r.application__rim
                and r.clinical_study_number__v is null
                except
                SELECT f.application_id
                FROM {validation['target_schema']}.{validation['target_table']} f"""
    
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

    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"application_id")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_id'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'application_id'"
        f"contains NO NULL values!\n")

def test_TS_RDCC_46_TC_RDCC_52_Application_name_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-52 : This Test case validates the application_name in DIM table is correctly mapped  with id in application and not null.")
    print("Identify application_name in the dim_regcor_registration table that are missing in the source table (product): '.")
    query =f"""SELECT DISTINCT f.application_id,application_name
                FROM {validation['target_schema']}.{validation['target_table']} f 
                where f.application_id !='No_Source_Value'
                Except
                Select a.id,a.name__v 
                from {validation['source_schema']}.application a ,
                {validation['source_schema']}.{validation['source_table']} r  
                where a.id = r.application__rim
                and r.clinical_study_number__v is null
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

    print(f"\n2) Identify application_id in the source table that are missing in the dim_regcor_registration table:")
    query =f""" Select a.id,a.name__v 
                from {validation['source_schema']}.application a ,
                {validation['source_schema']}.{validation['source_table']} r  
                where a.id = r.application__rim
                and r.clinical_study_number__v is null
                except
                SELECT f.application_id, application_name
                FROM {validation['target_schema']}.{validation['target_table']} f"""
    
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

def test_TS_RDCC_46_TC_RDCC_53_country_code_null_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-53 : This Test case validates the country_code in DIM table is correctly fetched from country_code_rim in country table.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"country_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'country_code'"
        f"contains NO NULL values!\n")
    
    query =f"""SELECT f.registration_id, f.country_code
                FROM {validation['target_schema']}.{validation['target_table']} f 
                where f.country_code !='No_Source_Value'
                Except
                Select r.id,c.country_code__rim 
                from {validation['source_schema']}.country c ,
                {validation['source_schema']}.{validation['source_table']} r  
                where r.country__rim = c.id
                and r.clinical_study_number__v is null
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

    print(f"\n2) Identify country_code_rim in the source table that are missing in the dim_regcor_registration table:")
    query =f""" select r.id,coalesce( c.country_code__rim, 
                'No_Source_Value' ) as country_code
                from {validation['source_schema']}.country c ,{validation['source_schema']}.{validation['source_table']} r 
                where r.country__rim = c.id
                and r.clinical_study_number__v is null
                except
                SELECT f.registration_id, f.country_code
                FROM {validation['target_schema']}.{validation['target_table']} f
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
    
def test_TS_46_TC_RDCC_54_drug_product_name_col_validation(db_connection: connection | None,validation: dict[str, str]):
    print("This test case ensures that the drug_product_name in the DIM_Registration table is correctly mapped to the name__v in the drug_product table and allows null values.\n")
    query =f"""SELECT f.registration_id, f.drug_product_name
                FROM {validation['target_schema']}.{validation['target_table']} f 
                Except
                Select r.id,dp.name__V
                from {validation['source_schema']}.{validation['source_table']} r  
                left join {validation['source_schema']}.drug_product dp on 
                dp.id = r.pharmaceutical_product__rim 
                and r.clinical_study_number__v is null
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

    print(f"\n2) Identify country_code_rim in the source table that are missing in the dim_regcor_registration table:")
    query =f""" Select r.id,dp.name__V
                from {validation['source_schema']}.{validation['source_table']} r  
                left join {validation['source_schema']}.drug_product dp on 
                dp.id = r.pharmaceutical_product__rim 
                where r.clinical_study_number__v is null
                except
                SELECT f.registration_id, f.drug_product_name
                FROM {validation['target_schema']}.{validation['target_table']} f 
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

def test_TS_RDCC_46_TC_RDCC_55_item_code_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-55 : This Test case validates the family_item_code in  DIM registration is correctly mapped with item_code__c in source product_Detail table .")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"family_item_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'family_item_code'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'family_item_code'"
        f"contains NO NULL values!\n")
    
    query =f"""SELECT distinct f.registration_id, f.family_item_code
                FROM {validation['target_schema']}.{validation['target_table']} f
                except
                SELECT coalesce(r.id, 
                'No_Source_Value') as registration_id,
                case 
                when coalesce( pd.item_code__c, 
                complex_dp.family_item_code ) is null then 'No_Source_Value' 
                else coalesce( pd.item_code__c, 
                complex_dp.family_item_code )
                end as family_item_code
                FROM {validation['source_schema']}.{validation['source_table']} r 
                left join {validation['source_schema']}.product p on 
                p.id = r.product_family__v 
                left join {validation['source_schema']}.application a on 
                r.application__rim = a.id 
                left join {validation['source_schema']}.country c on 
                r.country__rim = c.id 
                left join {validation['source_schema']}.drug_product dp on 
                dp.id = r.pharmaceutical_product__rim 
                left join ( 
                select 
                pc.name__v as drug_product, 
                dpc.id as drug_product_id, 
                pdc.name__v as product_variant_name, 
                pdc.item_code__c as family_item_code 
                from 
                {validation['source_schema']}.product_component pc 
                join {validation['source_schema']}.drug_product dpc on 
                pc.complex_product__rim = dpc.id 
                join {validation['source_schema']}.product_detail pdc on 
                pc.product_detail__rim = pdc.id) as complex_dp on 
                complex_dp.drug_product_id = r.pharmaceutical_product__rim 
                left join {validation['source_schema']}.product_detail pd on 
                r.presentation__rim = pd.id 
                left join {validation['source_schema']}.objectlifecyclestate_ref r_lc on 
                r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v 
                where r.clinical_study_number__v is null  
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

    print(f"\n2) Identify family_item_code in the source table that are missing in the dim_regcor_registration table:")
    query =f""" SELECT coalesce(r.id, 
            'No_Source_Value') as registration_id,
            case 
            when coalesce( pd.item_code__c, 
            complex_dp.family_item_code ) is null then 'No_Source_Value' 
            else coalesce( pd.item_code__c, 
            complex_dp.family_item_code )
            end as family_item_code
            FROM {validation['source_schema']}.{validation['source_table']} r 
            left join {validation['source_schema']}.product p on 
            p.id = r.product_family__v 
            left join {validation['source_schema']}.application a on 
            r.application__rim = a.id 
            left join {validation['source_schema']}.country c on 
            r.country__rim = c.id 
            left join {validation['source_schema']}.drug_product dp on 
            dp.id = r.pharmaceutical_product__rim 
            left join ( 
            select 
            pc.name__v as drug_product, 
            dpc.id as drug_product_id, 
            pdc.name__v as product_variant_name, 
            pdc.item_code__c as family_item_code 
            from 
            {validation['source_schema']}.product_component pc 
            join {validation['source_schema']}.drug_product dpc on 
            pc.complex_product__rim = dpc.id 
            join {validation['source_schema']}.product_detail pdc on 
            pc.product_detail__rim = pdc.id) as complex_dp on 
            complex_dp.drug_product_id = r.pharmaceutical_product__rim 
            left join {validation['source_schema']}.product_detail pd on 
            r.presentation__rim = pd.id 
            left join {validation['source_schema']}.objectlifecyclestate_ref r_lc on 
            r_lc.objectlifecycle_name = r.lifecycle__v 
            and r_lc.objectlifecyclestate_name = r.state__v 
            where	r.clinical_study_number__v is null 
            except
            SELECT  distinct f.registration_id, f.family_item_code
            FROM {validation['target_schema']}.{validation['target_table']} f
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

def test_TS_RDCC_46_TC_RDCC_56_registration_state_check(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC-56 : This Test case validates the registration_state in dim_regcor_registration table  is correctly mapped with lifecycle_state__V from objectlifecyclestate source table.")
    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"family_item_code")
    assert null_count == 0, (
        f"\n❌ {validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_state'"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['db']}.{validation['target_schema']}.{validation['target_table']}.'registration_state'"
        f"contains NO NULL values!\n")

    print("\nIdentify registration_state in the dim_regcor_registration table that are missing in the source table (objectlifecyclestate): '.")
    query =f"""SELECT f.registration_id, f.registration_state
                FROM {validation['target_schema']}.{validation['target_table']} f 
                Except
                Select r.id,r_lc.lifecyclestate_label
                from {validation['source_schema']}.{validation['source_table']} r  
                left join {validation['source_schema']}.objectlifecyclestate_ref r_lc on 
                r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v 
                where	r.clinical_study_number__v is null"""
    
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

    print(f"\n2) Identify lifecyclestate_label in the source table that are missing in the dim_regcor_registration table:")
    query =f""" Select r.id,r_lc.lifecyclestate_label
                from {validation['source_schema']}.{validation['source_table']} r  
                left join {validation['source_schema']}.objectlifecyclestate_ref r_lc on 
                r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v 
                where	r.clinical_study_number__v is null
                except
                SELECT f.registration_id, f.registration_state
                FROM {validation['target_schema']}.{validation['target_table']} f 
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
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

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

def test_TS_RDCC_46_TC_RDCC_128_No_source_value_transformation(db_connection: connection | None,validation: dict[str, str]): 
    # This test case validates the coulmns in dim registration is being transformed as no_source_values in case of null present in source table.
    query =f"""WITH source_data AS (Select distinct 
        coalesce(r.id, 
        'No_Source_Value') as registration_id,
        coalesce(r.name__v, 
        'No_Source_Value') as registration_name, 
        coalesce(a.id, 
        'No_Source_Value') as application_id, 
        coalesce(a.name__v, 
        'No_Source_Value') as application_name, 
        coalesce( c.country_code__rim, 
        'No_Source_Value' ) as country_code, 
        case 
        when coalesce( pd.item_code__c, 
        complex_dp.family_item_code ) is null then 'No_Source_Value' 
        else coalesce( pd.item_code__c, 
        complex_dp.family_item_code ) 
        end as family_item_code, 
        coalesce( p.name__v, 
        'No_Source_Value' ) as product_family 
        from 
        {validation['source_schema']}.{validation['source_table']} r 
        left join {validation['source_schema']}.product p on 
        p.id = r.product_family__v 
        left join {validation['source_schema']}.application a on 
        r.application__rim = a.id 
        left join {validation['source_schema']}.country c on 
        r.country__rim = c.id 
        left join {validation['source_schema']}.drug_product dp on 
        dp.id = r.pharmaceutical_product__rim 
        left join ( 
        select 
        pc.name__v as drug_product, 
        dpc.id as drug_product_id, 
        pdc.name__v as product_variant_name, 
        pdc.item_code__c as family_item_code 
        from 
        {validation['source_schema']}.product_component pc 
        join {validation['source_schema']}.drug_product dpc on 
        pc.complex_product__rim = dpc.id 
        join {validation['source_schema']}.product_detail pdc on 
        pc.product_detail__rim = pdc.id) as complex_dp on 
        complex_dp.drug_product_id = r.pharmaceutical_product__rim 
        left join {validation['source_schema']}.product_detail pd on 
        r.presentation__rim = pd.id 
        left join {validation['source_schema']}.objectlifecyclestate_ref r_lc on 
        r_lc.objectlifecycle_name = r.lifecycle__v 
        and r_lc.objectlifecyclestate_name = r.state__v 
        where r.clinical_study_number__v is null),
        target_data AS (
            SELECT 
                registration_id, 
                registration_name,
                application_id,
                application_name,
                country_code,
                family_item_code,
                product_family
            FROM 
                {validation['target_schema']}.{validation['target_table']}
        )
        -- Compare source and target data
        SELECT 
            s.registration_id AS source_registration_id,
            t.registration_id AS target_registration_id,
            s.registration_name AS source_registration_name,
            t.registration_name AS target_registration_name,
            s.application_id AS source_application_id,
            t.application_id AS target_application_id,
            s.application_name AS source_application_name,
            t.application_name AS target_application_name,
            s.country_code AS source_country_code,
            t.country_code AS target_country_code,
            s.family_item_code AS family_item_code,
            t.family_item_code AS target_family_item_code,
            s.product_family AS product_family,
            t.product_family AS target_product_family
        FROM 
            source_data s
        FULL OUTER JOIN 
            target_data t
        ON 
            s.registration_id = t.registration_id
            AND s.registration_name = t.registration_name
            and s.application_id=t.application_id
            and s.application_name=t.application_name
            and s.country_code=t.country_code
            and s.family_item_code=t.family_item_code
            and s.product_family=t.product_family
        WHERE 
            s.registration_id != t.registration_id
            AND s.registration_name != t.registration_name
            and s.application_id!=t.application_id
            and s.application_name!=t.application_name
            and s.country_code!=t.country_code
            and s.family_item_code!=t.family_item_code
            and s.product_family!=t.product_family
            """
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "No_Source_Value_Transformation")
    
    try:
        if diff_count == 0:
            message = f"✅ No_Source_Value_Transformation check passed: All records from {validation['source_table']} gets transformed in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ No_Source_Value_Transformation failed: {diff_count} records in {validation['source_table']} gets transformed from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during No_Source_Value_Transformation completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)
