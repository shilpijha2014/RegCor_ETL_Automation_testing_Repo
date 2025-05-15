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
        "source_table": "registration",
        "target_db": "regcor_refine_db",
        "target_schema": "regcor_refine",
        "target_table": "stg_fact_regcor_drug_substance_registration",
        "target_column": "registration_id",
        "source_column":"id"
    }

# -----------✅ Test: Table Exists ----------

def test_table_exists(validation):
    print(f"Test Set-RDCC-18 - This Test set contains test cases for Fact_regcor_drug_substance_registration.")
    conn = get_connection(validation["target_db"])
    assert validate_table_exists(conn, validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

def test_validate_connection(validation):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        conn = get_connection(validation["target_db"])
        
        assert conn is not None, f"❌ Connection object is None for {validation["target_db"]}"
        print(f"✅ Successfully connected to database: {validation["target_db"]}")
        conn.close()
    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation["target_db"]}: {str(e)}")

# fact_regcor_drug_product_registration.Registration_id column Validation
def test_TC_RDCC_19_1_registration_id_null_values(validation: dict[str, str]):
    conn = get_connection(validation["target_db"])
    cursor = conn.cursor()
    print("Checking if a column contains NULL values in a given table and schema.")
    query = f"""
        SELECT COUNT(*) 
        FROM "{validation['target_schema']}"."{validation['target_table']}" 
        WHERE "{validation['target_column']}" IS NULL;
    """

    cursor.execute(query)
    null_count = cursor.fetchone()[0]

    assert null_count == 0, (
        f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.{validation['target_column']} "
        f"contains NO NULL values!\n")


# fact_regcor_drug_product_registration.Registration_id column Validation
def test_TS_RDCC_18_TC_RDCC_19_registration_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case ensures that the Registration_id in the Fact_regcor_drug_substance_registration table is correctly mapped to the id in the source rim_ref.registration table.\n")
    print(f"Test : Identify registration_id in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration): '.):\n")
    query =f"""SELECT fdm.registration_id
                FROM {validation['target_schema']}.{validation['target_table']} fdm 
                except
                select r.id from {validation['source_schema']}.{validation['source_table']} r 
                LEFT JOIN UNNEST(r.status__v) AS registration_status ON 
                TRUE
                where 
                r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
                and registration_status = 'active__v'
                and r.clinical_study_number__v  is null
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

    print(f"Test : Identify source_registration_id in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""select r.id from {validation['source_schema']}.{validation['source_table']} r,
             {validation['source_schema']}.registered_active_ingredient rai ,
            {validation['source_schema']}.country c,
            {validation['source_schema']}.product p,
            {validation['source_schema']}.product_active_substance pas
            LEFT JOIN UNNEST(r.status__v) AS registration_status ON 
            true
            LEFT JOIN UNNEST(rai.status__v) AS registerd_status ON 
            true
            LEFT join UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity on true
            LEFT join UNNEST(pas.status__v) AS product_Activi_status on true
            where 
            p.id = r.product_family__v
            and p.id = pas.product__rim 
            and product_Activi_status::text = '{{active__v}}' 
            and r.country__rim =c.id
            and r.id=rai.registration__rim 
            and rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
            and r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
            and registration_status = 'active__v'
            and registerd_status='active__v'
            AND manufacturing_activity::TEXT IN ('manufacture_of_active_substance__c', 'manufacture_of_active_substance_intermed__c', 'manufacture_of_fermentation__c', 'micronization_of_active_substance__c', 'packaging_of_active_substance__c')
            and r.clinical_study_number__v  is null
            except 
            SELECT fdm.registration_id
            FROM {validation['target_schema']}.{validation['target_table']} fdm
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

def test_TS_RDCC_18_TC_RDCC_20_registration_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case ensures that the Registration_name in the Fact_regcor_drug_substance_registration table is correctly mapped to the Name__v  in the source rim_ref.registration table.\n")
    print(f"Test : Identify registration_name in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration): '.):\n")
    # query =f"""SELECT fdm.registration_name
    #             FROM {validation['target_schema']}.{validation['target_table']}  fdm 
    #             Except
    #             select r.name__v from 
    #             {validation['source_schema']}.{validation['source_table']}  r,
    #             {validation['source_schema']}.registered_active_ingredient rai  
    #             where r.id=rai.registration__rim and  r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
    #             and rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') """

    query =f"""SELECT fdm.registration_name
                FROM {validation['target_schema']}.{validation['target_table']} fdm 
                except
                select r.name__v from {validation['source_schema']}.{validation['source_table']} r 
                LEFT JOIN UNNEST(r.status__v) AS registration_status ON 
                TRUE
                where 
                r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
                and registration_status = 'active__v'
                and r.clinical_study_number__v  is null
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

    print(f"Test : Identify source_registration_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""select r.name__v from {validation['source_schema']}.{validation['source_table']} r,
             {validation['source_schema']}.registered_active_ingredient rai ,
            {validation['source_schema']}.country c,
            {validation['source_schema']}.product p,
            {validation['source_schema']}.product_active_substance pas
            LEFT JOIN UNNEST(r.status__v) AS registration_status ON 
            true
            LEFT JOIN UNNEST(rai.status__v) AS registerd_status ON 
            true
            LEFT join UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity on true
            LEFT join UNNEST(pas.status__v) AS product_Activi_status on true
            where 
            p.id = r.product_family__v
            and p.id = pas.product__rim 
            and product_Activi_status::text = '{{active__v}}' 
            and r.country__rim =c.id
            and r.id=rai.registration__rim 
            and rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
            and r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
            and registration_status = 'active__v'
            and registerd_status='active__v'
            AND manufacturing_activity::TEXT IN ('manufacture_of_active_substance__c', 'manufacture_of_active_substance_intermed__c', 'manufacture_of_fermentation__c', 'micronization_of_active_substance__c', 'packaging_of_active_substance__c')
            and r.clinical_study_number__v  is null
            except 
            SELECT fdm.registration_name
            FROM {validation['target_schema']}.{validation['target_table']} fdm
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

def test_TS_RDCC_18_TC_RDCC_21_registration_state_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case ensures that the Registration_state in the Fact_regcor_drug_substance_registration table is correctly mapped  in the source rim_ref.registration table.\n")
    print(f"Identify registration_state in the stg_fact_regcor_drug_product_registration table that are missing from source table and give if transformation logic not working:\n")
    print(f"Test : Identify registration_state in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration): '.):\n")

    query =f"""select f.registration_state
            from {validation['target_schema']}.{validation['target_table']} f
            except
            select r_lc.lifecyclestate_label as registration_state
            from {validation['source_schema']}.{validation['source_table']} r,
            {validation['source_schema']}.registered_drug_product raisub, 
            {validation['source_schema']}.objectlifecyclestate_ref r_lc
            where r.id=registration__rim
            and r_lc.objectlifecycle_name = r.lifecycle__v 
            and r_lc.objectlifecyclestate_name = r.state__v
            and  r.state__v in  ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c')

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

    print(f"Test : Identify source_registration_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""select r_lc.lifecyclestate_label as registration_state
                from {validation['source_schema']}.{validation['source_table']} r,
                {validation['source_schema']}.registered_drug_product raisub, 
                {validation['source_schema']}.objectlifecyclestate_ref r_lc
                where r.id=registration__rim
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                and  r.state__v in  ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c')
                except
                select f.registration_state
                from {validation['target_schema']}.{validation['target_table']} f """

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


def test_TS_RDCC_18_TC_RDCC_22_manufacturer_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the manufacturer_id in the fact table is correctly mapped to the id in the source rim_ref.manufacturer table.\n")
    print(f"Identify manufacturer_id in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    print(f"Test : Identify registration_state in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration): '.):\n")

    query =f"""select f.manufacturer_id  
                from {validation['target_schema']}.{validation['target_table']} f
                left join {validation['source_schema']}.manufacturer m
                on m.id=f.manufacturer_id
                where m.id is null"""

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

    print(f"Test : Identify source_registration_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""SELECT 
    m.id AS manufacturer_id
FROM 
    {validation['source_schema']}.manufacturer m
JOIN 
    {validation['source_schema']}.registered_active_ingredient rai
ON 
    m.id = rai.manufacturer_name__rim
LEFT JOIN 
    UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity
ON 
    TRUE
LEFT JOIN 
    {validation['target_schema']}.{validation['target_table']} f
ON 
    f.manufacturer_id = m.id
WHERE 
    f.manufacturer_id IS NULL
    AND manufacturing_activity::TEXT IN (
        'manufacture_of_active_substance__c', 
        'manufacture_of_active_substance_intermed__c', 
        'manufacture_of_fermentation__c', 
        'micronization_of_active_substance__c', 
        'packaging_of_active_substance__c'
    )
    and 
    rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
 """

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from manufacturer exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in manufacturer missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TS_RDCC_18_TC_RDCC_23_manufacturer_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the manufacturer_name in the fact table is correctly mapped to the name__v in the source rim_ref.manufacturer table.\n")
    print(f"Identify manufacturer_name in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
    query =f"""select f.manufacturer_name  
                from {validation['target_schema']}.{validation['target_table']} f
                left join {validation['source_schema']}.manufacturer m
                on m.id=f.manufacturer_id
                where m.id is null"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in manufacturer."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from manufacturer"
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify manufacturer_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""SELECT 
    m.name__v AS manufacturer_name 
        FROM 
            {validation['source_schema']}.manufacturer m
        JOIN 
            {validation['source_schema']}.registered_active_ingredient rai
        ON 
            m.id = rai.manufacturer_name__rim
        LEFT JOIN 
            UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity
        ON 
            TRUE
        LEFT JOIN 
            {validation['target_schema']}.{validation['target_table']} f
        ON 
            f.manufacturer_id = m.id
        WHERE 
            f.manufacturer_id IS NULL
            AND manufacturing_activity::TEXT IN (
                'manufacture_of_active_substance__c', 
                'manufacture_of_active_substance_intermed__c', 
                'manufacture_of_fermentation__c', 
                'micronization_of_active_substance__c', 
                'packaging_of_active_substance__c'
    )
    and 
    rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
 """

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from manufacturer exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in manufacturer missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TS_RDCC_18_TC_RDCC_24_application_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the application_id in the fact table is correctly mapped to the id in the source rim_ref.application table.\n")
    print(f"Identify application_id in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
    query =f"""select f.application_id  
                from {validation['target_schema']}.{validation['target_table']} f
                left join {validation['source_schema']}.application a
                on a.id=f.application_id
                where a.id is null"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in application."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from application."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify application_id in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""SELECT 
    a.id AS application_id
        FROM 
            {validation['source_schema']}.application a
        JOIN 
            {validation['source_schema']}.registered_active_ingredient rai
        ON 
            a.id = rai.application__rim
        JOIN 
            {validation['source_schema']}.{validation['source_table']} r
        ON   r.id=rai.registration__rim  
        LEFT JOIN 
            UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity
        ON 
            true
        LEFT JOIN UNNEST(rai.status__v) AS registerd_status ON 
            true    
        LEFT JOIN 
            {validation['target_schema']}.{validation['target_table']} f
        ON 
            f.application_id = a.id
        WHERE 
            f.application_id IS NULL
            AND manufacturing_activity::TEXT IN (
                'manufacture_of_active_substance__c', 
                'manufacture_of_active_substance_intermed__c', 
                'manufacture_of_fermentation__c', 
                'micronization_of_active_substance__c', 
                'packaging_of_active_substance__c'
            )
            and 
            rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
            and registerd_status='active__v'
            and r.state__v IN (
                'approved_state1__c', 
                'no_registration_required_state__c', 
                'transferred_state__v', 
                'expired_import_allowed_state__c'
            )
            and 'active_v' = ANY(r.status__v)"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from application exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in application missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print("\nIdentify there is no Null values for application_id in dim table.\n")
    columns_to_check = ['application_id']
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

def test_TS_RDCC_18_TC_RDCC_25_application_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the application_name in the fact table is correctly mapped to the name__v in the source rim_ref.application table.\n")
    print(f"Identify application_name in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
    query =f"""select f.application_name 
                from {validation['target_schema']}.{validation['target_table']} f
                left join {validation['source_schema']}.application a
                on a.id=f.application_id
                where a.id is null"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in application."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from application."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify application_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""SELECT 
    a.name__v AS application_name
        FROM 
            {validation['source_schema']}.application a
        JOIN 
            {validation['source_schema']}.registered_active_ingredient rai
        ON 
            a.id = rai.application__rim
        JOIN 
            {validation['source_schema']}.{validation['source_table']} r
        ON   r.id=rai.registration__rim  
        LEFT JOIN 
            UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity
        ON 
            true
        LEFT JOIN UNNEST(rai.status__v) AS registerd_status ON 
            true    
        LEFT JOIN 
            {validation['target_schema']}.{validation['target_table']} f
        ON 
            f.application_id = a.id
        WHERE 
            f.application_id IS NULL
            AND manufacturing_activity::TEXT IN (
                'manufacture_of_active_substance__c', 
                'manufacture_of_active_substance_intermed__c', 
                'manufacture_of_fermentation__c', 
                'micronization_of_active_substance__c', 
                'packaging_of_active_substance__c'
            )
            and 
            rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
            and registerd_status='active__v'
            and r.state__v IN (
                'approved_state1__c', 
                'no_registration_required_state__c', 
                'transferred_state__v', 
                'expired_import_allowed_state__c'
            )
            and 'active_v' = ANY(r.status__v)"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from application exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in application missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

def test_TS_RDCC_18_TC_RDCC_26_product_family_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the product_family_name in the fact table is correctly mapped to the name__v in the source rim_ref.product table and is not null.\n")
    print(f"Identify product_family_name in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
    query =f"""select count(fdm.product_family_name) 
    from {validation['target_schema']}.{validation['target_table']} fdm  
	except
	SELECT count(p.name__v)
        FROM {validation['source_schema']}.drug_substance ds
        JOIN {validation['source_schema']}.registered_active_ingredient rai ON ds.id = rai.active_substance__rim
        JOIN {validation['source_schema']}.product p ON p.id = ds.product__v
        WHERE  rai.state__v IN (
                'approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c'
            )
            AND 'active_v' = ANY(rai.status__v)"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in product table."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from product table."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify product_family_name in the source table that are missing in the {validation['target_schema']}.{validation['target_table']} table:\n")
    query =f"""SELECT count(p.name__v)
                FROM {validation['source_schema']}.drug_substance ds
                JOIN {validation['source_schema']}.registered_active_ingredient rai ON ds.id = rai.active_substance__rim
                JOIN {validation['source_schema']}.product p ON p.id = ds.product__v
                WHERE  rai.state__v IN (
                        'approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c'
                    )
                    AND 'active_v' = ANY(rai.status__v);
                Except
                select count(fdm.product_family_name) from {validation['target_schema']}.{validation['target_table']} fdm"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-Target check passed: All records from {validation['source_table']} exist in product table."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['source_table']} missing from product table."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)






