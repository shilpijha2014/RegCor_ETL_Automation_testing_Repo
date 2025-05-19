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
        "target_table": "stg_fact_regcor_drug_product_registration",
        "target_column": "registration_id",
        "source_column":"id"
    }


def test_TS_143_TC_RDCC_144_registration_id_validation(db_connection, validation: dict[str, str]):
    print(f"Test Set-RDCC-143 - This Test set contains test cases for Fact_regcor_drug_product_registration.")
    conn = get_connection(validation["target_db"])
    assert validate_table_exists(conn, validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

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

    print(f"Test : Identify registration_id in the stg_fact_regcor_drug_product_registration table that are missing in the source table (registration): '.\n")
    query =f"""SELECT f.registration_id
                FROM {validation['target_schema']}.{validation['target_table']} f
                except select r.id from {validation['source_schema']}.{validation['source_table']} r,
                {validation['source_schema']}.registered_drug_product rdp,
                {validation['source_schema']}.manufacturer m,
                {validation['source_schema']}.objectlifecyclestate_ref r_lc
                where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 
                    'no_registration_required_state__c', 'divested_reporting_state__c') 
                    and r.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 
                'expired_import_allowed_state__c')
                    and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                    and rdp.verified__c is true
                    and r.clinical_study_number__v is null
                    and r_lc.objectlifecycle_name = r.lifecycle__v 
                    and r_lc.objectlifecyclestate_name = r.state__v;"""

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

    print(f"Test : Identify source_registration_name in the source table that are missing in the fact_regcor_drug_product_registration table:\n")
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

    print(f"\n2)Identify records in the source table that are missing in the target table:")

    query =f"""select r.id from {validation['source_schema']}.{validation['source_table']} r,
                {validation['source_schema']}.registered_drug_product rdp,
                {validation['source_schema']}.manufacturer m,
                {validation['source_schema']}.objectlifecyclestate_ref r_lc
                where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                and rdp.state__v IN ('approved_state1__c', 
                'no_registration_required_state__c', 'divested_reporting_state__c') 
                and r.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 
                'expired_import_allowed_state__c')
                and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                and rdp.verified__c is true
                and r.clinical_study_number__v is null
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                except 
                SELECT f.registration_id
                FROM {validation['target_schema']}.{validation['target_table']} f;
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

def test_TS_RDCC_143_TC_RDCC_146_registration_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"""This test case ensures that the registration_name in the fact drug product registration table is correctly fetched from the Name__v column in the source registration table.\n""")
    print(f"Test 1 : Identify registration_name in the target table that are missing in the source table and vice versa.")
    
    query = (f"""SELECT f.registration_name
                    FROM {validation['target_schema']}.{validation['target_table']} f
                    except select r.name__v 
                    from {validation['source_schema']}.{validation['source_table']} r,
                    {validation['source_schema']}.registered_drug_product rdp,
                    {validation['source_schema']}.manufacturer m,
                    {validation['source_schema']}.objectlifecyclestate_ref r_lc
                    where m.id = rdp.manufacturer__rim 
                        and r.id=rdp.registration__rim
                        and rdp.state__v IN ('approved_state1__c', 
                        'no_registration_required_state__c', 'divested_reporting_state__c') 
                        and r.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 
                    'expired_import_allowed_state__c')
                        and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                        and rdp.verified__c is true
                        and r.clinical_study_number__v is null
                        and r_lc.objectlifecycle_name = r.lifecycle__v 
                        and r_lc.objectlifecyclestate_name = r.state__v;""")

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

    query =f"""Select r.name__v from {validation['source_schema']}.{validation['source_table']} r,
                {validation['source_schema']}.registered_drug_product rdp,
                {validation['source_schema']}.manufacturer m,
                {validation['source_schema']}.objectlifecyclestate_ref r_lc
                where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 
                    'no_registration_required_state__c', 'divested_reporting_state__c') 
                    and r.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 
                'expired_import_allowed_state__c')
                    and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                    and rdp.verified__c is true
                    and r.clinical_study_number__v is null
                    and r_lc.objectlifecycle_name = r.lifecycle__v 
                    and r_lc.objectlifecyclestate_name = r.state__v
                    except
                SELECT f.registration_name
                FROM {validation['target_schema']}.{validation['target_table']} f;"""
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

def test_TS_RDCC_143_TC_RDCC_147_registration_state_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"""This test case validates the registration_state in the fact table, ensuring that the label value is retrieved from the objectlifecyclestate_ref table.\n""")
    print(f"Test : Identify registration_state in the target table that are missing in the source table and vice versa.")
    
    query = (f"""select f.registration_state
                from {validation['target_schema']}.{validation['target_table']}  f
                except
                select  r_lc.lifecyclestate_label as registration_state
                from {validation['source_schema']}.{validation['source_table']}  r,
                {validation['source_schema']}.registered_drug_product raisub, 
                {validation['source_schema']}.objectlifecyclestate_ref r_lc
                where r.id=registration__rim
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                and  r.state__v in  ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c')
                """)

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

    query =f"""Select r_lc.lifecyclestate_label from 
            {validation['source_schema']}.{validation['source_table']} r,
            {validation['source_schema']}.registered_drug_product rdp,
            {validation['source_schema']}.manufacturer m,
            {validation['source_schema']}.objectlifecyclestate_ref r_lc
            where m.id = rdp.manufacturer__rim 
                and r.id=rdp.registration__rim
                and rdp.state__v IN ('approved_state1__c', 
                'no_registration_required_state__c', 'divested_reporting_state__c') 
                and r.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 
            'expired_import_allowed_state__c')
                and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                and rdp.verified__c is true
                and r.clinical_study_number__v is null
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                except
                SELECT f.registration_state
            FROM  {validation['target_schema']}.{validation['target_table']} f;"""

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


def test_TS_RDCC_143_TC_RDCC_148_manufacturer_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"""This test case validates that the manufacturer_id in the fact table is correctly fetched from the id in the source manufacturer table.\n""")
    print(f"Test : Identify manufacturer_id in the stg_fact_regcor_drug_product_registration table that are missing from source table.")
    
    query = (f"""select f.manufacturer_id 
                    from {validation['target_schema']}.{validation['target_table']} f
                    except
                    select  m.id
                    FROM {validation['source_schema']}.manufacturer m, 
                        {validation['source_schema']}.registered_drug_product rdp, 
                        {validation['source_schema']}.{validation['source_table']} r,
                        {validation['source_schema']}.objectlifecyclestate_ref r_lc 
                        where m.id = rdp.manufacturer__rim 
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 
                    'divested_reporting_state__c')
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 
                    'no_registration_required_state__c', 'divested_reporting_state__c') 
                    and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                    and rdp.verified__c is true
                    and clinical_study_number__v is null
                    and r_lc.objectlifecycle_name = r.lifecycle__v 
                    and r_lc.objectlifecyclestate_name = r.state__v;
    """)

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

    query =f"""select  m.id
                FROM {validation['source_schema']}.manufacturer m, 
                    {validation['source_schema']}.registered_drug_product rdp,
                    {validation['source_schema']}.{validation['source_table']} r,
                    {validation['source_schema']}.objectlifecyclestate_ref r_lc
                    where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                and rdp.state__v IN ('approved_state1__c', 
                'no_registration_required_state__c', 'divested_reporting_state__c') 
                and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                and rdp.verified__c is true
                and clinical_study_number__v is null
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                except
                select f.manufacturer_id 
                from {validation['target_schema']}.{validation['target_table']} f;"""
            
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


def test_TS_RDCC_143_TC_RDCC_149_manufacturer_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"""This test case validates that the manufacturer_name in the fact table is correctly fetched from the id in the source manufacturer table.\n""")
    print(f"Test : Identify manufacturer_name in the stg_fact_regcor_drug_product_registration table that are missing from source table.")
    
    query = (f"""select f.manufacturer_name 
                    from {validation['target_schema']}.{validation['target_table']} f
                    except
                    select m.name__v
                    FROM {validation['source_schema']}.manufacturer m, 
                        {validation['source_schema']}.registered_drug_product rdp, 
                        {validation['source_schema']}.{validation['source_table']} r,
                        {validation['source_schema']}.objectlifecyclestate_ref r_lc 
                        where m.id = rdp.manufacturer__rim 
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 
                    'divested_reporting_state__c')
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 
                    'no_registration_required_state__c', 'divested_reporting_state__c') 
                    and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                    and rdp.verified__c is true
                    and clinical_study_number__v is null
                    and r_lc.objectlifecycle_name = r.lifecycle__v 
                    and r_lc.objectlifecyclestate_name = r.state__v;
    """)

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

    query =f"""select m.name__v
                FROM {validation['source_schema']}.manufacturer m, 
                    {validation['source_schema']}.registered_drug_product rdp,
                    {validation['source_schema']}.{validation['source_table']} r,
                    {validation['source_schema']}.objectlifecyclestate_ref r_lc
                    where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                and rdp.state__v IN ('approved_state1__c', 
                'no_registration_required_state__c', 'divested_reporting_state__c') 
                and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                and rdp.verified__c is true
                and clinical_study_number__v is null
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                except
                select f.manufacturer_name 
                from {validation['target_schema']}.{validation['target_table']} f;"""
            
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

def test_TS_RDCC_143_TC_RDCC_150_application_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"""This test case validates that the application_id in the fact table is correctly fetched from the id in the rim_ref.application table and is not null.\n""")
    print(f"""\nTest : Identify application_id in the stg_fact_regcor_drug_product_registration table that are missing from source table and vice versa.""")
    
    query = (f"""select f.manufacturer_name 
                    from {validation['target_schema']}.{validation['target_table']} f
                    except
                    select m.name__v
                    FROM {validation['source_schema']}.manufacturer m, 
                        {validation['source_schema']}.registered_drug_product rdp, 
                        {validation['source_schema']}.{validation['source_table']} r,
                        {validation['source_schema']}.objectlifecyclestate_ref r_lc 
                        where m.id = rdp.manufacturer__rim 
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 
                    'divested_reporting_state__c')
                        and r.id=rdp.registration__rim
                    and rdp.state__v IN ('approved_state1__c', 
                    'no_registration_required_state__c', 'divested_reporting_state__c') 
                    and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                    and rdp.verified__c is true
                    and clinical_study_number__v is null
                    and r_lc.objectlifecycle_name = r.lifecycle__v 
                    and r_lc.objectlifecyclestate_name = r.state__v;
    """)

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

    query =f"""select m.name__v
                FROM {validation['source_schema']}.manufacturer m, 
                    {validation['source_schema']}.registered_drug_product rdp,
                    {validation['source_schema']}.{validation['source_table']} r,
                    {validation['source_schema']}.objectlifecyclestate_ref r_lc
                    where m.id = rdp.manufacturer__rim 
                    and r.id=rdp.registration__rim
                and rdp.state__v IN ('approved_state1__c', 
                'no_registration_required_state__c', 'divested_reporting_state__c') 
                and rdp.manufacturing_activity__c @> ARRAY['manufacture_of_drug_product__c']
                and rdp.verified__c is true
                and clinical_study_number__v is null
                and r_lc.objectlifecycle_name = r.lifecycle__v 
                and r_lc.objectlifecyclestate_name = r.state__v
                except
                select f.manufacturer_name 
                from {validation['target_schema']}.{validation['target_table']} f;"""
            
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







