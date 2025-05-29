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
# fact_regcor_drug_product_registration.Registration_id column Validation
def test_TS_RDCC_18_TC_RDCC_19_registration_id_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"Test Set-RDCC-18 - This Test set contains test cases for Fact_regcor_drug_substance_registration.")
    print(f"This test case ensures that the Registration_id in the Fact_regcor_drug_substance_registration table is correctly mapped to the id in the source rim_ref.registration table.\n")
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

    print(f"Test : Identify registration_id in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration): '.):\n")
    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
FROM {validation['target_schema']}.{validation['target_table']} fdm
except
(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
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
    query =f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 
      'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
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
    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.registration_name,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
FROM {validation['target_schema']}.{validation['target_table']} fdm
except
(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r.name__v as registration_name,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r.name__v as registration_name,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN regcor_refine.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
 """

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ Source-to-target check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-target check failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify source_registration_name in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r.name__v as registration_name,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r.name__v as registration_name,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.registration r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,
  fdm.registration_name,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
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

    query =f"""SELECT DISTINCT
          fdm.registration_id,fdm.registration_state,
          fdm.manufacturer_id,
          fdm.drug_substance_name,
          fdm.registered_active_ingredient_id
        FROM {validation['target_schema']}.{validation['target_table']} fdm
        except
        (SELECT
          COALESCE(r.id, 'No_Source_Value') AS registration_id,
          r_lc.lifecyclestate_label as registration_state,
          COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
          COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
          COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id
        FROM (
            SELECT
              raisub.id,
              raisub.manufacturer_name__rim,
              raisub.active_substance__rim,
              raisub.registration__rim,
              raisub.application__rim,
              raisub.manufacturing_activity__c, 
            raisub.verified__c
            FROM {validation['source_schema']}.registered_active_ingredient raisub
            JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
              ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
            AND raisub_lc.objectlifecyclestate_name = raisub.state__v
            CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
            CROSS JOIN UNNEST(status__v) AS rai_status
            WHERE
              raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
              AND manufacturing_activity::text IN (
                'manufacture_of_active_substance__c',
                'manufacture_of_active_substance_intermed__c',
                'manufacture_of_fermentation__c',
                'micronization_of_active_substance__c',
                'packaging_of_active_substance__c'
              )
              AND rai_status = 'active__v'
              AND raisub.verified__C IS TRUE
            GROUP BY
              raisub.id,
              raisub.manufacturer_name__rim,
              raisub.active_substance__rim,
              raisub.registration__rim,
              raisub.application__rim,
              raisub.manufacturing_activity__c, 
            raisub.verified__c
        ) AS rai
        LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
        JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
        JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
        JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
          AND r.state__v IN (
            'approved_state1__c', 
            'no_registration_required_state__c', 
            'transferred_state__v', 
            'expired_import_allowed_state__c'
          )
        LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
        JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
        LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
          ON r_lc.objectlifecycle_name = r.lifecycle__v
        AND r_lc.objectlifecyclestate_name = r.state__v
        WHERE
          registration_status = 'active__v'
          AND r.clinical_study_number__v IS NULL
        UNION
        SELECT
          COALESCE(r.id, 'No_Source_Value') AS registration_id,r_lc.lifecyclestate_label as registration_state,
          COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
          COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
          'No_Source_Value' AS registered_active_ingredient_id
        FROM {validation['source_schema']}.{validation['source_table']} r
        JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
        JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
          AND pas.status__v::text = '{{active__v}}'
        JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
          AND asm.status__v::text = '{{active__v}}'
        LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
        JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
        JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
          AND r.state__v IN ('no_registration_required_state__c')
        LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
        JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
        LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
          ON r_lc.objectlifecycle_name = r.lifecycle__v
        AND r_lc.objectlifecyclestate_name = r.state__v
        WHERE
          registration_status = 'active__v'
          AND r.clinical_study_number__v IS null)
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

    print(f"Test : Identify source_registration_state in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r_lc.lifecyclestate_label as registration_state,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,r_lc.lifecyclestate_label as registration_state,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{active__v}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,fdm.registration_state,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id
FROM {validation['target_schema']}.{validation['target_table']} fdm"""

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


def test_TS_RDCC_18_TC_RDCC_22_manufacturer_id_RDCC_23_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the manufacturer_id in the fact table is correctly mapped to the id in the source rim_ref.manufacturer table.\n")
    print(f"\nIdentify manufacturer_id in the stg_fact_regcor_drug_substance_registration table that are missing in the source table  \n")
    print(f"Test : Identify registration_state in the {validation['target_schema']}.{validation['target_table']} table that are missing in the source table (registration): '.):\n")

    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.manufacturer_name,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
FROM {validation['target_schema']}.{validation['target_table']} fdm
except
(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
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

    print(f"Test : Identify manufacturer_id in the source table that are missing in the fact_regcor_drug_substance_registration table:\n")
    query =f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.manufacturer_name,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
FROM {validation['target_schema']}.{validation['target_table']} fdm
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
    
    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
        FROM {validation['target_schema']}.{validation['target_table']} fdm
        except
        (SELECT
          COALESCE(r.id, 'No_Source_Value') AS registration_id,
          COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
          COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
          COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
          a.id
        FROM (
            SELECT
              raisub.id,
              raisub.manufacturer_name__rim,
              raisub.active_substance__rim,
              raisub.registration__rim,
              raisub.application__rim,
              raisub.manufacturing_activity__c, 
            raisub.verified__c
            FROM {validation['source_schema']}.registered_active_ingredient raisub
            JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
              ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
            AND raisub_lc.objectlifecyclestate_name = raisub.state__v
            CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
            CROSS JOIN UNNEST(status__v) AS rai_status
            WHERE
              raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
              AND manufacturing_activity::text IN (
                'manufacture_of_active_substance__c',
                'manufacture_of_active_substance_intermed__c',
                'manufacture_of_fermentation__c',
                'micronization_of_active_substance__c',
                'packaging_of_active_substance__c'
              )
              AND rai_status = 'active__v'
              AND raisub.verified__C IS TRUE
            GROUP BY
              raisub.id,
              raisub.manufacturer_name__rim,
              raisub.active_substance__rim,
              raisub.registration__rim,
              raisub.application__rim,
              raisub.manufacturing_activity__c, 
            raisub.verified__c
        ) AS rai
        LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
        JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
        JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
        JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
          AND r.state__v IN (
            'approved_state1__c', 
            'no_registration_required_state__c', 
            'transferred_state__v', 
            'expired_import_allowed_state__c'
          )
        LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
        JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
        LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
          ON r_lc.objectlifecycle_name = r.lifecycle__v
        AND r_lc.objectlifecyclestate_name = r.state__v
        WHERE
          registration_status = 'active__v'
          AND r.clinical_study_number__v IS NULL
        UNION
        SELECT
          COALESCE(r.id, 'No_Source_Value') AS registration_id,
          COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
          COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
          'No_Source_Value' AS registered_active_ingredient_id,
          a.id
        FROM {validation['source_schema']}.{validation['source_table']} r
        JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
        JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
          AND pas.status__v::text = '{{active__v}}'
        JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
          AND asm.status__v::text = '{{active__v}}'
        LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
        JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
        JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
          AND r.state__v IN ('no_registration_required_state__c')
        LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
        JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
        LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
          ON r_lc.objectlifecycle_name = r.lifecycle__v
        AND r_lc.objectlifecyclestate_name = r.state__v
        WHERE
          registration_status = 'active__v'
          AND r.clinical_study_number__v IS null)
        """

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
    query =f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.{validation['source_table']} r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  m.name__v,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id
FROM {validation['source_schema']}.{validation['source_table']} r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.manufacturer_name,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id
FROM {validation['target_schema']}.{validation['target_table']} fdm
"""

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

def test_TS_RDCC_18_TC_RDCC_25_application_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"This test case verifies that the application_name in the fact table is correctly mapped to the name__v in the source rim_ref.application table.\n")
    print(f"Identify application_name in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id,fdm.application_name
FROM {validation['target_schema']}.{validation['target_table']} fdm
except
(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id,a.name__V
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id,a.name__V
FROM {validation['source_schema']}.registration r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
"""

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
    query = f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id,a.name__V
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id,a.name__V
FROM {validation['source_schema']}.registration r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
EXCEPT
SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id,fdm.application_name
FROM {validation['target_schema']}.{validation['target_table']} fdm
            """
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
    
    query =f"""SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id,
 fdm.product_family_name
FROM {validation['target_schema']}.{validation['target_table']} fdm
except
(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id,p.name__v AS product_family_name
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
LEFT JOIN {validation['source_schema']}.product p 
    ON p.id = ds.product__v  
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id,p.name__v AS product_family_name
FROM {validation['source_schema']}.registration r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
"""

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
        message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)

    print(f"Test : Identify product_family_name in the source table that are missing in the {validation['target_schema']}.{validation['target_table']} table:\n")
    
    query = f"""(SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  COALESCE(rai.id, 'No_Source_Value') AS registered_active_ingredient_id,
  a.id,p.name__v AS product_family_name
FROM (
    SELECT
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
    FROM {validation['source_schema']}.registered_active_ingredient raisub
    JOIN {validation['source_schema']}.objectlifecyclestate_ref raisub_lc
      ON raisub_lc.objectlifecycle_name = raisub.lifecycle__v 
     AND raisub_lc.objectlifecyclestate_name = raisub.state__v
    CROSS JOIN UNNEST(manufacturing_activity__c) AS manufacturing_activity
    CROSS JOIN UNNEST(status__v) AS rai_status
    WHERE
      raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
      AND manufacturing_activity::text IN (
        'manufacture_of_active_substance__c',
        'manufacture_of_active_substance_intermed__c',
        'manufacture_of_fermentation__c',
        'micronization_of_active_substance__c',
        'packaging_of_active_substance__c'
      )
      AND rai_status = 'active__v'
      AND raisub.verified__C IS TRUE
    GROUP BY
      raisub.id,
      raisub.manufacturer_name__rim,
      raisub.active_substance__rim,
      raisub.registration__rim,
      raisub.application__rim,
      raisub.manufacturing_activity__c, 
		raisub.verified__c
) AS rai
LEFT JOIN {validation['source_schema']}.application a ON a.id = rai.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = rai.manufacturer_name__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = rai.active_substance__rim
JOIN {validation['source_schema']}.registration r ON r.id = rai.registration__rim
  AND r.state__v IN (
    'approved_state1__c', 
    'no_registration_required_state__c', 
    'transferred_state__v', 
    'expired_import_allowed_state__c'
  )
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN regcor_refine.country c ON r.country__rim = c.id
LEFT JOIN regcor_refine.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
LEFT JOIN {validation['source_schema']}.product p 
    ON p.id = ds.product__v  
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS NULL
UNION
SELECT
  COALESCE(r.id, 'No_Source_Value') AS registration_id,
  COALESCE(m.id, 'No_Source_Value') AS manufacturer_id,
  COALESCE(ds.name__v, 'No_Source_Value') AS drug_substance_name,
  'No_Source_Value' AS registered_active_ingredient_id,
  a.id,p.name__v AS product_family_name
FROM {validation['source_schema']}.registration r
JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim
  AND pas.status__v::text = '{{active__v}}'
JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim
  AND asm.status__v::text = '{{active__v}}'
LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim
JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim
  AND r.state__v IN ('no_registration_required_state__c')
LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc
  ON r_lc.objectlifecycle_name = r.lifecycle__v
 AND r_lc.objectlifecyclestate_name = r.state__v
WHERE
  registration_status = 'active__v'
  AND r.clinical_study_number__v IS null)
except 
SELECT DISTINCT
  fdm.registration_id,
  fdm.manufacturer_id,
  fdm.drug_substance_name,
  fdm.registered_active_ingredient_id,
  fdm.application_id,
 fdm.product_family_name
FROM {validation['target_schema']}.{validation['target_table']} fdm"""
    
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


# def test_TS_RDCC_18_TC_RDCC_27_drug_substance_name_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"This test case verifies that the drug_substance_name in the fact table is correctly mapped to the name__v in the rim_ref.drug_substance table and is not null.\n")
#     print(f"Identify drug_substance_name in the stg_fact_regcor_drug_substance_registration table that are missing from source table and vice versa.\n")
    
#     query =f"""select count(fdm.drug_substance_name) 
#                 from {validation['target_schema']}.{validation['target_table']} fdm  
#                 except SELECT count(ds.name__v)
#                 FROM {validation['source_schema']}.drug_substance ds
#                 JOIN {validation['source_schema']}.registered_active_ingredient rai 
#                 ON ds.id = rai.active_substance__rim
#                 WHERE  rai.state__v IN (
#                         'approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c'
#                     )
#                     AND 'active_v' = ANY(rai.status__v)"""

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in product table."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from product table."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during target-to-source completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     print(f"Test : Identify drug_substance_name in the source table that are missing in the {validation['target_schema']}.{validation['target_table']} table:\n")
#     query =f"""SELECT count(ds.name__v)
#                 FROM {validation['source_schema']}.drug_substance ds
#                 JOIN {validation['source_schema']}registered_active_ingredient rai 
#                 ON ds.id = rai.active_substance__rim
#                 WHERE  rai.state__v IN (
#                         'approved_state1__c', 'no_registration_required_state__c', 
#                         'divested_reporting_state__c'
#                     )
#                     AND 'active_v' = ANY(rai.status__v)
#                 Except
#                 select count(fdm.drug_substance_name) from {validation['target_schema']}.{validation['target_table']} fdm  
#                 """
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Source-to-Target check passed: All records from {validation['source_table']} exist in product table."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Source-to-Target check failed: {diff_count} records in {validation['source_table']} missing from product table."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_18_TC_RDCC_28_registered_active_ingredient_id_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"This test case verifies that the registered_active_ingredient_id in the fact table is correctly mapped to the name__v in the source rim_ref.registered_active_ingredient.\n")
#     print(f"Identify registered_active_ingredient_id in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
#     primary_keys = ['registered_active_ingredient_id']
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

#     print("\nIdentify registered_active_ingredient_id in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration) and vice versa.")

#     query =f"""SELECT fdm.registered_active_ingredient_id
#                 FROM {validation['target_schema']}.{validation['target_table']} fdm 
#                 where registered_active_ingredient_id != 'No_Source_Value'
#                 except
#                 select rai.id
#                 from {validation['source_schema']}.{validation['source_table']} r,
#                 {validation['source_schema']}.registered_active_ingredient rai  
#                 where r.id=rai.registration__rim and   r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
#                 and rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c');
#                 """

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in application."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from application."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during target-to-source completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     query =f"""select rai.id from {validation['source_schema']}.{validation['source_table']} r,
#             {validation['source_schema']}.registered_active_ingredient rai 
#             left join UNNEST(rai.manufacturing_activity__c) AS manufacturing_activity
#             ON 
#             true
#             where r.id=rai.registration__rim and   r.state__v in ( 'approved_state1__c', 'no_registration_required_state__c', 'transferred_state__v', 'expired_import_allowed_state__c' )
#             and rai.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c')
#             and r.status__v = '{{active__v}}' 
#             and rai.status__v = '{{active__v}}' 
#             and rai.verified__c IS TRUE
#             and r.clinical_study_number__v  is null
#             and  manufacturing_activity::TEXT IN (
#                     'manufacture_of_active_substance__c', 
#                     'manufacture_of_active_substance_intermed__c', 
#                     'manufacture_of_fermentation__c', 
#                     'micronization_of_active_substance__c', 
#                     'packaging_of_active_substance__c')
#             except 
#             SELECT fdm.registered_active_ingredient_id
#             FROM {validation['target_schema']}.{validation['target_table']} fdm"""

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Source-to-Target check passed: All records from application exist in {validation['target_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Source-to-Target check failed: {diff_count} records in application missing from {validation['target_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_18_TC_RDCC_29_registered_active_ingredient_state_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"This test case verifies the registered_active_ingredient_state in the fact table, ensuring that the label value is correctly retrieved from the objectlifecyclestate_ref table.\n")
#     print(f"Identify registered_active_ingredient_state in the stg_fact_regcor_drug_substance_registration table that are missing from source table \n")
    
#     print("\nIdentify registered_active_ingredient_id in the stg_fact_regcor_drug_substance_registration table that are missing in the source table (registration) and vice versa.")

#     query =f"""SELECT fdm.registered_active_ingredient_state
#                 FROM {validation['target_schema']}.{validation['target_table']} fdm 
#                 where registered_active_ingredient_state != 'No_Source_Value'
#                 except
#                 select  r_lc.lifecyclestate_label as registered_active_ingredient_state
#                 from {validation['source_schema']}.{validation['source_table']} r,
#                 {validation['source_schema']}.registered_active_ingredient raisub, 
#                 {validation['source_schema']}.objectlifecyclestate_ref r_lc
#                 where r.id=raisub.registration__rim
#                 and r_lc.objectlifecycle_name = raisub.lifecycle__v 
#                 and r_lc.objectlifecyclestate_name = raisub.state__v
#                 and  raisub.state__v IN ('approved_state1__c', 'no_registration_required_state__c', 'divested_reporting_state__c') 
#  """

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Target-to-Source check passed: All records from {validation['target_table']} exist in application."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Target-to-Source check failed: {diff_count} records in {validation['target_table']} missing from application."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during target-to-source completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     query =f"""select  r_lc.lifecyclestate_label as registered_active_ingredient_state
#                 from {validation['source_schema']}.{validation['source_table']} r,
#                 {validation['source_schema']}.registered_active_ingredient raisub, 
#                 {validation['source_schema']}.objectlifecyclestate_ref r_lc
#                 where r.id=raisub.registration__rim
#                 and r_lc.objectlifecycle_name = raisub.lifecycle__v 
#                 and r_lc.objectlifecyclestate_name = raisub.state__v
#                 and  raisub.state__v IN ('approved_state1__c', 
#                 'no_registration_required_state__c', 'divested_reporting_state__c') 
#                 except
#                 SELECT fdm.registered_active_ingredient_state
#                 FROM {validation['target_schema']}.{validation['target_table']} fdm 
#                 where registered_active_ingredient_state != 'No_Source_Value'"""
#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Source-to-Target check passed: All records from application exist in {validation['target_table']}."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Source-to-Target check failed: {diff_count} records in application missing from {validation['target_table']}."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Source-to-Target completeness validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)


# def test_TS_RDCC__TC_RDCC_37_PK_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates the duplicates and null checks of the primary key columns registration_id, manufacturer_id, country_code, and family_item_Code in the fact table.\n")
#     # -- Check for duplicates in primary keys 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['registration_id','manufacturer_id','drug_substance_name','registered_active_ingredient_id','country_code']
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

# def test_TS_RDCC_18_TC_RDCC_30_Foreign_key_validation(db_connection: connection | None,validation: dict[str, str]): 
#     print(f"Validate Foreign Key Relationship between fact_drug_substance_reg.registration_id and dim_registration.registration_id:\n")

#     query =f"""SELECT f.registration_id
#                 FROM {validation['target_schema']}.{validation['target_table']} f
#                 LEFT JOIN {validation['source_schema']}.stg_dim_regcor_registration d 
#                 ON f.registration_id = d.registration_id
#                 WHERE d.registration_id IS NULL;
#                 """

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Foreign Key check passed: All records from {validation['target_table']} exist in application."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Foreign Key check failed: {diff_count} records in {validation['target_table']} missing from application."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Foreign Key validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

#     print(f"Validate Foreign Key Relationship between fact_drug_substance_reg.country_code and dim_country.country_code:")

#     query =f"""SELECT f.country_code
#                     FROM {validation['target_schema']}.{validation['target_table']} f
#                     LEFT JOIN {validation['source_schema']}.dim_regcor_country d 
#                     ON f.country_code = d.country_code
#                     WHERE d.country_code IS NULL; """

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"✅ Foreign Key check passed: All records from {validation['target_table']} exist in application."
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ Foreign Key check failed: {diff_count} records in {validation['target_table']} missing from application."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during Foreign Key validation: {str(e)}"
#         logging.exception(message)
#         test = False
        
#     assert test,message
#     print(message)

# def test_TS_RDCC_18_TC_RDCC_208_No_Source_Value_Transformation_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates that columns in the Drug Substance Registration fact table are transformed to no_source_values when null values are present in the source table.\n")
#     query =f"""
#     WITH source_data AS (
#     -- Source Query: Extract rows with "No_Source_Value" transformation
#     SELECT 
#         r.id AS registration_id,
#         r.name__v AS registration_name,
#         m.id AS manufacturer_id,
#         m.name__v AS manufacturer_name,
#         a.id AS application_id,
#         a.name__v AS application_name,
#         p.name__v AS product_family_name,
#         ds.name__v AS drug_substance_name,
#         'No_Source_Value' AS registered_active_ingredient_id,
#         'No_Source_Value' AS registered_active_ingredient_state,
#         r_lc.lifecyclestate_label AS registration_state,
#         c.country_code__rim AS country_code,
#         null AS reg_verified_flag
#     FROM 
#         {validation['source_schema']}.{validation['source_table']} r
#     JOIN {validation['source_schema']}.product p ON p.id = r.product_family__v
#     JOIN {validation['source_schema']}.product_active_substance pas ON p.id = pas.product__rim 
#         AND pas.status__v::TEXT = '{{active__v}}'
#     JOIN {validation['source_schema']}.active_substance_manufacturer asm ON asm.active_substance__rim = pas.active_substance__rim  
#         AND asm.status__v::TEXT = '{{active__v}}'
#     LEFT JOIN {validation['source_schema']}.application a ON a.id = r.application__rim
#     JOIN {validation['source_schema']}.manufacturer m ON m.id = asm.manufacturer__rim  
#     JOIN {validation['source_schema']}.drug_substance ds ON ds.id = pas.active_substance__rim 
#         AND r.state__v IN ('no_registration_required_state__c')
#     LEFT JOIN UNNEST(r.status__v) AS registration_status ON TRUE
#     JOIN {validation['source_schema']}.country c ON r.country__rim = c.id
#     LEFT JOIN {validation['source_schema']}.objectlifecyclestate_ref r_lc ON r_lc.objectlifecycle_name = r.lifecycle__v 
#         AND r_lc.objectlifecyclestate_name = r.state__v
#     WHERE 
#         registration_status = 'active__v' 
#         AND r.clinical_study_number__v IS NULL
# ),
# target_data AS (
#     -- Target Table: Extract rows where registered_active_ingredient_id or state is "No_Source_Value"
#     SELECT 
#         registration_id,
#         registration_name,
#         manufacturer_id,
#         manufacturer_name,
#         application_id,
#         application_name,
#         product_family_name,
#         drug_substance_name,
#         registered_active_ingredient_id,
#         registered_active_ingredient_state,
#         registration_state,
#         country_code,
#         reg_verified_flag
#     FROM 
#         {validation['target_schema']}.{validation['target_table']}
#     WHERE 
#         registered_active_ingredient_id = 'No_Source_Value'
#         and registered_active_ingredient_state = 'No_Source_Value'
# )
# -- Compare Source and Target Data
# SELECT 
#     s.registration_id AS source_registration_id,
#     s.registered_active_ingredient_id AS source_rai_id,
#     s.registered_active_ingredient_state AS source_rai_state,
#     t.registration_id AS target_registration_id,
#     t.registered_active_ingredient_id AS target_rai_id,
#     t.registered_active_ingredient_state AS target_rai_state
# FROM 
#     source_data s
# FULL OUTER JOIN 
#     target_data t
# ON 
#     s.registration_id = t.registration_id
# WHERE 
#     (s.registered_active_ingredient_id IS NULL OR t.registered_active_ingredient_id IS NULL)
#     OR (s.registered_active_ingredient_state IS NULL OR t.registered_active_ingredient_state IS NULL);
# """
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






