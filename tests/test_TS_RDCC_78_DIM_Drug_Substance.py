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
        "source_table": "dim_rdm_regcor_master_table",   
        "target_db": "regcor_refine_db" ,
        "target_schema": "regcor_refine",        
        "target_table": "stg_dim_regcor_drug_substance"
    }

#This Test set includes tests of DIM Drug Substance.

def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 78 - This Test set includes tests of DIM Drug Substance.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    print(f"TS-RDCC-69-This Test set contains test cases for Dim drug substance event")
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

def test_TS_RDCC_78_TC_RDCC_79_data_completeness(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-79-This Test case validates ds_materiel_number coulmn  data in dim drug substance table is correctly fetched from dim_rdm_regcor_master_table for DS Flavour Vocabulary name.\n")
    print(f"Test 1 : Identify ds_material_number in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    success, count, msg = validate_target_to_source_with_filter(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['ds_material_number'],
        src_filter=f"{validation['source_schema']}.{validation['source_table']}.vocabulary_name='DS Flavor'",
        tgt_filter=""
    )

    assert success, msg

    result, count, msg = validate_source_to_target_with_filter(
       connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['ds_material_number'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
        vocabulary_name='DS Flavor'""",
        tgt_filter=""
    )
    print(msg)
    assert result, msg

    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = ["ds_material_number"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_80_process_identifier_validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify process_identfier in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.process_identifier 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT 
    MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) AS process_identifier
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
    group by concept_code"""
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

    query =f"""SELECT 
        MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) 
        AS process_identifier
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        group by concept_code 
    except 
    SELECT tgt.process_identifier 
    FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

    print("\nIdentify there is no Null values for process_identifier in dim table.\n")
    columns_to_check = ["process_identifier"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_81_drug_substance_validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify drug_substance in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT 
    tgt.drug_substance
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT  
        MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'DS Flavor'
    GROUP BY concept_code"""
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

    query =f"""SELECT  
        MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        GROUP BY concept_code
        except 
        SELECT 
            tgt.drug_substance
        FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

    print("\nIdentify there is no Null values for drug_substance in dim table.\n")
    columns_to_check = ["drug_substance"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_82_drug_substance_manufacturer_validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify drug_substance_manufacturer in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT 
    tgt.drug_substance_manufacturer
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT  
        max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        GROUP BY concept_code"""
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

    query =f"""SELECT  
        max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        GROUP BY concept_code
        except 
        SELECT 
            tgt.drug_substance_manufacturer
        FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

    print("\nIdentify there is no Null values for drug_substance in dim table.\n")
    columns_to_check = ["drug_substance_manufacturer"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_83_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify family_item_code in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT 
    tgt.family_item_code
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT  
        MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DS Flavor'
        GROUP BY concept_code
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

    query =f"""SELECT  
    MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'DS Flavor'
    GROUP BY concept_code
    except 
    SELECT 
        tgt.family_item_code
    FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

    print("\nIdentify there is no Null values for drug_substance in dim table.\n")
    columns_to_check = ["family_item_code"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_78_TC_RDCC_84_validations(db_connection: connection | None,validation: dict[str, str]):
    print("\nIdentify manufacturer_id and sap_plant_code in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""select drug_substance_manufacturer,manufacturer_id,sap_plant_code 
        from {validation['target_schema']}.{validation['target_table']} ds   
        except 
        select 
        ds.drug_substance_manufacturer, 
        dm.manufacturer_id, 
        dm.sap_plant_code 
        from 
        ( 
        select 
        max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
        from 
        {validation['source_schema']}.{validation['source_table']}
        where 
        vocabulary_name = 'DS Flavor' 
        group by 
        concept_code, 
        concept_name) ds 
        left join regcor_refine.dim_regcor_manufacturer dm on 
        ds.drug_substance_manufacturer = dm.manufacturer
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

    query =f"""select 
        ds.drug_substance_manufacturer, 
        dm.manufacturer_id, 
        dm.sap_plant_code 
        from 
        ( 
        select 
        max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
        from 
        {validation['source_schema']}.{validation['source_table']}
        where 
        vocabulary_name = 'DS Flavor' 
        group by 
        concept_code, 
        concept_name) ds 
        left join regcor_refine.dim_regcor_manufacturer dm on 
        ds.drug_substance_manufacturer = dm.manufacturer 
        except 
        select drug_substance_manufacturer,manufacturer_id,sap_plant_code 
        from {validation['target_schema']}.{validation['target_table']} ds"""
    
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



def test_TS_RDCC_78_TC_RDCC_87_PK_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Duplicates,Null checks of Primary key column ds_material_number in DIM table.\n")
    # -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['ds_material_number']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in customers table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in customers table for keys {primary_keys}")

    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = ["ds_material_number"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message


def test_TS_RDCC_78_TC_RDCC_126_No_source_value_transformation_validation(db_connection: connection | None,validation: dict[str, str]):
    print(f"RDCC-126-This test case validates the coulmns in dim drug substance is being transformed as no_source_values in case of null present in source table.\n")
    
    query =f"""WITH source_data AS (
    SELECT 
        COALESCE(ds.ds_material_number, 'No_Source_Value') AS ds_material_number,
        ds.drug_substance_manufacturer,
        dm.manufacturer_id,
        ds.drug_substance,
        ds.process_identifier,
        ds.family_item_code,
        CONCAT(ds.concept_name, '|', ds.drug_substance_manufacturer) AS drug_substance_manufacturer_key,
        dm.sap_plant_code,
        CONCAT(ds.ds_material_number, '|', dm.sap_plant_code) AS material_plant_key
    FROM (
        SELECT 
            concept_code AS ds_material_number,
            concept_name,
            MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code,
            MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) AS process_identifier,
            MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance,
            MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS drug_substance_manufacturer
        FROM 
            {validation['source_schema']}.{validation['source_table']} 
        WHERE 
            vocabulary_name = 'DS Flavor'
        GROUP BY 
            concept_code, 
            concept_name
    ) ds
    LEFT JOIN regcor_refine.dim_regcor_manufacturer dm 
    ON ds.drug_substance_manufacturer = dm.manufacturer
    )
    SELECT 
        s.ds_material_number AS source_ds_material_number,
        t.ds_material_number AS target_ds_material_number,
        s.family_item_code AS source_family_item_code,
        t.family_item_code AS target_family_item_code,
        s.drug_substance AS source_drug_substance,
        t.drug_substance AS target_drug_substance
    FROM 
        source_data s
    FULL OUTER JOIN 
        {validation['target_schema']}.{validation['target_table']} t
    ON 
        s.ds_material_number = t.ds_material_number
    WHERE 
        s.family_item_code != t.family_item_code
        OR s.drug_substance != t.drug_substance
        OR t.ds_material_number IS NULL
        OR s.ds_material_number IS NULL"""

    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"""✅ No Source Value transformation check passed: All NULL values from {validation['target_table']} is getting transformed in {validation['source_table']} as No_Source_Value."""
            logging.info(message)
            test = True
        else:
            message = f"❌ No Source Value transformation check passed: All NULL values from {validation['source_table']} is not getting transformed in {validation['target_table']} as No_Source_Value."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during NULLs to No_Source_value transformation validation: {str(e)}"
        logging.exception(message)
        test = False

    assert test,message
    print(message)






    















