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
        "target_table": "stg_dim_regcor_drug_product"
    }

#This Test set includes tests of DIM Drug Assembly Table.

def test_TS_RDCC_88_TC_RDCC_89_dp_material_number_validation(db_connection: connection | None,validation: dict[str, str]): 
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 88 - This Test set includes test cases of DIM Drug Product.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

    print(f"RDCC-89-This Test case validates dp_material_number coulmn  data in dim drug product table is correctly fetched from dim_rdm_regcor_master_table for Dp Flavour Vocabulary name\n")
    print(f"Test 1 : Identify dp_material_number in the dim_regcor_drug_product table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    success, count, msg = validate_target_to_source_with_filter(
        connection=db_connection,
        src_schema=validation['source_schema'],
        src_table=validation['source_table'],
        tgt_schema=validation['target_schema'],
        tgt_table=validation['target_table'],
        src_cols=['concept_code'],
        tgt_cols=['dp_material_number'],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.vocabulary_name='DP Flavor'""",
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
        tgt_cols=['dp_material_number '],
        src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
        vocabulary_name='DP Flavor' """,
        tgt_filter=""
    )
    print(msg)
    assert result, msg

def test_TS_RDCC_88_TC_RDCC_90_drug_formulation_manufacturer_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-90-This Test case validates the drug formulation manufacturer in dim drug product is correctly retriving property_value using property_name as “RIM Manufacturers” from dim_rdm_regcor_master_table for DP flavour as vocabulary name\n")
    print(f"Test 1 : Identify drug formulation manufacturer in the dim_regcor_drug_product table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f"""SELECT tgt.dp_material_number,tgt.drug_formulation_manufacturer  
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT concept_code,
    MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS drug_formulation_manufacturer
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DP Flavor'
        group by concept_code
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

    query =f"""SELECT concept_code,
    MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS drug_formulation_manufacturer
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DP Flavor'
        group by concept_code 
    except 
    SELECT tgt.dp_material_number,tgt.drug_formulation_manufacturer  
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

def test_TS_RDCC_88_TC_RDCC_91_dp_manufacturing_line_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-91-This Test case validates the dp manufacturing line in dim drug product is correctly retriving  property_value using property_name as “Manufacturing Line”  from dim_rdm_regcor_master_table for DP flavour as vocabulary name")
    print(f"Test 1 : Identify dp_manufacturing_line in the dim_regcor_device_assembly table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f"""SELECT tgt.dp_material_number, tgt.dp_manufacturing_line 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT concept_code,
    MAX(CASE WHEN property_name = 'Manufacturing Line' THEN property_value END) AS dp_manufacturing_line
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DP Flavor'
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

    query =f"""SELECT concept_code,
    MAX(CASE WHEN property_name = 'Manufacturing Line' THEN property_value END) AS dp_manufacturing_line
        FROM  {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DP Flavor'
        group by concept_code 
    except 
    SELECT tgt.dp_material_number, tgt.dp_manufacturing_line 
    FROM  {validation['target_schema']}.{validation['target_table']} tgt"""

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

def test_TS_RDCC_88_TC_RDCC_92_batch_size_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-92-This Test case validates the batch size in dim drug product is correctly Retrieve property_value using property_name as “Batch Size”  from dim_rdm_regcor_master_table for DP flavour as vocabulary name\n")
    print(f"Test 1 :  Identify  in the dim_regcor_drug_product table that are missing in the source table (dim_rdm_regcor_master_table):   \n")

    query =f"""SELECT tgt.dp_material_number,tgt.batch_size 
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT concept_code,
    MAX(CASE WHEN property_name = 'Batch Size' THEN property_value END) AS batch_size
        FROM {validation['source_schema']}.{validation['source_table']}
        WHERE vocabulary_name = 'DP Flavor'
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

    query =f"""SELECT concept_code,
    MAX(CASE WHEN property_name = 'Batch Size' THEN property_value END) AS batch_size
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'DP Flavor'
    group by concept_code 
    except 
    SELECT tgt.dp_material_number, tgt.batch_size  
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

def test_TS_RDCC_88_TC_RDCC_93_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-93-This Test case validates the family_item_code in dim drug product is correctly Retrieve property_value using property_name as 'Family Item Code'  from dim_rdm_regcor_master_table for DP flavour as vocabulary name.\n")
    print(f"Test 1 :  Identify family_item_code in the dim_regcor_drug_product table that are missing in the source table (dim_rdm_regcor_master_table):  \n")

    query =f"""SELECT tgt.dp_material_number,tgt.family_item_code
    FROM {validation['target_schema']}.{validation['target_table']} tgt
    except
    SELECT concept_code,
    MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'DP Flavor'
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

    query =f"""
         SELECT concept_code,
            MAX(CASE WHEN property_name = 'Family Item Code'THEN property_value END) AS family_item_code
            FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'DP Flavor'
                group by concept_code 
            except 
            SELECT tgt.dp_material_number,tgt.family_item_code 
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

def test_TS_RDCC_88_TC_RDCC_94_manufacturer_id_and_sap_plant_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-94-This test case validates manufacturer_id & sap_plant_code in dim drug product table retrives data from di_regcor_manufacturer table.\n")
    print(f"Test 1 : Identify manufacturer_id,sap_plant_code in the dim_regcor_drug_product table that are missing in the source table (dim_regcor_manufacturer):  \n")

    query =f"""select dp_material_number,
            drug_formulation_manufacturer, manufacturer_id,sap_plant_code 
            from {validation['target_schema']}.{validation['target_table']} 
            except
            select a.concept_code,
            a.drug_formulation_manufacturer, 
            b.manufacturer_id, 
            b.sap_plant_code
            from 
            ( 
            select concept_code,
            max(case when property_name = 'Manufacturer' then property_value end) as drug_formulation_manufacturer 
            from 
            {validation['source_schema']}.{validation['source_table']} 
            where 
            vocabulary_name = 'DP Flavor' 
            group by 
            concept_code) a 
            left join regcor_refine.dim_regcor_manufacturer b on b.manufacturer = a.drug_formulation_manufacturer"""

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

    query =f"""
        select a.concept_code,
        a.drug_formulation_manufacturer, 
        b.manufacturer_id, 
        b.sap_plant_code
        from 
        ( 
        select concept_code,
        max(case when property_name = 'Manufacturer' then property_value end) as drug_formulation_manufacturer 
        from 
        {validation['source_schema']}.{validation['source_table']} 
        where 
        vocabulary_name = 'DP Flavor' 
        group by 
        concept_code) a 
        left join regcor_refine.dim_regcor_manufacturer b on 	b.manufacturer = a.drug_formulation_manufacturer  
        except 
        select dp_material_number,drug_formulation_manufacturer, manufacturer_id,sap_plant_code from {validation['target_schema']}.{validation['target_table']}  ds"""

    print(query)
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

def test_TS_RDCC_88_TC_RDCC_95_concatenation_family_item_code_manufacturer_key_validations(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case validates the concatenation rule used to populate the family_item_code_manufacturer_key in the Dim_regcor_drug_product table.\n")

    query = f"""
    SELECT dp.dp_material_number,
	dm.concept_code,
    dp.family_item_code_manufacturer_key,
    SPLIT_PART(family_item_code_manufacturer_key, '|', 1) AS extracted_dp_family_item_code,
    SPLIT_PART(family_item_code_manufacturer_key, '|', 2) AS extracted_drug_formulation_manufacturer,
    CASE 
        WHEN dp.family_item_code = SPLIT_PART(family_item_code_manufacturer_key, '|', 1)
         AND dp.drug_formulation_manufacturer = SPLIT_PART(family_item_code_manufacturer_key, '|', 2)
        THEN 'Valid'
        ELSE 'Invalid'
    END AS validation_status
from  {validation['target_schema']}.{validation['target_table']} dp 
inner join {validation['source_schema']}.{validation['source_table']} dm
on dp.dp_material_number =dm.concept_code 
where 
dm.vocabulary_name = 'DP Flavor'
group by
dp.dp_material_number,
dm.concept_code,
dp.family_item_code,
dp.family_item_code_manufacturer_key,
dp.drug_formulation_manufacturer;
    """

    try:
        cursor = db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Get column index of 'validation_status'
        col_names = [desc[0] for desc in cursor.description]
        status_idx = col_names.index("validation_status")

        # Check for invalid statuses
        invalid_rows = [row for row in results if row[status_idx] != "Valid"]
        invalid_count = len(invalid_rows)

        assert invalid_count == 0, f"❌ {invalid_count} rows have invalid validation_status."
        print("✅ All rows have 'Valid' in validation_status.")
        logging.info("✅ All rows have 'Valid' in validation_status.")

    except Exception as e:
        error_msg = f"❌ Error during validation status check: {str(e)}"
        logging.exception(error_msg)
        return False, -1, error_msg

def test_TS_RDCC_88_TC_RDCC_96_concatenation_material_plant_key_validations(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case validates the concatenation rule used to populate the material_plant_key in the Dim_regcor_drug_product table.\n")

    query = f"""
    SELECT 
    dp.dp_material_number,
    dm.sap_plant_code,
    SPLIT_PART(material_plant_key, '|', 1) AS extracted_dp_material_number,
    SPLIT_PART(material_plant_key, '|', 2) AS extracted_sap_plant_code,
    CASE 
        WHEN dp.dp_material_number = SPLIT_PART(material_plant_key, '|', 1)
         AND dm.sap_plant_code = SPLIT_PART(material_plant_key, '|', 2)
        THEN 'Valid'
        ELSE 'Invalid'
    END AS validation_status
FROM {validation['target_schema']}.{validation['target_table']} dp
LEFT JOIN {validation['source_schema']}.dim_regcor_manufacturer dm 
ON dp.drug_formulation_manufacturer  = dm.manufacturer
    """

    try:
        cursor = db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Get column index of 'validation_status'
        col_names = [desc[0] for desc in cursor.description]
        status_idx = col_names.index("validation_status")

        # Check for invalid statuses
        invalid_rows = [row for row in results if row[status_idx] != "Valid"]
        invalid_count = len(invalid_rows)

        assert invalid_count == 0, f"❌ {invalid_count} rows have invalid validation_status."
        print("✅ All rows have 'Valid' in validation_status.")
        logging.info("✅ All rows have 'Valid' in validation_status.")

    except Exception as e:
        error_msg = f"❌ Error during validation status check: {str(e)}"
        logging.exception(error_msg)
        return False, -1, error_msg

def test_TS_RDCC_88_TC_RDCC_97_PK_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Duplicates,Null checks  Primary key column dp_material_number in DIM table and with source tables.\n")
    # -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['dp_material_number']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = ["dp_material_number"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message











