from psycopg2._psycopg import connection
import pytest
import yaml
import sys
import os
import logging
import pandas as pd

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

# #This Test set includes tests of DIM Drug Substance.

# def test_TS_RDCC_78_TC_RDCC_79_Drug_Substance_Validation(db_connection: connection | None,validation: dict[str, str]): 
#     """
#     Test to validate that a connection to the database can be established.
#     """
#     try:
#         print(f"\nTest Set-RDCC- 78 - This Test set includes tests of DIM Drug Substance.\n")
        
#         assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
#         print(f"✅ Successfully connected to database: {validation['target_db']}")

#     except Exception as e:
#         pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

#     assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
#     print(f"\nTable {validation["target_table"]} exists.")

#     print(f"RDCC-79-This Test case validates ds_materiel_number coulmn  data in dim drug substance table is correctly fetched from dim_rdm_regcor_master_table for DS Flavour Vocabulary name.\n")
#     print(f"Test 1 : Identify ds_material_number in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table):\n")
#     success, count, msg = validate_target_to_source_with_filter(
#         connection=db_connection,
#         src_schema=validation['source_schema'],
#         src_table=validation['source_table'],
#         tgt_schema=validation['target_schema'],
#         tgt_table=validation['target_table'],
#         src_cols=['concept_code'],
#         tgt_cols=['ds_material_number'],
#         src_filter=f"{validation['source_schema']}.{validation['source_table']}.vocabulary_name='DS Flavor'",
#         tgt_filter=""
#     )

#     assert success, msg

#     result, count, msg = validate_source_to_target_with_filter(
#        connection=db_connection,
#         src_schema=validation['source_schema'],
#         src_table=validation['source_table'],
#         tgt_schema=validation['target_schema'],
#         tgt_table=validation['target_table'],
#         src_cols=['concept_code'],
#         tgt_cols=['ds_material_number'],
#         src_filter=f"""{validation['source_schema']}.{validation['source_table']}.
#         vocabulary_name='DS Flavor'""",
#         tgt_filter=""
#     )
#     print(msg)
#     assert result, msg

#     print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
#     columns_to_check = ["ds_material_number"]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message

# def test_TS_RDCC_78_TC_RDCC_80_process_identifier_validation(db_connection: connection | None,validation: dict[str, str]):
#     print("\nIdentify process_identfier in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
#     query =f"""SELECT tgt.ds_material_number,tgt.process_identifier 
#     FROM {validation['target_schema']}.{validation['target_table']} tgt
#     except
#     SELECT concept_code, 
#     MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) AS process_identifier
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#     group by concept_code"""
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

#     query =f"""SELECT concept_code, 
#         MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) 
#         AS process_identifier
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#         group by concept_code 
#     except 
#     SELECT tgt.ds_material_number, tgt.process_identifier 
#     FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

#     print("\nIdentify there is no Null values for process_identifier in dim table.\n")
#     columns_to_check = ["process_identifier"]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message

# def test_TS_RDCC_78_TC_RDCC_81_drug_substance_validation(db_connection: connection | None,validation: dict[str, str]):
#     print("\nIdentify drug_substance in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
#     query =f"""SELECT tgt.ds_material_number,
#     tgt.drug_substance
#     FROM {validation['target_schema']}.{validation['target_table']} tgt
#     except
#     SELECT  concept_code,
#         MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance
#     FROM {validation['source_schema']}.{validation['source_table']}
#     WHERE vocabulary_name = 'DS Flavor'
#     GROUP BY concept_code"""
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

#     query =f"""SELECT concept_code, 
#         MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#         GROUP BY concept_code
#         except 
#         SELECT  tgt.ds_material_number,
#             tgt.drug_substance
#         FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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


# def test_TS_RDCC_78_TC_RDCC_82_drug_substance_manufacturer_validation(db_connection: connection | None,validation: dict[str, str]):
#     print("\nIdentify drug_substance_manufacturer in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
#     query =f"""SELECT tgt.ds_material_number,
#     tgt.drug_substance_manufacturer
#     FROM {validation['target_schema']}.{validation['target_table']} tgt
#     except
#     SELECT concept_code, 
#         max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#         GROUP BY concept_code"""
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

#     query =f"""SELECT  concept_code,
#         max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#         GROUP BY concept_code
#         except 
#         SELECT tgt.ds_material_number,
#             tgt.drug_substance_manufacturer
#         FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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

# def test_TS_RDCC_78_TC_RDCC_83_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]):
#     print("\nIdentify family_item_code in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
#     query =f"""SELECT  tgt.ds_material_number,
#     tgt.family_item_code
#     FROM {validation['target_schema']}.{validation['target_table']} tgt
#     except
#     SELECT concept_code,
#         MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code
#         FROM {validation['source_schema']}.{validation['source_table']}
#         WHERE vocabulary_name = 'DS Flavor'
#         GROUP BY concept_code
# """
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

#     query =f"""SELECT concept_code,
#     MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code
#     FROM {validation['source_schema']}.{validation['source_table']}
#     WHERE vocabulary_name = 'DS Flavor'
#     GROUP BY concept_code
#     except 
#     SELECT 
#          tgt.ds_material_number,tgt.family_item_code
#     FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    
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



# def test_TS_RDCC_78_TC_RDCC_84_validations(db_connection: connection | None,validation: dict[str, str]):
#     print("\nIdentify manufacturer_id and sap_plant_code in the dim_regcor_drug_substance table that are missing in the source table (dim_rdm_regcor_master_table): \n")
#     query =f"""select ds_material_number,drug_substance_manufacturer,
#         manufacturer_id,sap_plant_code
#         from {validation['target_schema']}.{validation['target_table']} ds   
#         except 
#         select ds.concept_code,
#         ds.drug_substance_manufacturer, 
#         dm.manufacturer_id, 
#         dm.sap_plant_code 
#         from 
#         ( 
#         select concept_code,
#         max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
#         from 
#         {validation['source_schema']}.{validation['source_table']}
#         where 
#         vocabulary_name = 'DS Flavor' 
#         group by 
#         concept_code, 
#         concept_name) ds 
#         left join regcor_refine.dim_regcor_manufacturer dm on 
#         ds.drug_substance_manufacturer = dm.manufacturer
# """
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

#     query =f"""select ds.concept_code,
#         ds.drug_substance_manufacturer, 
#         dm.manufacturer_id, 
#         dm.sap_plant_code 
#         from 
#         ( 
#         select concept_code,
#         max(case when property_name = 'Manufacturer' then property_value end) as drug_substance_manufacturer 
#         from 
#         {validation['source_schema']}.{validation['source_table']}
#         where 
#         vocabulary_name = 'DS Flavor' 
#         group by 
#         concept_code, 
#         concept_name) ds 
#         left join regcor_refine.dim_regcor_manufacturer dm on 
#         ds.drug_substance_manufacturer = dm.manufacturer 
#         except 
#         select ds_material_number, drug_substance_manufacturer,manufacturer_id,sap_plant_code 
#         from {validation['target_schema']}.{validation['target_table']} ds"""
    
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

# def test_TS_RDCC_78_TC_RDCC_85_concatenation_drug_manufacturer_key_validations(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates the concatenation rule used to populate the drug_substance_manufacturer_key in the Dim_drug_substance table.\n")

#     query = f"""
#     SELECT 
#         dm.concept_code,
#         dm.concept_name,
#         ds.drug_substance_manufacturer,
#         SPLIT_PART(ds.drug_substance_manufacturer_key, '|', 1) AS extracted_ds_concept_name,
#         SPLIT_PART(ds.drug_substance_manufacturer_key, '|', 2) AS extracted_drug_substance_manufacturer,
#         CASE 
#             WHEN dm.concept_name = SPLIT_PART(ds.drug_substance_manufacturer_key, '|', 1)
#              AND ds.drug_substance_manufacturer = SPLIT_PART(ds.drug_substance_manufacturer_key, '|', 2)
#             THEN 'Valid'
#             ELSE 'Invalid'
#         END AS validation_status
#     FROM {validation['target_schema']}.{validation['target_table']} ds
#     INNER JOIN {validation['source_schema']}.{validation['source_table']} dm
#         ON ds.ds_material_number = dm.concept_code
#     WHERE dm.vocabulary_name = 'DS Flavor'
#     GROUP BY
#         dm.concept_code,
#         dm.concept_name,
#         ds.drug_substance_manufacturer,
#         ds.drug_substance_manufacturer_key;
#     """

#     try:
#         cursor = db_connection.cursor()
#         cursor.execute(query)
#         results = cursor.fetchall()

#         # Get column index of 'validation_status'
#         col_names = [desc[0] for desc in cursor.description]
#         status_idx = col_names.index("validation_status")

#         # Check for invalid statuses
#         invalid_rows = [row for row in results if row[status_idx] != "Valid"]
#         invalid_count = len(invalid_rows)

#         assert invalid_count == 0, f"❌ {invalid_count} rows have invalid validation_status."
#         print("✅ All rows have 'Valid' in validation_status.")
#         logging.info("✅ All rows have 'Valid' in validation_status.")

#     except Exception as e:
#         error_msg = f"❌ Error during validation status check: {str(e)}"
#         logging.exception(error_msg)
#         return False, -1, error_msg

# def test_TS_RDCC_78_TC_RDCC_86_concatenation_material_plant_key_validations(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis test case validates concatenation rule used to populate the material_plant_key in the Dim_regcor_drug_substance table.\n")

#     query = f"""
#     SELECT 
#     ds.ds_material_number,
#     dm.sap_plant_code,
#     SPLIT_PART(material_plant_key, '|', 1) AS extracted_ds_material_number,
#     SPLIT_PART(material_plant_key, '|', 2) AS extracted_sap_plant_code,
#     CASE 
#         WHEN ds.ds_material_number = SPLIT_PART(material_plant_key, '|', 1)
#          AND dm.sap_plant_code = SPLIT_PART(material_plant_key, '|', 2)
#         THEN 'Valid'
#         ELSE 'Invalid'
#     END AS validation_status
# FROM {validation['target_schema']}.{validation['target_table']} ds
# LEFT JOIN {validation['source_schema']}.dim_regcor_manufacturer dm 
# ON ds.drug_substance_manufacturer = dm.manufacturer
#     """

#     try:
#         cursor = db_connection.cursor()
#         cursor.execute(query)
#         results = cursor.fetchall()

#         # Get column index of 'validation_status'
#         col_names = [desc[0] for desc in cursor.description]
#         status_idx = col_names.index("validation_status")

#         # Check for invalid statuses
#         invalid_rows = [row for row in results if row[status_idx] != "Valid"]
#         invalid_count = len(invalid_rows)

#         assert invalid_count == 0, f"❌ {invalid_count} rows have invalid validation_status."
#         print("✅ All rows have 'Valid' in validation_status.")
#         logging.info("✅ All rows have 'Valid' in validation_status.")

#     except Exception as e:
#         error_msg = f"❌ Error during validation status check: {str(e)}"
#         logging.exception(error_msg)
#         return False, -1, error_msg

# def test_TS_RDCC_78_TC_RDCC_87_PK_Check(db_connection: connection | None,validation: dict[str, str]):
#     print("\nThis Test case validates the Duplicates,Null checks of Primary key column ds_material_number in DIM table.\n")
#     # -- Check for duplicates in primary keys 
#     print(f"1.Check for Duplicates\n")
#     primary_keys = ['ds_material_number']
#     result = check_primary_key_duplicates(
#     connection=db_connection,
#     schema_name=validation['target_schema'],
#     table_name=validation["target_table"],
#     primary_keys=primary_keys)
#     assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
#     print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

#     print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
#     columns_to_check = ["ds_material_number"]
#     result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

#     for col, null_count in result.items():
#         message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
#         print(message)
#         assert null_count == 0, message


# def test_TS_RDCC_78_TC_RDCC_126_No_source_value_transformation_validation(db_connection: connection | None,validation: dict[str, str]):
#     print(f"RDCC-126-This test case validates the coulmns in dim drug substance is being transformed as no_source_values in case of null present in source table.\n")
    
#     query =f"""WITH source_data AS (
#     SELECT 
#         COALESCE(ds.ds_material_number, 'No_Source_Value') AS ds_material_number,
#         ds.drug_substance_manufacturer,
#         dm.manufacturer_id,
#         ds.drug_substance,
#         ds.process_identifier,
#         ds.family_item_code,
#         CONCAT(ds.concept_name, '|', ds.drug_substance_manufacturer) AS drug_substance_manufacturer_key,
#         dm.sap_plant_code,
#         CONCAT(ds.ds_material_number, '|', dm.sap_plant_code) AS material_plant_key
#     FROM (
#         SELECT 
#             concept_code AS ds_material_number,
#             concept_name,
#             MAX(CASE WHEN property_name = 'Family Item Code' THEN property_value END) AS family_item_code,
#             MAX(CASE WHEN property_name = 'Process Identifier' THEN property_value END) AS process_identifier,
#             MAX(CASE WHEN property_name = 'RIM Drug Substance' THEN property_value END) AS drug_substance,
#             MAX(CASE WHEN property_name = 'Manufacturer' THEN property_value END) AS drug_substance_manufacturer
#         FROM 
#             {validation['source_schema']}.{validation['source_table']} 
#         WHERE 
#             vocabulary_name = 'DS Flavor'
#         GROUP BY 
#             concept_code, 
#             concept_name
#     ) ds
#     LEFT JOIN regcor_refine.dim_regcor_manufacturer dm 
#     ON ds.drug_substance_manufacturer = dm.manufacturer
#     )
#     SELECT 
#         s.ds_material_number AS source_ds_material_number,
#         t.ds_material_number AS target_ds_material_number,
#         s.family_item_code AS source_family_item_code,
#         t.family_item_code AS target_family_item_code,
#         s.drug_substance AS source_drug_substance,
#         t.drug_substance AS target_drug_substance
#     FROM 
#         source_data s
#     FULL OUTER JOIN 
#         {validation['target_schema']}.{validation['target_table']} t
#     ON 
#         s.ds_material_number = t.ds_material_number
#     WHERE 
#         s.family_item_code != t.family_item_code
#         OR s.drug_substance != t.drug_substance
#         OR t.ds_material_number IS NULL
#         OR s.ds_material_number IS NULL"""

#     test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
#     try:
#         if diff_count == 0:
#             message = f"""✅ No Source Value transformation check passed: All NULL values from {validation['target_table']} is getting transformed in {validation['source_table']} as No_Source_Value."""
#             logging.info(message)
#             test = True
#         else:
#             message = f"❌ No Source Value transformation check passed: All NULL values from {validation['source_table']} is not getting transformed in {validation['target_table']} as No_Source_Value."
#             logging.error(message)
#             test = False

#     except Exception as e:
#         message = f"❌ Error during NULLs to No_Source_value transformation validation: {str(e)}"
#         logging.exception(message)
#         test = False

#     assert test,message
#     print(message)






    















