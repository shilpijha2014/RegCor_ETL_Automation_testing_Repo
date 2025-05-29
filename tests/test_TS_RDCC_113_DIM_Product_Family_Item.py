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
        "target_table": "stg_dim_regcor_product_family_item_configuration"
    }

#This Test set includes test cases of DIM Product Family Item Configuration .

def test_TS_RDCC_113_TC_RDCC_114_product_configuration_name_validation(db_connection: connection | None,validation: dict[str, str]): 
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 113 - This Test set includes test cases of DIM Product Family Item Configuration.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")
  
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

    print(f"RDCC-114-This test case ensures that the product_configuration_name column data in the dim_product_family_item_configuration table is accurately retrieved from the dim_rdm_regcor_master_table by filtering the vocabulary name with 'Product Family Item Configuration'.\n")
    print(f"Test 1 : Identify product_configuration_name in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    query =f"""select drds.product_configuration_name 
                from {validation['target_schema']}.{validation['target_table']} drds
                except
                select drrmt.concept_code 
                from {validation['source_schema']}.{validation['source_table']} drrmt 
                where drrmt.vocabulary_name='Product Family Item Configuration'"""

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
        select drrmt.concept_code 
                from {validation['source_schema']}.{validation['source_table']} drrmt 
                where drrmt.vocabulary_name='Product Family Item Configuration'
        except 
        select drds.product_configuration_name 
                from {validation['target_schema']}.{validation['target_table']} drds
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

    print(f"\nIdentify there is Null values for product_configuration_name  in {validation['target_table']} table.\n")
    columns_to_check = ["product_configuration_name"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message 

def test_TS_RDCC_113_TC_RDCC_115_description_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-115-This test case ensures that the description column data in the dim_product_family_item_configuration table is accurately retrieved from the dim_rdm_regcor_master_table by filtering the vocabulary name with 'Product Family Item Configuration'.\n")
    print(f"Test 1 : Identify description in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    query =f"""select drds.product_configuration_name ,drds.description  
                from {validation['target_schema']}.{validation['target_table']} drds
                except
                select drrmt.concept_code,drrmt.concept_name  
                from {validation['source_schema']}.{validation['source_table']} drrmt 
                where drrmt.vocabulary_name='Product Family Item Configuration'"""

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
        select drrmt.concept_code,drrmt.concept_name 
                from {validation['source_schema']}.{validation['source_table']} drrmt 
                where drrmt.vocabulary_name='Product Family Item Configuration'
        except 
        select drds.product_configuration_name ,drds.description  
                from {validation['target_schema']}.{validation['target_table']} drds
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

    print(f"\nIdentify there is Null values for description   in {validation['target_table']} table.\n")
    columns_to_check = ["description"]
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message 

def test_TS_RDCC_113_TC_RDCC_116_dosage_strength_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-116-This test case ensures that the dosage_strength in the dim_product_family_item_configuration table is accurately retrieved as property_value using property_name as 'Dosage Strength', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table..\n")
    print(f"Test 1 : Identify dosage_strength  in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    query =f"""select drds.product_configuration_name ,drds.dosage_strength  
                from {validation['target_schema']}.{validation['target_table']} drds
                except
                SELECT concept_code,
                MAX(CASE WHEN property_name = 'Dosage Strength' THEN property_value END) AS dosage_strength
                    FROM {validation['source_schema']}.{validation['source_table']}
                    WHERE vocabulary_name = 'Product Family Item Configuration'
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

    query =f"""
        SELECT concept_code,
                MAX(CASE WHEN property_name = 'Dosage Strength' THEN property_value END) AS dosage_strength
                    FROM {validation['source_schema']}.{validation['source_table']}
                    WHERE vocabulary_name = 'Product Family Item Configuration'
                    group by concept_code 
        except 
        select drds.product_configuration_name ,drds.dosage_strength  
                from {validation['target_schema']}.{validation['target_table']} drds
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

def test_TS_RDCC_113_TC_RDCC_117_dosage_strength__units_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-117-This test case ensures that the dosage_strength_units in the dim_product_family_item_configuration table is accurately retrieved as property_value using property_name as 'Dosage Strength units', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify dosage_strength_units in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table):\n")
    query =f"""SELECT tgt.product_configuration_name ,tgt.dosage_strength_units
                FROM {validation['target_schema']}.{validation['target_table']} tgt
                except
                SELECT concept_code,
                MAX(CASE WHEN property_name = 'Dosage Strength Units' THEN property_value END) AS dosage_strength_units
    FROM {validation['source_schema']}.{validation['source_table']}
    WHERE vocabulary_name = 'Product Family Item Configuration'
    group by concept_code """

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
                MAX(CASE WHEN property_name = 'Dosage Strength Units' THEN property_value END) AS dosage_strength_units
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code 
        except 
                SELECT tgt.product_configuration_name ,tgt.dosage_strength_units
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

def test_TS_RDCC_113_TC_RDCC_118_fill_volume_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-118-This test case ensures that the fill_volume in the dim_product_family_item_configuration table is accurately retrieved as property_value using property_name as 'Dosage Volume', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify fill_volume in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.product_configuration_name ,tgt.fill_volume
            FROM {validation['target_schema']}.{validation['target_table']} tgt
            except
            SELECT concept_code,
            MAX(CASE WHEN property_name = 'Dosage Volume' THEN property_value END) AS fill_volume
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code;"""

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
            MAX(CASE WHEN property_name = 'Dosage Volume' THEN property_value END) AS fill_volume
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code
        except 
                SELECT tgt.product_configuration_name,tgt.fill_volume
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

def test_TS_RDCC_113_TC_RDCC_119__validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-119-This test case ensures that the fill_volume_units in the dim_product_family_item_configuration table is accurately retrieved as property_value using property_name as 'Dosage Volume Units', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify fill_volume_units in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.product_configuration_name,tgt.fill_volume_units
            FROM {validation['target_schema']}.{validation['target_table']} tgt
            except
            SELECT concept_code,
            MAX(CASE WHEN property_name = 'Dosage Volume Units' THEN property_value END) AS fill_volume_units
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code;"""

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
            MAX(CASE WHEN property_name = 'Dosage Volume Units' THEN property_value END) AS fill_volume_units
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code
        except 
                SELECT tgt.product_configuration_name,tgt.fill_volume_units
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

def test_TS_RDCC_113_TC_RDCC_120_Product_family_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-120-This test case ensures that the Product_family in the dim_product_family_item_configuration table is accurately retrieved as property_value using property_name as 'Product Family', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify Product_family in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.product_configuration_name,tgt.product_family
            FROM {validation['target_schema']}.{validation['target_table']} tgt
            except
            SELECT concept_code,
            MAX(CASE WHEN property_name = 'Product Family' THEN property_value END) AS product_family
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code;"""

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
            MAX(CASE WHEN property_name = 'Product Family' THEN property_value END) AS product_family
                FROM {validation['source_schema']}.{validation['source_table']}
                WHERE vocabulary_name = 'Product Family Item Configuration'
                group by concept_code
        except 
                SELECT tgt.product_configuration_name,tgt.product_family
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


def test_TS_RDCC_113_TC_RDCC_121_dp_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-121-This test case ensures that the dp_family_item_code in the dim_product_family_item_configuration table is accurately retrieved as relation_code_to using relation_name as 'Drug Product Family Item Code', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify dp_family_item_code in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.product_configuration_name,dp_family_item_code
            FROM {validation['target_schema']}.{validation['target_table']} tgt
            except
            select distinct b.concept_code, 
            COALESCE(dp_rel.relation_code_to, 'No_Source_Value') AS dp_family_item_code
            FROM ( 
            SELECT concept_code
            FROM 
            {validation['source_schema']}.{validation['source_table']}
            WHERE 
            vocabulary_name = 'Product Family Item Configuration' 
            GROUP BY 
            concept_code
            ) b 
            LEFT JOIN {validation['source_schema']}.{validation['source_table']} dp_rel 
            ON dp_rel.concept_code = b.concept_code 
            AND dp_rel.vocabulary_name = 'Product Family Item Configuration' 
            AND dp_rel.relation_name = 'Drug Product Family Item Code'"""
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
        select distinct b.concept_code, 
            COALESCE(dp_rel.relation_code_to, 'No_Source_Value') AS dp_family_item_code
            FROM ( 
            SELECT concept_code
            FROM 
            {validation['source_schema']}.{validation['source_table']}
            WHERE 
            vocabulary_name = 'Product Family Item Configuration' 
            GROUP BY 
            concept_code
            ) b 
            LEFT JOIN {validation['source_schema']}.{validation['source_table']} dp_rel 
            ON dp_rel.concept_code = b.concept_code 
            AND dp_rel.vocabulary_name = 'Product Family Item Configuration' 
            AND dp_rel.relation_name = 'Drug Product Family Item Code'
        except 
                SELECT product_configuration_name,dp_family_item_code
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

def test_TS_RDCC_113_TC_RDCC_122_da_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-122-This test case ensures that the da_family_item_code in the dim_product_family_item_configuration table is accurately retrieved as relation_code_to using relation_name as 'Device Assembly Family Item Code', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify da_family_item_code in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT product_configuration_name,tgt.da_family_item_code
        FROM {validation['target_schema']}.{validation['target_table']} tgt
        except
        select distinct b.concept_code,
        COALESCE(da_rel.relation_code_to, 'No_Source_Value') AS da_family_item_code
        FROM ( 
        SELECT concept_code
        FROM 
        {validation['source_schema']}.{validation['source_table']}
        WHERE 
        vocabulary_name = 'Product Family Item Configuration' 
        GROUP BY 
        concept_code
        ) b 
        LEFT JOIN {validation['source_schema']}.{validation['source_table']} da_rel 
        ON da_rel.concept_code = b.concept_code 
        AND da_rel.vocabulary_name = 'Product Family Item Configuration' 
        AND da_rel.relation_name = 'Device Assembly Family Item Code'   
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

    query =f"""
        select b.concept_code, 
COALESCE(da_rel.relation_code_to, 'No_Source_Value') AS da_family_item_code
FROM ( 
SELECT 
concept_code
FROM 
{validation['source_schema']}.{validation['source_table']}
WHERE 
vocabulary_name = 'Product Family Item Configuration' 
GROUP BY 
concept_code 
) b 
LEFT JOIN {validation['source_schema']}.{validation['source_table']} da_rel 
ON da_rel.concept_code = b.concept_code 
AND da_rel.vocabulary_name = 'Product Family Item Configuration' 
AND da_rel.relation_name = 'Device Assembly Family Item Code'   
except 
SELECT tgt.product_configuration_name, tgt.da_family_item_code
FROM {validation['target_schema']}.{validation['target_table']} tgt"""
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")

    print(query)  
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

def test_TS_RDCC_113_TC_RDCC_123_fp_family_item_code_validation(db_connection: connection | None,validation: dict[str, str]): 
    print(f"RDCC-123-This test case ensures that the fp_family_item_code in the dim_product_family_item_configuration table is accurately retrieved as relation_code_to using relation_name as 'Finished Packaging Family Item Code', with the filter condition 'Product Family Item Configuration' as the vocabulary name from the dim_rdm_regcor_master_table.\n")
    print(f"Test 1 : Identify fp_family_item_code in the dim_regcor_product_family_item_configuration table that are missing in the source table (dim_rdm_regcor_master_table): \n")
    query =f"""SELECT tgt.product_configuration_name ,tgt.fp_family_item_code
                FROM {validation['target_schema']}.{validation['target_table']} tgt
                except
                select b.concept_code, 
                COALESCE(fp_rel.relation_code_to, 'No_Source_Value') AS fp_family_item_code 
                FROM ( 
                SELECT 
                concept_code
                FROM 
                {validation['source_schema']}.{validation['source_table']}
                WHERE 
                vocabulary_name = 'Product Family Item Configuration' 
                GROUP BY 
                concept_code 
                ) b  
                LEFT JOIN {validation['source_schema']}.{validation['source_table']} fp_rel 
                ON fp_rel.concept_code = b.concept_code 
                AND fp_rel.vocabulary_name = 'Product Family Item Configuration' 
                AND fp_rel.relation_name = 'Finished Packaging Family Item Code'
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

    query =f"""
        select b.concept_code, 
                COALESCE(fp_rel.relation_code_to, 'No_Source_Value') AS fp_family_item_code 
                FROM ( 
                SELECT 
                concept_code
                FROM 
                {validation['source_schema']}.{validation['source_table']}
                WHERE 
                vocabulary_name = 'Product Family Item Configuration' 
                GROUP BY 
                concept_code 
                ) b  
                LEFT JOIN {validation['source_schema']}.{validation['source_table']} fp_rel 
                ON fp_rel.concept_code = b.concept_code 
                AND fp_rel.vocabulary_name = 'Product Family Item Configuration' 
                AND fp_rel.relation_name = 'Finished Packaging Family Item Code'
                except 
                SELECT tgt.product_configuration_name ,tgt.fp_family_item_code
                FROM {validation['target_schema']}.{validation['target_table']} tgt
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

def test_TS_RDCC_113_TC_RDCC_124_PK_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case validates the duplicates and null checks of the primary key columns in the DIM Product Family Item Configuration table.\n")
    # -- Check for duplicates in primary keys 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['product_configuration_name','da_family_item_code','dp_family_item_code','fp_family_item_code']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in {validation["target_table"]} table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in {validation["target_table"]} table for keys {primary_keys}")

    print("\nIdentify there is no Null values for ds_material_number in dim table.\n")
    columns_to_check = primary_keys
    result = check_columns_for_nulls(db_connection, validation['target_schema'], validation['target_table'], columns_to_check)

    for col, null_count in result.items():
        message = f"✅ Column '{col}' has no NULL values." if null_count == 0 else f"❌ Column '{col}' contains {null_count} NULL values."
        print(message)
        assert null_count == 0, message

def test_TS_RDCC_113_TC_RDCC_125_No_Source_Value_Transformation_Check(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis test case ensures that the columns in the dim_product_family_item_configuration table are transformed to no_source_values when null values are present in the source table.\n")
    query =f"""
        WITH source_data AS (
    SELECT 
        b.concept_code AS product_configuration_name, 
        b.concept_name AS description, 
        COALESCE(dp_rel.relation_code_to, 'No_Source_Value') AS dp_family_item_code, 
        COALESCE(da_rel.relation_code_to, 'No_Source_Value') AS da_family_item_code, 
        COALESCE(fp_rel.relation_code_to, 'No_Source_Value') AS fp_family_item_code 
    FROM (
        SELECT 
            concept_name, 
            concept_code 
        FROM 
            {validation['source_schema']}.{validation['source_table']}
        WHERE 
            vocabulary_name = 'Product Family Item Configuration'
    ) b 
    LEFT JOIN {validation['source_schema']}.{validation['source_table']} dp_rel 
        ON dp_rel.concept_name = b.concept_name 
        AND dp_rel.relation_name = 'Drug Product Family Item Code'
    LEFT JOIN {validation['source_schema']}.{validation['source_table']} da_rel 
        ON da_rel.concept_name = b.concept_name 
        AND da_rel.relation_name = 'Device Assembly Family Item Code'
    LEFT JOIN {validation['source_schema']}.{validation['source_table']} fp_rel 
        ON fp_rel.concept_name = b.concept_name 
        AND fp_rel.relation_name = 'Finished Packaging Family Item Code'
),
target_data AS (
    SELECT 
        product_configuration_name, 
        description, 
        dp_family_item_code, 
        da_family_item_code, 
        fp_family_item_code 
    FROM 
        {validation['target_schema']}.{validation['target_table']}-- Replace with the target table name
)
-- Compare source and target
SELECT 
    s.dp_family_item_code, 
    s.da_family_item_code,
    s.fp_family_item_code,
    t. dp_family_item_code,
    t.dp_family_item_code,
    t.fp_family_item_code
FROM 
    source_data s 
FULL OUTER JOIN 
    target_data t 
ON 
    s.dp_family_item_code=t.dp_family_item_code
    and s.da_family_item_code = t.da_family_item_code
    and s.fp_family_item_code = t.fp_family_item_code
WHERE 
    s.dp_family_item_code != t.dp_family_item_code 
    OR s.da_family_item_code != t.da_family_item_code 
    OR s.fp_family_item_code != t.fp_family_item_code"""
    test, diff_count , message  = run_and_validate_empty_query(db_connection, query, "Data Completeness Check")
    
    try:
        if diff_count == 0:
            message = f"✅ No_Source_Value_Transformation check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ No_Source_Value_Transformation failed: {diff_count} records in {validation['source_table']} missing from {validation['target_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during No_Source_Value_Transformation completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    assert test,message
    print(message)
