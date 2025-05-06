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
        "source_table": "event",   
        "target_db": "regcor_refine_db" ,
        "target_schema": "regcor_refine",        
        "target_table": "stg_dim_regcor_event",
    }
# This Test set contains test cases for Dim Event table
def test_validate_connection(db_connection: connection | None, validation: dict[str, str]):
    """
    Test to validate that a connection to the database can be established.
    """
    try:
        print(f"\nTest Set-RDCC- 64 - This Test set contains test cases for Dim Event table.\n")
        
        assert db_connection is not None, f"❌ Connection object is None for {validation['target_db']}"
        print(f"✅ Successfully connected to database: {validation['target_db']}")

    except Exception as e:
        pytest.fail(f"❌ Failed to connect to {validation['target_db']}: {str(e)}")

def test_table_exists(db_connection: connection | None,validation: dict[str, str]):
    
    assert validate_table_exists( db_connection,validation["target_schema"], validation["target_table"]), "❌ Target Table does not exist!"
    print(f"\nTable {validation["target_table"]} exists.")

    
def test_TS_RDCC_64_TC_RDCC_65_Event_id_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("\nTC_RDCC_65 : This Test case validates the Event id in  dim_regcor_event is correctly fetched the id in from event table having 'event_lifecycle__C' records.\n")
    cursor = db_connection.cursor()

    try:
        query = f"""SELECT 
        e.event_id 
        FROM (
            SELECT 
                e.id AS event_id 
        FROM 
            {validation["source_schema"]}.{validation['source_table']} e 
        JOIN 
            {validation["source_schema"]}.objectlifecyclestate_ref lcs 
            ON e.state__v = lcs.objectlifecyclestate_name 
        WHERE 
            lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) as e
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} dre 
        ON e.event_id = dre.event_id
        WHERE dre.event_id IS NULL;
        """
        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)


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
        
    
    assert test,message
    print(message)

    try:
        query = f"""SELECT 
        dre.event_id 
        FROM {validation['target_schema']}.{validation['target_table']} dre
        LEFT JOIN (
            SELECT 
                e.id AS event_id 
            FROM 
                {validation["source_schema"]}.{validation["source_table"]} e 
            JOIN 
                {validation["source_schema"]}.objectlifecyclestate_ref lcs 
                ON e.state__v = lcs.objectlifecyclestate_name 
            WHERE 
                lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) e
        ON dre.event_id = e.event_id
        WHERE e.event_id IS NULL;
        """

        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Souce-to-Target check for event id passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)

        else:
            message = f"❌ Souce-to-Target check for event id failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)


    except Exception as e:
        message = f"❌ Error during Souce-to-Target for event id completeness validation: {str(e)}"
        logging.exception(message)

    assert test,message
    print(message)

def test_TS_RDCC_64_TC_RDCC_66_Event_name_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Event name in  dim_regcor_event is correctly fetched the name_v in from event table having 'event_lifecycle_C' records")
    cursor = db_connection.cursor()

    try:
        query = f"""SELECT 
        e.event_name
        FROM (
            SELECT 
                e.name__v AS event_name
        FROM 
            {validation["source_schema"]}.{validation['source_table']} e 
        JOIN 
            {validation["source_schema"]}.objectlifecyclestate_ref lcs 
            ON e.state__v = lcs.objectlifecyclestate_name 
        WHERE 
            lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) as e
        LEFT JOIN {validation['target_schema']}.{validation['target_table']} dre 
        ON e.event_name = dre.event_name
        WHERE dre.event_id IS NULL;
        """
        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)
        test = True

        if diff_count == 0:
            message = f"✅ Target-to-Source for event name check passed: All records from {validation['target_table']} exist in {validation['source_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source for event name check failed: {diff_count} records in {validation['target_table']} missing from {validation['source_table']}."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during target-to-source for event name completeness validation: {str(e)}"
        logging.exception(message)
        test = False
        
    
    assert test,message
    print(message)

    try:
        query = f"""SELECT 
        dre.event_id 
        FROM {validation['target_schema']}.{validation['target_table']} dre
        LEFT JOIN (
            SELECT 
                e.id AS event_id 
            FROM 
                {validation["source_schema"]}.{validation["source_table"]} e 
            JOIN 
                {validation["source_schema"]}.objectlifecyclestate_ref lcs 
                ON e.state__v = lcs.objectlifecyclestate_name 
            WHERE 
                lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) e
        ON dre.event_id = e.event_id
        WHERE e.event_id IS NULL;
        """

        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Souce-to-Target for event name check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Souce-to-Target for event name check failed: {diff_count} records in {validation['source_table']}missing from {validation['target_table']} ."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Souce-to-Target for event name completeness validation: {str(e)}"
        logging.exception(message)
        test = False

    assert test,message
    print(message)

def test_TS_RDCC_64_TC_RDCC_67_Event_lifecycle_state_data_completeness(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Event_lifecycle_state in  dim_regcor_event is correctly fetched from event & objectlifecyclestate_ref table having 'event_lifecycle__C' records\n")
    cursor = db_connection.cursor()
    
    try:
        print(f"\nTest 1) Ensure that event_lifecycle_status in the target matches the source.")
        query = f"""SELECT 
    src.event_id, 
    src.event_lifecycle_status AS source_status, 
    tgt.event_lifecycle_status AS target_status
        FROM (
            SELECT 
                e.id AS event_id, 
                lcs.lifecyclestate_label AS event_lifecycle_status 
            FROM 
                {validation["source_schema"]}.event e 
            JOIN 
                {validation["source_schema"]}.objectlifecyclestate_ref lcs 
                ON e.state__v = lcs.objectlifecyclestate_name 
            WHERE 
                lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) src
        LEFT JOIN {validation["target_schema"]}.dim_regcor_event tgt
        ON src.event_id = tgt.event_id
        WHERE src.event_lifecycle_status != tgt.event_lifecycle_status
        OR tgt.event_lifecycle_status IS NULL;
        """

        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Target-to-Source for event_lifecycle_status check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source for event_lifecycle_status check failed: {diff_count} records in {validation['source_table']}missing from {validation['target_table']} ."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Target-to-Source for event_lifecycle_status completeness validation: {str(e)}"
        logging.exception(message)
        test = False

    assert test,message
    print(message)

    try:
        print(f"\nTest 2)Ensure all event_id values with event_lifecycle_status from the source exist in the target.")
        query = f"""SELECT 
        src.event_id 
        FROM (
            SELECT 
                e.id AS event_id, 
                lcs.lifecyclestate_label AS event_lifecycle_status 
            FROM 
                {validation["source_schema"]}.event e 
            JOIN 
                {validation["source_schema"]}.objectlifecyclestate_ref lcs 
                ON e.state__v = lcs.objectlifecyclestate_name 
            WHERE 
                lcs.objectlifecycle_name = 'events_lifecycle__c'
        ) src
        LEFT JOIN {validation["target_schema"]}.dim_regcor_event tgt
        ON src.event_id = tgt.event_id
        WHERE tgt.event_id IS NULL;
        """

        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Source-to-Target for event_id check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Source-to-Target for event_id check failed: {diff_count} records in {validation['source_table']}missing from {validation['target_table']} ."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Source-to-Target for event_id completeness validation: {str(e)}"
        logging.exception(message)
        test = False

    assert test,message
    print(message)

    try:
        print(f"\nTest 3) Ensure there are no event_id values in the target with event_lifecycle_status that are not present in the source.")
        query = f"""SELECT 
    tgt.event_id 
FROM {validation["target_schema"]}.dim_regcor_event tgt
LEFT JOIN (
    SELECT 
        e.id AS event_id, 
        lcs.lifecyclestate_label AS event_lifecycle_status 
    FROM 
        {validation["source_schema"]}.event e 
    JOIN 
        {validation["source_schema"]}.objectlifecyclestate_ref lcs 
        ON e.state__v = lcs.objectlifecyclestate_name 
    WHERE 
        lcs.objectlifecycle_name = 'events_lifecycle__c'
) src
ON tgt.event_id = src.event_id
WHERE src.event_id IS NULL;

        """

        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Target-to-Source for event_id check passed: All records from {validation['source_table']} exist in {validation['target_table']}."
            logging.info(message)
            test = True
        else:
            message = f"❌ Target-to-Source forevent_id check failed: {diff_count} records in {validation['source_table']}missing from {validation['target_table']} ."
            logging.error(message)
            test = False

    except Exception as e:
        message = f"❌ Error during Target-to-Source for event_lifecycle_status completeness validation: {str(e)}"
        logging.exception(message)
        test = False

    assert test,message
    print(message)

def test_TS_RDCC_64_TC_RDCC_68_Primary_Key_Validation(db_connection: connection | None,validation: dict[str, str]):
    print("\nThis Test case validates the Duplicates,Null checks of Primary key column event_id in DIM table. \n")
# -- Check for duplicates in country_code 
    print(f"1.Check for Duplicates\n")
    primary_keys = ['event_id']
    result = check_primary_key_duplicates(
    connection=db_connection,
    schema_name=validation['target_schema'],
    table_name=validation["target_table"],
    primary_keys=primary_keys)
    assert result, f"❌ Duplicate values found in customers table for keys {primary_keys}!"
    print(f"✅ Duplicate values Not found in customers table for keys {primary_keys}")

    null_count = check_null_values(db_connection,validation["target_schema"],validation["target_table"],"event_id")

    assert null_count == 0, (
        f"\n❌ {validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.event_id"
        f" contains {null_count} NULL values!\n"
    )
    print(f"\n{validation['target_db']}.{validation['target_schema']}.{validation['target_table']}.event_id"
        f" contains NO NULL values!\n")





        
    