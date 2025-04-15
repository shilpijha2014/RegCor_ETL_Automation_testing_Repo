import logging

def validate_table_exists(conn, schema, table):
    """
    ✅ Validate if a table exists in the given schema.

    Args:
        conn: Database connection object.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        bool: True if table exists, False otherwise.
    """
    cursor = conn.cursor()
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """
    cursor.execute(query, (schema, table))
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def check_null_values(connection, schema_name, table_name, column_name):
    """
    Checks the number of NULL values in a specific column of a table.
    Author : Shilpi Jha
    Args:
        connection: psycopg2 database connection object.
        schema_name (str): The schema name.
        table_name (str): The source table name.
        column_name (str): The column to check for NULLs.

    Returns:
        int: The number of NULL values found in the column.
    """
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT COUNT(*) 
                FROM {schema_name}.{table_name}
                WHERE {column_name} IS NULL;
            """
            cursor.execute(query)
            null_count = cursor.fetchone()[0]
            return null_count
    except Exception as e:
        logging.error(f"Error checking NULLs in {schema_name}.{table_name}.{column_name}: {str(e)}")
        return -1

def validate_row_count_match(source_conn, target_conn, source_schema, source_table, target_schema, target_table):
    """
    🔁 Validate if row count between source and target tables is the same.

    Args:
        source_conn: Connection to source DB.
        target_conn: Connection to target DB.
        source_schema (str): Source schema name.
        source_table (str): Source table name.
        target_schema (str): Target schema name.
        target_table (str): Target table name.

    Returns:
        (bool, int, int): Match result, source count, target count.
    """
    src_cursor = source_conn.cursor()
    tgt_cursor = target_conn.cursor()

    src_cursor.execute(f"SELECT COUNT(*) FROM {source_schema}.{source_table};")
    tgt_cursor.execute(f"SELECT COUNT(*) FROM {target_schema}.{target_table};")

    src_count = src_cursor.fetchone()[0]
    tgt_count = tgt_cursor.fetchone()[0]

    src_cursor.close()
    tgt_cursor.close()

    return src_count == tgt_count, src_count, tgt_count

import logging

# def check_data_completeness(
#     conn_src, conn_tgt,
#     src_schema, src_table, src_key,
#     tgt_schema, tgt_table, tgt_key
# ):
#     """
#     Validates data completeness between source and target using a LEFT JOIN,
#     allowing different key column names.

#     Args:
#         conn_src: psycopg2 connection object to source DB
#         conn_tgt: psycopg2 connection object to target DB
#         src_schema (str): Source schema name
#         src_table (str): Source table name
#         src_key (str): Source key column name
#         tgt_schema (str): Target schema name
#         tgt_table (str): Target table name
#         tgt_key (str): Target key column name

#     Returns:
#         Tuple: (passed: bool, missing_count: int, message: str)
#     """
#     try:
#         cursor = conn_src.cursor()

#         query = f"""
#         SELECT COUNT(*) 
#         FROM {tgt_schema}.{tgt_table} tgt
#         LEFT JOIN {src_schema}.{src_table} src
#         ON tgt.{tgt_key} = src.{src_key}
#         WHERE src.{src_key} IS NULL;
#         """

#         cursor.execute(query)
#         missing_count = cursor.fetchone()[0]
#         cursor.close()

#         if missing_count == 0:
#             message = f"\n✅ All records in {src_schema}.{src_table} exist in {tgt_schema}.{tgt_table}."
#             logging.info(message)
#             print(message)
#             return True, 0, message
#         else:
#             message = f"❌ {missing_count} records in {src_schema}.{src_table} are missing in {tgt_schema}.{tgt_table}."
#             logging.warning(message)
#             print(message)
#             return False, missing_count, message

#     except Exception as e:
#         error_msg = f"❌ Error during completeness check: {str(e)}"
#         logging.error(error_msg)
#         print(error_msg)
#         return False, -1, error_msg

import psycopg2
import logging

def check_data_completeness(connection, src_schema, src_table, src_key, 
                            tgt_schema, tgt_table, tgt_key):
    """
    Validates data completeness between source and target tables using LEFT JOINs in both directions.
    
    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        src_key (str): Primary/Join key in the source table.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        tgt_key (str): Primary/Join key in the target table.
    
    Returns:
        dict: A summary of missing records.
    """
    cursor = connection.cursor()

    # Check: Records in Source but missing in Target
    src_to_tgt_query = f"""
        SELECT COUNT(*) 
        FROM {src_schema}.{src_table} src
        LEFT JOIN {tgt_schema}.{tgt_table} tgt
        ON src.{src_key} = tgt.{tgt_key}
        WHERE tgt.{tgt_key} IS NULL;
    """
    
    # Check: Records in Target but missing in Source
    tgt_to_src_query = f"""
        SELECT COUNT(*) 
        FROM {tgt_schema}.{tgt_table} tgt
        LEFT JOIN {src_schema}.{src_table} src
        ON tgt.{tgt_key} = src.{src_key}
        WHERE src.{src_key} IS NULL;
    """

    cursor.execute(src_to_tgt_query)
    missing_in_target = cursor.fetchone()[0]

    cursor.execute(tgt_to_src_query)
    missing_in_source = cursor.fetchone()[0]

    cursor.close()

    # Logging Results
    if missing_in_target == 0 and missing_in_source == 0:
        logging.info(f"✅ Data completeness check PASSED: No missing records between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}.")
        return True, 0, message
    else:
        logging.warning(f"⚠️ Data completeness check FAILED:")
        logging.warning(f"👉 Records present in Source but missing in Target: {missing_in_target}")
        logging.warning(f"👉 Records present in Target but missing in Source: {missing_in_source}")

    return {
        "missing_in_target": missing_in_target,
        "missing_in_source": missing_in_source
    }
