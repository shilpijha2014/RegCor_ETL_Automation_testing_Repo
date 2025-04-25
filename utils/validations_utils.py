import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

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
        logging.error(f"\nError checking NULLs in {schema_name}.{table_name}.{column_name}: {str(e)}")
        return -1

def check_columns_for_nulls(connection, schema, table, columns):
    """
    Checks if any of the specified columns in a table contain NULL values.

    Args:
        connection: Active DB connection object.
        schema (str): Schema name.
        table (str): Table name.
        columns (list): List of column names to check.

    Returns:
        dict: Dictionary with column names as keys and NULL counts as values.
              If a column has 0 nulls, its value will be 0.
    """
    null_results = {}
    try:
        cursor = connection.cursor()
        for column in columns:
            query = f"""
                SELECT COUNT(*) 
                FROM {schema}.{table}
                WHERE {column} IS NULL;
            """
            cursor.execute(query)
            null_count = cursor.fetchone()[0]
            null_results[column] = null_count
        return null_results
    except Exception as e:
        return {"error": str(e)}


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
    print(f"Source to Target Query: {src_to_tgt_query}")
    # Check: Records in Target but missing in Source
    tgt_to_src_query = f"""
        SELECT COUNT(*) 
        FROM {tgt_schema}.{tgt_table} tgt
        LEFT JOIN {src_schema}.{src_table} src
        ON tgt.{tgt_key} = src.{src_key}
        WHERE src.{src_key} IS NULL;
    """
    print(f"Target to Source Query: {tgt_to_src_query}")
    cursor.execute(src_to_tgt_query)
    missing_in_target = cursor.fetchone()[0]

    cursor.execute(tgt_to_src_query)
    missing_in_source = cursor.fetchone()[0]

    cursor.close()

    # Logging Results
    if missing_in_target == 0 and missing_in_source == 0:
        logging.info(f"✅ Data completeness check PASSED: No missing records between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}.")
        return True, 0
    else:
        logging.warning(f"⚠️ Data completeness check FAILED:")
        logging.warning(f"👉 Records present in Source but missing in Target: {missing_in_target}")
        logging.warning(f"👉 Records present in Target but missing in Source: {missing_in_source}")

    return {
        "missing_in_target": missing_in_target,
        "missing_in_source": missing_in_source
    }

def check_col_data_completeness_src_to_tgt(connection, src_schema, src_table, src_key, 
                                tgt_schema, tgt_table, tgt_key):
    """
    Validates data completeness between source to target tables using LEFT JOINs
    and distinct key comparison.

    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        src_key (str): Primary/Join key in the source table.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        tgt_key (str): Primary/Join key in the target table.

    Returns:
        tuple: (bool, int, str) indicating success, number of missing records, and message.
    """
    try:
        cursor = connection.cursor()

        # Find records in Target but missing in Source
        src_to_tgt_query = f"""
            SELECT DISTINCT tgt.{tgt_key}
            FROM {tgt_schema}.{tgt_table} tgt
            LEFT JOIN {src_schema}.{src_table} src
            ON src.{src_key} = tgt.{tgt_key}
            WHERE src.{src_key} IS NULL and tgt.{tgt_key} ='No Source Value';
        """

        cursor.execute(src_to_tgt_query)
        missing_in_target = cursor.fetchall()

        count_missing_in_target = len(missing_in_target)

        if count_missing_in_target == 0:
            message = f"✅ \nData completeness passed: No missing records between {src_table} and {tgt_table}."
            logging.info(message)
            print(message)
            return True, 0, message
        else:
            message = (f"❌ \nData completeness failed:\n"
                       f"Missing in Target: {count_missing_in_target} keys\n")
            logging.warning(message)
            print(message)
            return False, count_missing_in_target, message

    except Exception as e:
        error_message = f"❌ \nError during completeness check: {str(e)}"
        logging.error(error_message)
        print(error_message)
        return False, -1, error_message

    finally:
        if 'cursor' in locals():
            cursor.close()

def check_col_data_completeness_tgt_to_src(connection, src_schema, src_table, src_key, 
                                tgt_schema, tgt_table, tgt_key):
    """
    Validates data completeness between target to source tables using LEFT JOINs
    and distinct key comparison.

    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        src_key (str): Primary/Join key in the source table.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        tgt_key (str): Primary/Join key in the target table.

    Returns:
        tuple: (bool, int, str) indicating success, number of missing records, and message.
    """
    try:
        cursor = connection.cursor()

        # Find records in Source but missing in Target
        tgt_to_src_query = f"""
            SELECT DISTINCT src.{src_key}
            FROM {src_schema}.{src_table} src
            LEFT JOIN {tgt_schema}.{tgt_table} tgt
            ON tgt.{tgt_key} = src.{src_key}
            WHERE tgt.{tgt_key} ='No Source Value';
        """

        cursor.execute(tgt_to_src_query)
        missing_in_source = cursor.fetchall()

        count_missing_in_source = len(missing_in_source)

        if count_missing_in_source == 0:
            message = f"✅ \nData completeness passed: No missing records between {src_table} and {tgt_table}."
            logging.info(message)
            print(message)
            return True, 0, message
        else:

            message = (f"❌ \nData completeness failed:\n"
                       f"Missing in Source: {count_missing_in_source} keys\n")
            logging.warning(message)
            print(message)
            return False, count_missing_in_source, message

    except Exception as e:
        error_message = f"❌ \nError during completeness check: {str(e)}"
        logging.error(error_message)
        print(error_message)
        return False, -1, error_message

    finally:
        if 'cursor' in locals():
            cursor.close()

def check_col_data_completeness(connection, src_schema, src_table, src_key, 
                                tgt_schema, tgt_table, tgt_key):
    """
    Validates data completeness between source and target tables using LEFT JOINs
    and distinct key comparison.

    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        src_key (str): Primary/Join key in the source table.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        tgt_key (str): Primary/Join key in the target table.

    Returns:
        tuple: (bool, int, str) indicating success, number of missing records, and message.
    """
    try:
        cursor = connection.cursor()

        # Find records in Target but missing in Source
        src_to_tgt_query = f"""
            SELECT DISTINCT tgt.{tgt_key}
            FROM {tgt_schema}.{tgt_table} tgt
            LEFT JOIN {src_schema}.{src_table} src
            ON src.{src_key} = tgt.{tgt_key}
            WHERE src.{src_key} IS NULL and tgt.{tgt_key} = 'No Source Value';
        """

        # Find records in Source but missing in Target
        tgt_to_src_query = f"""
            SELECT DISTINCT src.{src_key}
            FROM {src_schema}.{src_table} src
            LEFT JOIN {tgt_schema}.{tgt_table} tgt
            ON tgt.{tgt_key} = src.{src_key}
            WHERE tgt.{tgt_key} ='No Source Value' ;
        """

        cursor.execute(src_to_tgt_query)
        missing_in_target = cursor.fetchall()

        cursor.execute(tgt_to_src_query)
        missing_in_source = cursor.fetchall()

        count_missing_in_target = len(missing_in_target)
        count_missing_in_source = len(missing_in_source)

        if count_missing_in_target == 0 and count_missing_in_source == 0:
            message = f"✅ \nData completeness passed: No missing records between {src_table} and {tgt_table}."
            logging.info(message)
            print(message)
            return True, 0, message
        else:
            total_missing = count_missing_in_target + count_missing_in_source
            message = (f"❌ \nData completeness failed:\n"
                       f"Missing in Target: {count_missing_in_target} keys\n"
                       f"Missing in Source: {count_missing_in_source} keys\n"
                       f"Total missing: {total_missing}")
            logging.warning(message)
            print(message)
            return False, total_missing, message

    except Exception as e:
        error_message = f"❌ \nError during completeness check: {str(e)}"
        logging.error(error_message)
        print(error_message)
        return False, -1, error_message

    finally:
        if 'cursor' in locals():
            cursor.close()

def check_col_key_data_completeness(connection, src_schema, src_table, src_key, 
                                tgt_schema, tgt_table, tgt_key,src_col,tgt_col):
    """
    Validates data completeness between source and target tables using LEFT JOINs
    and distinct key comparison.

    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        src_key (str): Primary/Join key in the source table.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        tgt_key (str): Primary/Join key in the target table.
        src_col (str): Col to be validated in Target table.
        tgt_col (str): Col to be validated in source table.

    Returns:
        tuple: (bool, int, str) indicating success, number of missing records, and message.
    """
    try:
        cursor = connection.cursor()

        # Find records in Target but missing in Source
        src_to_tgt_query = f"""
            SELECT tgt.{tgt_col}
            FROM {tgt_schema}.{tgt_table} tgt
            LEFT JOIN {src_schema}.{src_table} src
            ON src.{src_key} = tgt.{tgt_key}
            WHERE src.{src_key} IS NULL;
        """

        # Find records in Source but missing in Target
        tgt_to_src_query = f"""
            SELECT src.{src_col}
            FROM {src_schema}.{src_table} src
            LEFT JOIN {tgt_schema}.{tgt_table} tgt
            ON tgt.{tgt_key} = src.{src_key}
            WHERE tgt.{tgt_key} IS NULL;
        """
        print(f"Source to Target Query: {src_to_tgt_query} ")
        cursor.execute(src_to_tgt_query)
        missing_in_target = cursor.fetchall()

        print(f"Target to Source Query: {tgt_to_src_query} ")
        cursor.execute(tgt_to_src_query)
        missing_in_source = cursor.fetchall()

        count_missing_in_target = len(missing_in_target)
        count_missing_in_source = len(missing_in_source)

        if count_missing_in_target == 0 and count_missing_in_source == 0:
            message = f"✅ \nData completeness passed: No missing records between {src_table} and {tgt_table}."
            logging.info(message)
            print(message)
            return True, 0, message
        else:
            total_missing = count_missing_in_target + count_missing_in_source
            message = (f"❌ \nData completeness failed:\n"
                       f"Missing in Target: {count_missing_in_target} keys\n"
                       f"Missing in Source: {count_missing_in_source} keys\n"
                       f"Total missing: {total_missing}")
            logging.warning(message)
            print(message)
            return False, total_missing, message

    except Exception as e:
        error_message = f"❌ \nError during completeness check: {str(e)}"
        logging.error(error_message)
        print(error_message)
        return False, -1, error_message

    finally:
        if 'cursor' in locals():
            cursor.close()


def check_primary_key_duplicates(connection, schema_name, table_name, primary_keys  ):
    """
    Validates if there are duplicate values for the given primary key(s).

    Args:
        connection: Active psycopg2 connection object.
        schema_name (str): Name of the schema.
        table_name (str): Name of the table.
        primary_keys (list): List of primary key columns.

    Returns:
        bool: True if no duplicates, False if duplicates found.
    """
    try:
        cursor = connection.cursor()
        key_columns = ", ".join(primary_keys)

        query = f"""
            SELECT {key_columns}, COUNT(*)
            FROM {schema_name}.{table_name}
            GROUP BY {key_columns}
            HAVING COUNT(*) > 1;
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()

        if duplicates:
            logging.error(f"❌ Duplicate records found in {schema_name}.{table_name} on keys {primary_keys}: {duplicates}")
            return False
        else:
            logging.info(f"✅ No duplicate values found in {schema_name}.{table_name} on keys {primary_keys}.")
            return True

    except Exception as e:
        logging.error(f"❌ Error during primary key duplicate check: {str(e)}")
        return False

def check_referential_integrity(connection, src_schema, src_table, src_column, 
                                tgt_schema, tgt_table, tgt_column):
    """
    Validates referential integrity between source and target tables.
    Ensures all target values have a matching source reference.

    Args:
        connection: Active psycopg2 connection object.
        src_schema (str): Schema of the source table.
        src_table (str): Source table name.
        src_column (str): Source reference column.
        tgt_schema (str): Schema of the target table.
        tgt_table (str): Target table name.
        tgt_column (str): Target foreign key column.

    Returns:
        bool: True if referential integrity is valid, False otherwise.
    """
    try:
        cursor = connection.cursor()

        query = f"""
            SELECT {tgt_column}
            FROM {tgt_schema}.{tgt_table} f
            LEFT JOIN {src_schema}.{src_table} r
            ON f.{tgt_column} = r.{src_column}
            WHERE r.{src_column} IS NULL;
        """
        cursor.execute(query)
        missing_records = cursor.fetchall()

        if missing_records:
            logging.error(
                f"❌ Referential integrity failed! {len(missing_records)} records in {tgt_schema}.{tgt_table}.{tgt_column} "
                f"have no matching entry in {src_schema}.{src_table}.{src_column}."
            )
            return False
        else:
            logging.info(
                f"✅ Referential integrity passed! All {tgt_column} values in {tgt_table} exist in {src_table}."
            )
            return True

    except Exception as e:
        logging.error(f"❌ Error during referential integrity check: {str(e)}")
        return False
    

def check_data_completeness_with_full_join(
    connection,
    src_schema1, src_table1, src_col1,
    src_schema2, src_table2, src_col2,
    tgt_schema, tgt_table, tgt_col,
    join_condition=""
):
    """
    Dynamically validates data completeness using FULL OUTER JOIN with COALESCE and EXCEPT.

    Args:
        connection: Active psycopg2 connection.
        src_schema1 (str): Schema of first source table.
        src_table1 (str): First source table.
        src_col1 (str): Column from first source table.
        src_schema2 (str): Schema of second source table.
        src_table2 (str): Second source table.
        src_col2 (str): Column from second source table.
        tgt_schema (str): Schema of target table.
        tgt_table (str): Target table.
        tgt_col (str): Column from target table.
        join_condition (str): Join condition between source tables.

    Returns:
        tuple: (bool: status, int: count of missing records, str: message)
    """

    try:
        cursor = connection.cursor()

        join_sql = f"ON {join_condition}" if join_condition else ""

        query = f"""
            SELECT a.col
            FROM (
                SELECT COALESCE(r.{src_col1}, g.{src_col2}) AS col
                FROM {src_schema1}.{src_table1} r
                FULL OUTER JOIN {src_schema2}.{src_table2} g
                {join_sql}
                WHERE COALESCE(r.{src_col1}, g.{src_col2}) IS NOT NULL
            ) a
            EXCEPT
            SELECT {tgt_col}
            FROM {tgt_schema}.{tgt_table};
        """

        logging.debug(f"Generated SQL:\n{query}")
        print(f"Generated SQL:\n{query}")
        cursor.execute(query)
        missing_records = cursor.fetchall()

        count = len(missing_records)
        if count == 0:
            message = f"✅ Data completeness passed: All records are present in {tgt_schema}.{tgt_table}"
            logging.info(message)
            return True, 0, message
        else:
            message = f"❌ Data completeness failed: {count} records missing in {tgt_schema}.{tgt_table}"
            logging.error(message)
            return False, count, message

    except Exception as e:
        error_msg = f"❌ Error during completeness check: {str(e)}"
        logging.exception(error_msg)
        return False, -1, error_msg
    finally:
        if cursor:
            cursor.close()
  
def validate_source_to_target_with_filter(
    connection,
    src_schema,
    src_table,
    tgt_schema,
    tgt_table,
    src_cols,
    tgt_cols,
    src_filter=None,
    tgt_filter=None
):
    """
    Validates data completeness between source and target using EXCEPT with optional filters and column mapping.

    Args:
        connection: psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        src_cols (list): List of columns in source table.
        tgt_cols (list): List of corresponding columns in target table.
        src_filter (str, optional): Filter condition for source table.
        tgt_filter (str, optional): Filter condition for target table.

    Returns:
        tuple: (bool, int, str) - status, count of mismatches, message
    """
    try:
        cursor = connection.cursor()

        src_expr = ", ".join(src_cols)
        tgt_expr = ", ".join(tgt_cols)

        src_where = f"WHERE {src_filter}" if src_filter else ""
        tgt_where = f"WHERE {tgt_filter}" if tgt_filter else ""

        query = f"""
            SELECT {src_expr}
            FROM {src_schema}.{src_table}
            {src_where}
            EXCEPT
            SELECT {tgt_expr}
            FROM {tgt_schema}.{tgt_table}
            {tgt_where};
        """

        print(query)
        logging.debug("Executing EXCEPT query:\n%s", query)
        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Data completeness passed: No mismatched records found between {src_table} and {tgt_table}."
            logging.info(message)
            return True, 0, message
        else:
            message = f"❌ Data completeness failed: {diff_count} mismatched records between {src_table} and {tgt_table}."
            logging.error(message)
            return False, diff_count, message

    except Exception as e:
        error_message = f"❌ Error during completeness validation: {str(e)}"
        logging.exception(error_message)
        return False, -1, error_message
    
def validate_target_to_source_with_filter(
    connection,
    src_schema,
    src_table,
    tgt_schema,
    tgt_table,
    src_cols,
    tgt_cols,
    src_filter=None,
    tgt_filter=None
):
    """
    Validates data completeness from target to source using EXCEPT with optional filters and column mapping.

    Args:
        connection: psycopg2 connection object.
        src_schema (str): Source schema name.
        src_table (str): Source table name.
        tgt_schema (str): Target schema name.
        tgt_table (str): Target table name.
        src_cols (list): List of columns in source table.
        tgt_cols (list): List of corresponding columns in target table.
        src_filter (str, optional): Filter condition for source table.
        tgt_filter (str, optional): Filter condition for target table.

    Returns:
        tuple: (bool, int, str) - status, count of mismatches, message
    """
    try:
        cursor = connection.cursor()

        src_expr = ", ".join(src_cols)
        tgt_expr = ", ".join(tgt_cols)

        src_where = f"WHERE {src_filter}" if src_filter else ""
        tgt_where = f"WHERE {tgt_filter}" if tgt_filter else ""

        query = f"""
            SELECT {tgt_expr}
            FROM {tgt_schema}.{tgt_table}
            {tgt_where}
            EXCEPT
            SELECT {src_expr}
            FROM {src_schema}.{src_table}
            {src_where};
        """

        logging.debug("Executing Target-to-Source EXCEPT query:\n%s", query)
        cursor.execute(query)
        differences = cursor.fetchall()
        diff_count = len(differences)

        if diff_count == 0:
            message = f"✅ Target-to-Source check passed: All records from {tgt_table} exist in {src_table}."
            logging.info(message)
            return True, 0, message
        else:
            message = f"❌ Target-to-Source check failed: {diff_count} records in {tgt_table} missing from {src_table}."
            logging.error(message)
            return False, diff_count, message

    except Exception as e:
        error_message = f"❌ Error during target-to-source completeness validation: {str(e)}"
        logging.exception(error_message)
        return False, -1, error_message
    
import logging

def run_and_validate_empty_query(connection, query, description="Query validation"):
    """
    Runs a query and returns True if no records are returned, otherwise False.

    Args:
        connection: Active database connection object (e.g., psycopg2).
        query (str): SQL query to execute.
        description (str): Optional description of the test case.

    Returns:
        tuple: (bool, int, str) - success status, record count, message
    """
    try:
        cursor = connection.cursor()
        logging.info(f"🔍 Running validation query: {description}")
        logging.debug(f"Executing SQL:\n{query}")

        cursor.execute(query)
        results = cursor.fetchall()
        result_count = len(results)

        if result_count == 0:
            message = f"✅ {description} passed: Query returned no records."
            logging.info(message)
            print(message)
            return True, 0, message
        else:
            message = f"❌ {description} failed: Query returned {result_count} records."
            logging.warning(message)
            print(message)
            return False, result_count, message

    except Exception as e:
        error_message = f"❌ Error running validation query: {str(e)}"
        logging.exception(error_message)
        print(error_message)
        return False, -1, error_message

