import logging

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


def check_data_completeness(connection, schema_name, source_table_name,target_table_name, source_column_name,target_column_name):
    """
    Checks the number of NULL values in a specific column of a table.
    Author : Shilpi Jha
    Args:
        connection: psycopg2 database connection object.
        schema_name (str): The schema name.
        source_table_name (str): The table name.
        target_table_name (str): The table name.
        source_column_name (str): The source column to check.
        target_column_name (str): The column to check.
    Returns:
        int: The number of NULL values found in the column.
    """
    try:
        with connection.cursor() as cursor:
            query = f"""
                SELECT DISTINCT f.{target_column_name}
                FROM {target_table_name} f
                LEFT JOIN {source_table_name} r ON f.{target_column_name} = r.{source_column_name}
                WHERE r.id IS NULL;
            """
            cursor.execute(query)
            null_count = cursor.fetchone()[0]
            return null_count
    except Exception as e:
        logging.error(f"Error checking completeness in {schema_name}.{target_table_name}.{target_column_name}: {str(e)}")
        return -1
