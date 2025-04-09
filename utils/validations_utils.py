import logging

def check_null_values(connection, schema_name, table_name, column_name):
    """
    Checks the number of NULL values in a specific column of a table.
    Author : Shilpi Jha
    Args:
        connection: psycopg2 database connection object.
        schema_name (str): The schema name.
        table_name (str): The table name.
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
