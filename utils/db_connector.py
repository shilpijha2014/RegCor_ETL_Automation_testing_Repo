import os
import yaml
import psycopg2
import logging

def load_db_config(db_name):
    """
    Loads database connection details from the db_config.yaml file.

    Args:
        db_name (str): The name of the database entry in the config file.

    Returns:
        dict: A dictionary containing the database connection parameters.
    """
    # Get absolute path to db_config.yaml relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    config_path = os.path.join(base_dir, "config", "db_config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Config file not found at {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    if "databases" not in config:
        raise ValueError("❌ Invalid config format: Missing 'databases' key.")

    db_config = config["databases"].get(db_name)

    if db_config is None:
        raise ValueError(f"❌ Database config for '{db_name}' not found!")

    return db_config

def get_connection(db_name):
    """
    Establishes and returns a database connection using credentials from the config.

    Args:
        db_name (str): The name of the database entry in the config file.

    Returns:
        connection: A live psycopg2 database connection object.

    Raises:
        psycopg2.OperationalError: If the connection fails due to incorrect credentials or network issues.
    """
    try:
        config = load_db_config(db_name)
        if not config:
            raise ValueError("Database config not found!")

        conn = psycopg2.connect(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            sslmode="require",
            port=config["port"]
        )

        return conn
    except psycopg2.Error as e:
        logging.error(f"❌ Database connection error: {str(e)}")
        return None

def validate_connection(db_name):
    """Validates the database connection."""
    conn = get_connection(db_name)
    if conn:
        print(f"\n**** Connection to '{db_name}' database successful! *****\n")
        conn.close()
        return True
    else:
        print(f"❌ \n**** Connection to '{db_name}' database failed! ****\n")
        return False



