import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.db_connector import *
from utils.validations_utils import *


if __name__ == "__main__":
    # Database Connection
    DB_NAME = "regcor_refine_db"  #DB in YAML config

    conn = get_connection(DB_NAME)
    os.system('cls' if os.name == 'nt' else 'clear')
    validate_connection(DB_NAME)

    # DB_NAME_2 = "regcor_source_GMDF_db" 
    # conn = get_connection(DB_NAME_2)
    # validate_connection(DB_NAME_2)

    
    # DB_NAME_3 = "regcor_source_RIM_REF_db" 
    # conn = get_connection(DB_NAME_3)
    # validate_connection(DB_NAME_3)

    # DB_NAME_3 = "regcor_source_RDM_db" 
    # conn = get_connection(DB_NAME_3)
    # validate_connection(DB_NAME_3)

nulls = check_null_values(conn, "regcor_refine", "stg_dim_regcor_registration", "registration_id")

if nulls == 0:
    print("✅ No NULL values found.")
elif nulls > 0:
    print(f"❌ Found {nulls} NULL values in the column.")
else:
    print("⚠️ Error occurred during NULL check.")

conn.close()
 

