🚀 ETL Testing Automation Framework
This ETL Testing Automation Framework is designed to validate the integrity and quality of data as it moves from a source system to a target system. It supports validations such as table existence, row counts, null checks, and record mismatches — without using pandas.

📦 Features
✅ Validate table existence in source and target

✅ Compare row counts between source and target tables

✅ Detect missing records in either direction using primary key(s)

✅ Null value checks on critical columns

✅ Supports YAML-driven test inputs

✅ HTML test reports via pytest-html

✅ Modular and reusable functions

✅ No pandas dependency — optimized for performance and memory

🏗️ Project Structure
bash
Copy
Edit
etl_testing_framework/
│
├── config/
│   └── db_config.yaml         # Database connection details
│
├── utils/
│   ├── db_connector.py        # Functions to connect to DB
│   └── validation_functions.py# Core reusable test functions
│
├── tests/
│   ├── test_nulls.py          # Sample test for null validation
│   ├── test_row_counts.py     # Sample test for row count validation
│   └── test_keys.py           # Sample test for missing record validation
│
├── reports/                   # Generated HTML test reports
│
└── run_tests.py               # Script to run all tests with report
⚙️ Setup Instructions
Clone the repository

bash
Copy
Edit
git clone <your_repo_url>
cd etl_testing_framework
Create and activate a virtual environment

bash
Copy
Edit
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux
Install dependencies

bash
Copy
Edit
pip install -r requirements.txt
Configure database connections Edit config/db_config.yaml with your database details:

yaml
Copy
Edit
databases:
  source_db:
    host: "hostname"
    port: 5432
    database: "source"
    user: "user"
    password: "password"
  target_db:
    ...
🧪 Running Tests
To run all tests and generate an HTML report:

bash
Copy
Edit
python run_tests.py
The report will be saved in the reports/ folder.