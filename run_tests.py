# run_tests.py
import os
from datetime import datetime

if __name__ == "__main__":
    os.system(f"pytest tests --html=reports/test_report_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.html --self-contained-html")
