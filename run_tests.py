# run_tests.py
import os
import pytest
from datetime import datetime

# # Create a timestamped report file
# timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# report_path = f"reports/test_report_{timestamp}.html"

# # Run pytest with HTML report
# pytest.main([
#     "tests", 
#     "--html=" + report_path,
#     "--self-contained-html"
# ])

import os
from datetime import datetime

if __name__ == "__main__":
    os.system(f"pytest tests --html=reports/test_report_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.html --self-contained-html")
