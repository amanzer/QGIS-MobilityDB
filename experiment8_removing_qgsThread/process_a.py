import subprocess
import pandas as pd
import json
import time
import datetime
import time


# Command to execute Program B
command = ['/home/ali/.venv/bin/python', '/home/ali/QGIS-MobilityDB/experiment8_removing_qgsThread/all-B.py', "1"]

# Execute the command and capture the output
result = subprocess.run(command, capture_output=True, text=True)


# Assuming the output from processB.py is a JSON string
#output_json = result.stdout
print("Captured output:", result)
