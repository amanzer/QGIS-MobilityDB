import time

# Start the timer
start_time = time.perf_counter()

import pandas as pd
from pymeos import *

pymeos_initialize()

ais = pd.read_csv('aisdk-2023-08-01.csv', nrows=500)
ais['point'] = ais.apply(lambda row: TGeogPointInst(point=(row['Latitude'], row['Longitude']), timestamp=row['# Timestamp']),
                        axis=1)

print(ais["point"].head(10))                        
print("ok")


# End the timer
end_time = time.perf_counter()

# Calculate the duration
duration = end_time - start_time

print(f"The code internally took {duration:.8f} seconds to run.")


# Start the timer
start_time = time.perf_counter()

import subprocess

# Path to the Python interpreter of the target virtual environment
python_interpreter = "/home/ali/QGIS-MobilityDB/luderic/memo/bin/python"

# The script you want to run in the target virtual environment
script_to_run = "test.py"

# Optional: arguments to pass to the script
#arguments = "arg1 arg2"
arguments = ''
# Construct the command
command = f"{python_interpreter} {script_to_run} {arguments}"

# Run the command and wait for it to complete
process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Get standard output and error
stdout = process.stdout.decode()
stderr = process.stderr.decode()

# Optional: print output and error
print(stdout)

# End the timer
end_time = time.perf_counter()

# Calculate the duration
duration = end_time - start_time

print(f"The code with subprocess took {duration:.8f} seconds to run.")
