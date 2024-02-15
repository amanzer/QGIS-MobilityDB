import datetime

start_time = datetime.datetime(2023, 8, 1, 0, 0, 0)
end_time = datetime.datetime(2023, 8, 1, 0, 10, 0)
time_diff = (end_time - start_time) / 50

timestamps = [str(start_time + i * time_diff) for i in range(50)]

# print(type(timestamps[0]))

import subprocess

# Command to execute Program B
command = ['/home/mali/QGIS-MobilityDB/luderic/test/bin/python', '/home/mali/QGIS-MobilityDB/luderic/processB.py', *timestamps]


# Execute the command and capture the output
result = subprocess.run(command, capture_output=True, text=True)

# Get the JSON content from the output
json_content = result.stdout

print( json_content)