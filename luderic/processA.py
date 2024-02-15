import datetime
import json
import subprocess

start_time = datetime.datetime(2023, 8, 1, 0, 0, 0)
end_time = datetime.datetime(2023, 8, 1, 0, 10, 0)
time_diff = (end_time - start_time) / 50

timestamps = [str(start_time + i * time_diff) for i in range(50)]

# print(type(timestamps[0]))



# Command to execute Program B
command = ['/home/mali/QGIS-MobilityDB/luderic/test/bin/python', '/home/mali/QGIS-MobilityDB/luderic/processB.py', *timestamps]



# Execute the command and capture the output
result = subprocess.run(command, capture_output=True, text=True)



# Check if the command ran successfully
if result.returncode == 0:
    # Assuming the output from processB.py is a JSON string
    output_json = result.stdout
    print("Captured output:", result.stdout)

    # Deserialize the JSON string into Python objects (e.g., dictionary, list)
    output_data = json.loads(output_json)
    
    # Now you can work with the deserialized data as needed
    print(output_data)
else:
    # Print the error message if the command did not run successfully
    print("Error occurred:", result.stderr)