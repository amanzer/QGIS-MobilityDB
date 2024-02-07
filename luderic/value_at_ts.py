from pymeos import *
import json
import sys

traj = sys.argv[1]
time = sys.argv[2]
val = traj.value_at_timestamp(time)
# Package the DataFrame and the additional variable into a dictionary
data_package = {
    'value': val
}

# Serialize the dictionary to a JSON string
json_str = json.dumps(data_package)


# flushing output
import sys
sys.stdout.flush()
