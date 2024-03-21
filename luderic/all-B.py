import sys
from datetime import datetime
from tqdm import tqdm
import time
import pandas as pd
import json
from pymeos import *




# Frames
timestamps = sys.argv[1:]

datetimes = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps]


# rows


from pymeos.db.psycopg import MobilityDB

pymeos_initialize()

host = "localhost"
port = 5432
db = "mobilitydb"
user = "postgres"
password = "postgres"

connection = MobilityDB.connect(
    host=host, port=port, dbname=db, user=user, password=password
)
cursor = connection.cursor()

cursor.execute(f"SELECT MMSI FROM public.PyMEOS_demo;")
mmsi_list = cursor.fetchall()

mmsi_list = mmsi_list[:100]
rows = {}

for mmsi in mmsi_list:
    ship_mmsi = mmsi[0]
    cursor.execute(f"SELECT * FROM public.PyMEOS_demo WHERE MMSI = {ship_mmsi} ;")
    _, trajectory, sog = cursor.fetchone()
    rows[mmsi] = trajectory

cursor.close()



interpolation_times = []



# Create features dictionary
features = {dt: [] for dt in timestamps}


for mmsi in mmsi_list:

    for j, datetime_obj in tqdm(enumerate(datetimes), total=len(datetimes)):
        try :
            now2 = time.time()
            val = rows[mmsi].value_at_timestamp(datetime_obj)

            interpolation_times.append(time.time()-now2)
            # features[i].append(( row.name , [val[0], val[1]]))
            features[datetime_obj.strftime('%Y-%m-%d %H:%M:%S')].append((val.x, val.y))
            
        except Exception as e: 
            val = None




pymeos_finalize()
#print(interpolation_times)
# Serialize the list into a JSON string
json_string = json.dumps(features)



# Output the JSON string to stdout
sys.stdout.write(json_string)