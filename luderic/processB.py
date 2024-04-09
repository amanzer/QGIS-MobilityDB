import sys
from datetime import datetime
from tqdm import tqdm
import time
import pandas as pd
import json
from pymeos import *



mmsi = int(sys.argv[1])
# Frames
timestamps = sys.argv[2:]
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

cursor.execute(f"SELECT * FROM public.PyMEOS_demo WHERE MMSI = {mmsi} ;")
mmsi, trajectory, sog = cursor.fetchone()
#connection.commit()
cursor.close()



interpolation_times = []

features = [[] for _ in range(len(datetimes))]


for i, datetime_obj in tqdm(enumerate(datetimes), total=len(datetimes)):
    try :
        now2 = time.time()
        val = trajectory.value_at_timestamp(datetime_obj)

        interpolation_times.append(time.time()-now2)
        # features[i].append(( row.name , [val[0], val[1]]))
        features[i].append({"mmsi": mmsi, "coordinates": [val.x, val.y]})
    except Exception as e: 
        val = None



pymeos_finalize()
#print(interpolation_times)
# Serialize the list into a JSON string
json_string = json.dumps(features)



# Output the JSON string to stdout
sys.stdout.write(json_string)

###########################################


# ais = pd.read_csv(
#     "aisdk-2023-06-01.zip",
#     usecols=["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG"],
# )
# ais.columns = ["t", "mmsi", "lat", "lon", "sog"]

# #ais = ais["mmsi"].unique()[:10]
# ais = ais[ais["mmsi"] == mmsi]

# ais["t"] = pd.to_datetime(ais["t"], format='%d/%m/%Y %H:%M:%S')
# ais = ais[ais["mmsi"] != 0]
# ais = ais.drop_duplicates(["t", "mmsi"])
# ais = ais[(ais["lat"] >= 40.18) & (ais["lat"] <= 84.17)]
# ais = ais[(ais["lon"] >= -16.1) & (ais["lon"] <= 32.88)]
# ais = ais[(ais["sog"] >= 0) & (ais["sog"] <= 1022)]
# ais.dropna()
# ais.head()
# ais["instant"] = ais.apply(lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),axis=1)
# ais["instant"] = ais.apply(
#     lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),
#     axis=1,
# )

# ais.drop(["lat", "lon"], axis=1, inplace=True)
# trajectories = (
#     ais.groupby("mmsi")
#     .aggregate(
#         {
#             "instant": lambda x: TGeogPointSeq.from_instants(x, upper_inc=True),
#         }
#     )
#     .rename({"instant": "trajectory"}, axis=1)
# )
# trajectories["distance"] = trajectories["trajectory"].apply(lambda t: t.length())

# interpolation_times = []

# features = [[] for _ in range(len(datetimes))]


# for i, datetime_obj in tqdm(enumerate(datetimes), total=len(datetimes)):
#     for index, row in tqdm(trajectories.iterrows(), total=len(trajectories)):
#         try :
#             now2 = time.time()
#             val = row["trajectory"].value_at_timestamp(datetime_obj)

#             interpolation_times.append(time.time()-now2)
#             # features[i].append(( row.name , [val[0], val[1]]))

#             features[i].append({"mmsi": row.name, "coordinates": [val.x, val.y]})
#         except Exception as e: 
#             val = None



# pymeos_finalize()
# #print(interpolation_times)
# # Serialize the list into a JSON string
# json_string = json.dumps(features)



# # Output the JSON string to stdout
# sys.stdout.write(json_string)

