import sys
from datetime import datetime
from tqdm import tqdm


# Frames
timestamps = sys.argv[1:]
datetimes = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps]

# rows
import time

import pandas as pd
import json
from pymeos import *

pymeos_initialize()

ais = pd.read_csv(
    "aisdk-2023-08-01.zip",
    usecols=["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG"], nrows=10000
)
ais.columns = ["t", "mmsi", "lat", "lon", "sog"]

ais["t"] = pd.to_datetime(ais["t"], format='%d/%m/%Y %H:%M:%S')
ais = ais[ais["mmsi"] != 0]
ais = ais.drop_duplicates(["t", "mmsi"])
ais = ais[(ais["lat"] >= 40.18) & (ais["lat"] <= 84.17)]
ais = ais[(ais["lon"] >= -16.1) & (ais["lon"] <= 32.88)]
ais = ais[(ais["sog"] >= 0) & (ais["sog"] <= 1022)]
ais.dropna()
ais.head()
ais["instant"] = ais.apply(lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),axis=1)
ais["instant"] = ais.apply(
    lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),
    axis=1,
)

ais.drop(["lat", "lon"], axis=1, inplace=True)
trajectories = (
    ais.groupby("mmsi")
    .aggregate(
        {
            "instant": lambda x: TGeogPointSeq.from_instants(x, upper_inc=True),
        }
    )
    .rename({"instant": "trajectory"}, axis=1)
)
trajectories["distance"] = trajectories["trajectory"].apply(lambda t: t.length())

interpolation_times = []

features = [[] for _ in range(len(datetimes))]


for i, datetime_obj in tqdm(enumerate(datetimes), total=len(datetimes)):
    for index, row in tqdm(trajectories.iterrows(), total=len(trajectories)):
        try:
            now2 = time.time()
            val = row["trajectory"].value_at_timestamp(datetimes[i])
            interpolation_times.append(time.time()-now2)
            features[i].append(( row.name , (val[0], val[1])))
        except Exception as e:
            val = None  

#print(interpolation_times)
# Serialize the list into a JSON string
json_string = json.dumps(features)



# Output the JSON string to stdout
sys.stdout.write(json_string)