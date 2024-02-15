import sys
from datetime import datetime
from tqdm import tqdm

print("B")

# Frames
timestamps = sys.argv[1:]
datetimes = [datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S') for timestamp in timestamps]

# rows


import pandas as pd
import json
from pymeos import *

pymeos_initialize()

ais = pd.read_csv(
    "aisdk-2023-08-01.zip",
    usecols=["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG"], nrows=100
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


features = {}

for frame in tqdm(datetimes):
    for index, row in tqdm(trajectories.iterrows(), total=len(trajectories)):
        try:
            val = row["trajectory"].value_at_timestamp(time)
        except Exception as e:
            val = None  

        if features[row["mmsi"]] is None:
            features[row["mmsi"]] = []
        features[row["mmsi"]].append(val)


data = {
    timestamp: [(trip[0], trip[1]) for trip in trips]
    for timestamp, trips in features.items()
}

df = pd.DataFrame.from_dict(data, orient='index', columns=['mmsi', 'val'])

# Package the DataFrame and the additional variable into a dictionary
data_package = {
    'value': df
}

# Serialize the dictionary to a JSON string
json_str = json.dumps(data_package)


# flushing output
import sys
sys.stdout.flush()
