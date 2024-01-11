
import pandas as pd
from pymeos import *

pymeos_initialize()

ais = pd.read_csv(
    "aisdk-2023-08-01.zip",
    usecols=["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG"],
)
ais.columns = ["t", "mmsi", "lat", "lon", "sog"]
ais = ais[ais["mmsi"] == 219020187]
ais = ais[ais["t"] != 0]
ais["t"] = pd.to_datetime(ais["t"], format='%d/%m/%Y %H:%M:%S')
ais = ais[ais["mmsi"] != 0]
ais = ais.drop_duplicates(["t", "mmsi"])
ais = ais[(ais["lat"] >= 40.18) & (ais["lat"] <= 84.17)]
ais = ais[(ais["lon"] >= -16.1) & (ais["lon"] <= 32.88)]
ais = ais[(ais["sog"] >= 0) & (ais["sog"] <= 1022)]
ais.dropna()
ais.head()

ais["instant"] = ais.apply(lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),axis=1)
ais["sog"] = ais.apply(
    lambda row: TFloatInst(value=row["sog"], timestamp=row["t"]), axis=1
)

ais["instant"] = ais.apply(
    lambda row: TGeogPointInst(point=(row["lon"], row["lat"]), timestamp=row["t"]),
    axis=1,
)
ais["sog"] = ais.apply(
    lambda row: TFloatInst(value=row["sog"], timestamp=row["t"]), axis=1
)
ais.drop(["lat", "lon"], axis=1, inplace=True)

trajectories = (
    ais.groupby("mmsi")
    .aggregate(
        {
            "instant": lambda x: TGeogPointSeq.from_instants(x, upper_inc=True),
            "sog": lambda x: TFloatSeq.from_instants(x, upper_inc=True),
        }
    )
    .rename({"instant": "trajectory"}, axis=1)
)
trajectories["distance"] = trajectories["trajectory"].apply(lambda t: t.length())
rows = [ais.iloc[0]]
