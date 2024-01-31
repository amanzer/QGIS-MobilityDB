import pandas as pd
from pymeos import *

pymeos_initialize()


ais = pd.read_csv('aisdk-2023-08-01.csv', nrows=500)
ais['point'] = ais.apply(lambda row: TGeogPointInst(point=(row['Latitude'], row['Longitude']), timestamp=row['# Timestamp']),
                        axis=1)

print(ais["point"].head(10))                        
print("ok")


# import pandas as pd
# from pymeos import *

# pymeos_initialize()
# try:

#     ais = pd.read_csv('aisdk-2023-08-01.csv', nrows=1000)
#     ais['point'] = ais.apply(lambda row: TGeogPointInst(point=(row['latitude'], row['longitude']), timestamp=row['t']),
#                          axis=1)
                         
#     ais['sog'] = ais.apply(lambda row: TFloatInst(value=row['sog'], timestamp=row['t']), axis=1)
#     ais.drop(['latitude', 'longitude'], axis=1, inplace=True)
#     trajectories = ais.groupby('mmsi').aggregate(
#     {
#         'point': TGeogPointSeq.from_instants,
#         'sog': TFloatSeq.from_instants
#     }
#     ).rename({'point': 'trajectory'}, axis=1)
#     trajectories['distance'] = trajectories['trajectory'].apply(lambda t: t.length())
    

# except Exception as e:
#     print(e)
    