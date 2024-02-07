import subprocess
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
            "sog": lambda x: TFloatSeq.from_instants(x, upper_inc=True),
        }
    )
    .rename({"instant": "trajectory"}, axis=1)
)
trajectories["distance"] = trajectories["trajectory"].apply(lambda t: t.length())







###
# Creating a layer with temporal controller, then adding a geometry feature
###

canvas = iface.mapCanvas()
temporalController = canvas.temporalController()
currentFrameNumber = temporalController.currentFrameNumber()

## Create a temporal layer (variable 'vlayer') with single field for datetime
vlayer = QgsVectorLayer("Point", "points_3", "memory")
pr = vlayer.dataProvider()
pr.addAttributes([QgsField("time", QVariant.DateTime)])
vlayer.updateFields()
tp = vlayer.temporalProperties()
tp.setIsActive(True)
#tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFixedTemporalRange)
#tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)

#tp.setMode(1) #single field with datetime
tp.setStartField("time")
crs = vlayer.crs()
crs.createFromId(22992)
vlayer.setCrs(crs)
vlayer.updateFields()
QgsProject.instance().addMapLayer(vlayer)

features_list = []


###

## Populate a layer stored in variable 'vlayer' with features using rows stored in variable 'rows'
## MAKE SURE to run import_rows_to_memory_using_driver.py and create_temporal_layer.py before
## running this script
import time
now = time.time()
FRAMES_NB = 50 # Number of frames to generate
#canvas = iface.mapCanvas()
#temporalController = canvas.temporalController()
#currentFrameNumber = temporalController.currentFrameNumber()
#features_list = []
interpolation_times = []
feature_times = []


######## ==> Build le dataframe row ici puis créer un subprocess B à appeler )à chaque frame gen

# For every frame, use  mobility driver to retrieve valueAtTimestamp(frameTime) and create a corresponding feature
for i in range(FRAMES_NB):
    dtrange = temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
    for index, row in trajectories.iterrows():
        now2 = time.time()

        # Define the command to run Program B
        command = ["python", "value_at_ts.py"]
        # Run Program B and capture its output
        result = subprocess.run(command,args=[row["trajectory"], dtrange.begin().toPyDateTime().replace(tzinfo=row[0].startTimestamp.tzinfo)] , stdout=subprocess.PIPE, text=True)

        # Deserialize the JSON output back into a Python dictionary
        data_package = json.loads(result.stdout)

        # Extract the DataFrame
        val = data_package['value']


        interpolation_times.append(time.time()-now2)
        if val: # If interpolation succeeds
            now3 = time.time()
            feat = QgsFeature(vlayer.fields())   # Create feature
            feat.setAttributes([dtrange.end()])  # Set its attributes
            geom = QgsGeometry.fromPointXY(QgsPointXY(val[0],val[1])) # Create geometry from valueAtTimestamp
            feat.setGeometry(geom) # Set its geometry
            feature_times.append(time.time()-now3)
            features_list.append(feat)
        
now4 = time.time()
vlayer.startEditing()
vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)
now5 = time.time()

print("Total time:", time.time()-now, "s.")
print("Add features time:", now5-now4, "s.") # Time to add features to the map
print("Interpolation:", sum(interpolation_times), "s.")
print("Number of features generated:", len(features_list))

###

