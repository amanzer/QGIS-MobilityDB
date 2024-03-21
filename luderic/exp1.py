import subprocess
import pandas as pd
import json
import time
import datetime
import time
###
# Creating a layer with temporal controller, then adding a geometry feature
###

canvas = iface.mapCanvas()
temporalController = canvas.temporalController()

# Define the new start and end dates
new_start_date = QDateTime.fromString("2024-01-01T00:00:00", Qt.ISODate)
new_end_date = QDateTime.fromString("2024-12-31T23:59:59", Qt.ISODate)

# Create a QgsDateTimeRange object with the new start and end dates
new_temporal_range = QgsDateTimeRange(new_start_date, new_end_date)

# Emit the updateTemporalRange signal with the new temporal range
temporalController.updateTemporalRange.emit(new_temporal_range)

currentFrameNumber = temporalController.currentFrameNumber()

## Create a temporal layer (variable 'vlayer') with single field for datetime
vlayer = QgsVectorLayer("Point", "points_3", "memory")
pr = vlayer.dataProvider()
pr.addAttributes([QgsField("time", QVariant.DateTime)])
vlayer.updateFields()
tp = vlayer.temporalProperties()
tp.setIsActive(True)
#tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
#tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFixedTemporalRange)
tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)

#tp.setMode(1) #single field with datetime
tp.setStartField("time")
crs = vlayer.crs()
#crs.createFromId(22992)
#vlayer.setCrs(crs)
vlayer.updateFields()
QgsProject.instance().addMapLayer(vlayer)

features_list = []


###

## Populate a layer stored in variable 'vlayer' with features using rows stored in variable 'rows'
## MAKE SURE to run import_rows_to_memory_using_driver.py and create_temporal_layer.py before
## running this script

now = time.time()
FRAMES_NB = 50 # Number of frames to generate

feature_times = []


######## ==> Build le dataframe row ici puis créer un subprocess B à appeler )à chaque frame gen

# For every frame, use  mobility driver to retrieve valueAtTimestamp(frameTime) and create a corresponding feature

timestamps = [308654000]
dtrange_ends = []
for i in range(FRAMES_NB):
    dtrange = temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
    timestamps.append(str(dtrange.begin().toPyDateTime().replace(tzinfo=dtrange.begin().toPyDateTime().tzinfo)))
    dtrange_ends.append(dtrange.end())


# Command to execute Program B
command = ['/home/ali/pymeos/bin/python', '/home/ali/QGIS-MobilityDB/luderic/processB.py', *timestamps]

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

    features_list =[]
    for i in range(len(output_data)):
        #elem = list of each datetime with a dictionnary of mmsi and coordinates
        if len(output_data[i]) > 0:
            feat = QgsFeature(vlayer.fields())   # Create feature
            feat.setAttributes([dtrange_ends[i]])  # Set its attributes
            x,y = output_data[i][0]["coordinates"]
            geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
            feat.setGeometry(geom) # Set its geometry
            features_list.append(feat)

else:
    # Print the error message if the command did not run successfully
    print("Error occurred:", result.stderr)


vlayer.startEditing()
vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)


