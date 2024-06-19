from datetime import datetime, timedelta

# import subprocess
import pandas as pd
import json
import time
import time
###
# Creating a layer with temporal controller, then adding a geometry feature
###


canvas = iface.mapCanvas()
temporalController = canvas.temporalController()

frame_rate = 30
temporalController.setFramesPerSecond(frame_rate)



# Define the new start and end dates
new_start_date = QDateTime.fromString("2024-01-01T00:00:00", Qt.ISODate)
new_end_date = QDateTime.fromString("2024-12-31T23:59:59", Qt.ISODate)

# Create a QgsDateTimeRange object with the new start and end dates
new_temporal_range = QgsDateTimeRange(new_start_date, new_end_date)

# Emit the updateTemporalRange signal with the new temporal range
temporalController.updateTemporalRange.emit(new_temporal_range)

currentFrameNumber = temporalController.currentFrameNumber()

## Create a temporal layer (variable 'vlayer') with single field for datetime
vlayer = QgsVectorLayer("Point", "points_4", "memory")
pr = vlayer.dataProvider()
pr.addAttributes([QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
vlayer.updateFields()
tp = vlayer.temporalProperties()
tp.setIsActive(True)
#tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
#tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFixedTemporalRange)
# tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTiset_timeseries_windowsmeInstantFromField)
tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
#tp.setMode(1) #single field with datetime
tp.setStartField("start_time")
tp.setEndField("end_time")
crs = vlayer.crs()
#crs.createFromId(22992)
#vlayer.setCrs(crs)
vlayer.updateFields()


url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"

# map_layer = QgsRasterLayer(url, "OpenStreetMap", "wms")

QgsProject.instance().addMapLayer(vlayer)

features_list =[]





start_datetime_obj = QDateTime(datetime(2023,6,1,0,0,0))

end_datetime_obj = QDateTime(datetime(2023,6,1,23,0,0))




feat = QgsFeature(vlayer.fields())   # Create feature
feat.setAttributes([start_datetime_obj, end_datetime_obj])  # Set its attributes
x,y = (1,1)
geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
feat.setGeometry(geom) # Set its geometry




vlayer.startEditing()
vlayer.addFeatures([feat]) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)