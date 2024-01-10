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
tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
#tp.setMode(1) #single field with datetime
tp.setStartField("time")
crs = vlayer.crs()
crs.createFromId(22992)
vlayer.setCrs(crs)
vlayer.updateFields()
QgsProject.instance().addMapLayer(vlayer)

features_list = []


  
for i in range(50):
    dtrange = temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
    feat = QgsFeature(vlayer.fields())   # Create feature
    feat.setAttributes([dtrange.end()])  # Set its attributes
    geom = QgsGeometry.fromPointXY(QgsPointXY(i, i)) # Create geometry from valueAtTimestamp
    feat.setGeometry(geom) # Set its geometry
    features_list.append(feat)
        

vlayer.startEditing()
vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)




###
# Not working, 5 elements pers frame
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
tp.setMode(Qgis.VectorTemporalMode.FixedTemporalRange)
#tp.setMode(1) #single field with datetime
tp.setStartField("time")

crs = vlayer.crs()
crs.createFromId(22992)
vlayer.setCrs(crs)
vlayer.updateFields()
QgsProject.instance().addMapLayer(vlayer)

features_list = []


  
for i in range(100):
    dtrange = temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
    for j in range(5):
        feat = QgsFeature(vlayer.fields())   # Create feature
        feat.setAttributes([dtrange.begin(), dtrange.end()])  # Set its attributes
        geom = QgsGeometry.fromPointXY(QgsPointXY(j, j)) # Create geometry from valueAtTimestamp
        feat.setGeometry(geom) # Set its geometry
        features_list.append(feat)
        

vlayer.startEditing()
vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)