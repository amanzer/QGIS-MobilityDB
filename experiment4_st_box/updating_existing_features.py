canvas = iface.mapCanvas()
temporalController = canvas.temporalController()
frame_rate = 30
temporalController.setFramesPerSecond(frame_rate)
currentFrameNumber = temporalController.currentFrameNumber()

## Create a temporal layer (variable 'vlayer') with single field for datetime
vlayer = QgsVectorLayer("Point", "testing", "memory")
pr = vlayer.dataProvider()
pr.addAttributes([QgsField("time", QVariant.DateTime)])
vlayer.updateFields()
tp = vlayer.temporalProperties()
tp.setIsActive(True)

tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)

#tp.setMode(1) #single field with datetime
tp.setStartField("time")
crs = vlayer.crs()
vlayer.updateFields()



QgsProject.instance().addMapLayer(vlayer)



features_list =[]


datetime_obj = QDateTime.fromString("2023-06-01 00:00:00", "yyyy-MM-dd HH:mm:ss")
feat = QgsFeature(vlayer.fields())   # Create feature
feat.setAttributes([datetime_obj])  # Set its attributes
x,y = (0,0)
geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
feat.setGeometry(geom) # Set its geometry
feat.setId(0)
features_list.append(feat)




vlayer.startEditing()
vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)


for ft in vlayer.getFeatures():
    print(ft.id(), ft.attributes())


vlayer.startEditing()


geometry = QgsGeometry.fromWkt("POINT(45 45)")
fid = 0
vlayer.changeGeometry(fid, geometry)


#vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)





vlayer.startEditing()


fieldIndex =1
value = QDateTime.fromString("2023-06-01 01:00:00", "yyyy-MM-dd HH:mm:ss")
vlayer.changeAttributeValue(1, fieldIndex, value)



#vlayer.addFeatures(features_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)