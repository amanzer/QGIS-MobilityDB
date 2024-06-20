"""
Scripts to delete and add features to a QGIS vector layer.

"""

# Delete
vlayer.startEditing()
delete_ids = [f.id() for f in vlayer.getFeatures()]
vlayer.deleteFeatures(delete_ids)
vlayer.commitChanges()

qgis_fields_list = []

# Add
vlayer.startEditing()
vlayer.addFeatures(qgis_fields_list) # Add list of features to vlayer
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)

# Update

vlayer.startEditing()
vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
vlayer.commitChanges()
iface.vectorLayerTools().stopEditing(vlayer)