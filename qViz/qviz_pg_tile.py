"""

1. MobDB : Singleton used to connect to the MobilityDB database
2. Qviz : Controls the temporal controller and stores the points


"""

from pymeos import *
from datetime import datetime, timedelta
import time
import math

FRAMES_FOR_30_FPS = 48

class qviz:
    """
    Main class used to create the temporal view and visualize the trajectories of ships.
    """
    def __init__(self, map:bool):
        if map :
            self.createMapsLayer()
        
        self.canvas = iface.mapCanvas()
        self.temporalController = self.canvas.temporalController()

        frame_rate = 30
        self.temporalController.setFramesPerSecond(frame_rate)
        self.steps = 1440
        self.vlayer = None

        start_date = datetime(2023, 6, 1, 0, 0, 0)
        end_date = datetime(2023, 6, 1, 23, 59, 59)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(self.steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]
                
        self.features = {}

        self.temporalController.updateTemporalRange.connect(self.on_new_frame)


        
        ts_start = self.timestamps_strings[0]
        ts_end = self.timestamps_strings[FRAMES_FOR_30_FPS]

        # Create a data source URI
        ds_uri = QgsDataSourceUri()
        ds_uri.setParam("type", "xyz")

        # Specify the base URL without p_start and p_end parameters
        base_url = "http://localhost:7800/public.beta/{z}/{x}/{y}.pbf"

        # Append p_start and p_end parameters to the base URL

        url_with_params = f"{base_url}?p_start={ts_start}&p_end={ts_end}" 
        #url_with_params = f"{base_url}?p_start=2023-06-01%2007:00:00&p_end=2023-06-01%2007:10:00"
        ds_uri.setParam("url", url_with_params)
        ds_uri.setParam("zmin", "0")
        ds_uri.setParam("zmax", "14")

        # Create the vector tile layer
        self.vt_layer = QgsVectorTileLayer(bytes(ds_uri.encodedUri()).decode(), "OpenMapTiles (OSM)")

        # Add the layer to the map
        QgsProject.instance().addMapLayer(self.vt_layer)

        print("vt_layer added")



        self.on_new_frame_times = []
        self.removePoints_times = []
        self.update_features_times = []
        self.number_of_points_stored_in_layer = []

        
    def get_stats(self):
        len_on_new_frame = len(self.on_new_frame_times)
        len_removePoints = len(self.removePoints_times)
        len_update_features = len(self.update_features_times)

        on_new_frame_average = sum(self.on_new_frame_times) / len_on_new_frame
        removePoints_average = sum(self.removePoints_times) / len_removePoints
        update_features_average = sum(self.update_features_times) / len_update_features

        average_number_of_points_stored_in_layer = sum(self.number_of_points_stored_in_layer) / len(self.number_of_points_stored_in_layer)
        return f"on_new_frame (average over {len_on_new_frame}): {on_new_frame_average}s \n removePoints (average over {len_removePoints}): {removePoints_average} s\n update_features (average over {len_update_features}): {update_features_average} s \n average number of points stored in layer: {average_number_of_points_stored_in_layer} "

    def setFeatures(self, features):
        self.features = features


    def next_frames_points(self, timestamps):

        return {key: self.features[key] for key in timestamps if key in self.features}
    
    def on_new_frame(self):
        """
        Function called every time temporal controller frame is changed. It is used to update the features displayed on the map.
        """
        
        curr_frame = self.temporalController.currentFrameNumber()

        
        if curr_frame % FRAMES_FOR_30_FPS == 0:
            
            #we update by range of 48 frames, find the start and end of the period for curr_frame
            p_start = math.floor(curr_frame / FRAMES_FOR_30_FPS)
            p_end = p_start + FRAMES_FOR_30_FPS

            ts_start = self.timestamps_strings[p_start]
            ts_end = self.timestamps_strings[p_end]

            print("p_start: ", p_start, " p_end: ", p_end, " ts_start: ", ts_start, " ts_end: ", ts_end)


            now = time.time()    
            QgsProject.instance().removeMapLayer(self.vt_layer)

            # Create a data source URI
            ds_uri = QgsDataSourceUri()
            ds_uri.setParam("type", "xyz")

            # Specify the base URL without p_start and p_end parameters
            base_url = "http://localhost:7800/public.beta/{z}/{x}/{y}.pbf"

            # Append p_start and p_end parameters to the base URL

            url_with_params = f"{base_url}?p_start={ts_start}&p_end={ts_end}" 
            #url_with_params = f"{base_url}?p_start=2023-06-01%2007:00:00&p_end=2023-06-01%2007:10:00"
            ds_uri.setParam("url", url_with_params)
            ds_uri.setParam("zmin", "0")
            ds_uri.setParam("zmax", "14")

            # Create the vector tile layer
            self.vt_layer = QgsVectorTileLayer(bytes(ds_uri.encodedUri()).decode(), "OpenMapTiles (OSM)")

            # Add the layer to the map
            QgsProject.instance().addMapLayer(self.vt_layer)

            print("vt_layer added")

            self.on_new_frame_times.append(time.time()-now)


    def createMapsLayer(self):
        url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        map_layer = QgsRasterLayer(url, "OpenStreetMap", "wms")
        QgsProject.instance().addMapLayer(map_layer)
    
    
    def createPointsLayer(self):
        self.vlayer = QgsVectorLayer("Point", "points_3", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setStartField("time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)

    def updateTimestamps(self):
        self.timestamps = []
        #self.dtrange_ends = []
        currentFrameNumber = self.temporalController.currentFrameNumber()
        for i in range(self.steps):
            dtrange = self.temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
            self.timestamps.append(dtrange.begin().toPyDateTime().replace(tzinfo=dtrange.begin().toPyDateTime().tzinfo))
            #self.dtrange_ends.append(dtrange.end())

    def update_features(self, currentFrameNumber=0):
        #self.updateTimestamps()
        #self.features.update(self.timestamps)
        now= time.time()

        self.features_list =[]
      
        # iterate over the output_data which is a dictionnary
        features=  self.next_frames_points(self.timestamps_strings[currentFrameNumber:currentFrameNumber+FRAMES_FOR_30_FPS])
        for keys, items in features.items():
            datetime_obj = QDateTime.fromString(keys, "yyyy-MM-dd HH:mm:ss")
            
            for i in range(len(items)):
                if len(items[i]) > 0:
                    feat = QgsFeature(self.vlayer.fields())   # Create feature
                    feat.setAttributes([datetime_obj])  # Set its attributes
                    x,y = items[i]
                    geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                    feat.setGeometry(geom) # Set its geometry
                    self.features_list.append(feat)

        self.vlayer.startEditing()
        self.vlayer.addFeatures(self.features_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        self.update_features_times.append(time.time()-now)
        self.number_of_points_stored_in_layer.append(len(self.features_list))

    def removePoints(self):
        now= time.time()
        self.vlayer.startEditing()
        delete_ids = [f.id() for f in self.vlayer.getFeatures()]
        self.vlayer.deleteFeatures(delete_ids)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        self.removePoints_times.append(time.time()-now)



tt= qviz(False)