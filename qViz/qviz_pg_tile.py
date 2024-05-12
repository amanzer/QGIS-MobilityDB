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

        
        # TODO : Control animation through qViz 
        frame_rate = 30
        self.temporalController.setFramesPerSecond(frame_rate)

        # TODO : Replace fixed code with a method to fetch min and max Time from TGeom table
        self.steps = 1440
        start_date = datetime(2023, 6, 1, 0, 0, 0)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(self.steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]
                    
        # List to store the time taken by each function
        self.on_new_frame_times = []
        self.delete_vector_tiles_layer_times = []
        self.create_vector_tiles_layer_times = []

        # Create first vector tile layer
        self.createVectorTileLayer(0, FRAMES_FOR_30_FPS)
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)


        


    def createVectorTileLayer(self, p_start, p_end, zmin=0, zmax=6):
        now = time.time()
        ts_start = self.timestamps_strings[p_start]
        ts_end = self.timestamps_strings[p_end]

        print("p_start: ", p_start, " p_end: ", p_end, " ts_start: ", ts_start, " ts_end: ", ts_end)

        ds_uri = QgsDataSourceUri()
        ds_uri.setParam("type", "xyz")
        
        

        pg_tile_server_url = f"http://localhost:7800/public.psi{p_start}/{{z}}/{{x}}/{{y}}.pbf"
        # Append p_start and p_end parameters to the base URL

        url_with_params = f"{pg_tile_server_url}?p_start={ts_start}&p_end={ts_end}" 
        #url_with_params = f"{base_url}?p_start=2023-06-01%2007:00:00&p_end=2023-06-01%2007:10:00"
        ds_uri.setParam("url", url_with_params)
        ds_uri.setParam("zmin", str(zmin))
        ds_uri.setParam("zmax", str(zmax))

        
        self.vt_layer = QgsVectorTileLayer(bytes(ds_uri.encodedUri()).decode(), "PG_tile_server")

        # Add the layer to the map
        QgsProject.instance().addMapLayer(self.vt_layer)

        print("vt_layer added")
        self.create_vector_tiles_layer_times.append(time.time()-now)

        
    def get_stats(self):
        len_on_new_frame = len(self.on_new_frame_times)
        len_delete_vt_layer = len(self.delete_vector_tiles_layer_times)
        len_create_vt_layer = len(self.create_vector_tiles_layer_times)

        on_new_frame_average = sum(self.on_new_frame_times) / len_on_new_frame
        removePoints_average = sum(self.delete_vector_tiles_layer_times) / len_delete_vt_layer
        update_features_average = sum(self.create_vector_tiles_layer_times) / len_create_vt_layer
        return f"on_new_frame (average over {len_on_new_frame}): {on_new_frame_average}s \n delete_vector_tiles_layer (average over {len_delete_vt_layer}): {removePoints_average} s\n create_vector_tiles_layer (average over {len_create_vt_layer}): {update_features_average} s"
        
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
            now = time.time()    

            self.remove_Vector_tiles_layer()
        
            # Calculate bin index
            bin_index = curr_frame // FRAMES_FOR_30_FPS
            p_start = bin_index * FRAMES_FOR_30_FPS
            p_end = min((bin_index + 1) * FRAMES_FOR_30_FPS - 1, self.steps - 1)

            print(f"Bin Index: {bin_index}")
            print(f"Start Frame: {p_start}")
            print(f"End Frame: {p_end}")
            self.createVectorTileLayer(p_start, p_end)

            self.on_new_frame_times.append(time.time()-now)


    def createMapsLayer(self):
        url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        map_layer = QgsRasterLayer(url, "OpenStreetMap", "wms")
        QgsProject.instance().addMapLayer(map_layer)
    

  

    def remove_Vector_tiles_layer(self):
        now= time.time()
        QgsProject.instance().removeMapLayer(self.vt_layer)
        self.delete_vector_tiles_layer_times.append(time.time()-now)



tt= qviz(False)