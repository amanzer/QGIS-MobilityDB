"""

In this version of qViz :

- The temporal controller is created and the frame rate is set to 30 fps.
- The temporal controller is connected to the on_new_frame function.
- The on_new_frame function Deletes and creates a new vector tile layer every 48 frames (in order to achieve 30 fps with fixed timestamps of AIS dataset).

Regarding PG_tile_server :

- The code creating the function layer is in pg_tile_function_generator.ipynb


"""

from pymeos import *
from datetime import datetime, timedelta
import time
import math

FRAMES_FOR_30_FPS = 48

class qviz:
    """
    Creates and controls the temporal controller and the vector tile layer
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
        """
        Creates a vector tile layer which fetches data from the pg_tile server
        we provide the start and end timestamps of the data we want to fetch from the Tgeom table
        """
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


        with open("renderer.xml", "r") as file:
            xml_string = file.read()
        doc = QDomDocument()
        doc.setContent(xml_string)
        symbology_element = doc.firstChildElement("symbology")
        
        self.vt_layer.renderer().readXml(symbology_element, QgsReadWriteContext())
        # Add the layer to the map
        QgsProject.instance().addMapLayer(self.vt_layer)

        print("vt_layer added")
        self.create_vector_tiles_layer_times.append(time.time()-now)

        
    def get_stats(self):
        """
        Returns the average time for:
        - on_new_frame function
        - remove_Vector_tiles_layer function
        - create_Vector_tiles_layer function
        """
        len_on_new_frame = len(self.on_new_frame_times)
        len_delete_vt_layer = len(self.delete_vector_tiles_layer_times)
        len_create_vt_layer = len(self.create_vector_tiles_layer_times)

        on_new_frame_average = sum(self.on_new_frame_times) / len_on_new_frame
        removePoints_average = sum(self.delete_vector_tiles_layer_times) / len_delete_vt_layer
        update_features_average = sum(self.create_vector_tiles_layer_times) / len_create_vt_layer
        return f"on_new_frame (average over {len_on_new_frame}): {on_new_frame_average}s \n delete_vector_tiles_layer (average over {len_delete_vt_layer}): {removePoints_average} s\n create_vector_tiles_layer (average over {len_create_vt_layer}): {update_features_average} s"
        

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
        """
        Creates the map layer using OpenStreetMap
        """

        url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        map_layer = QgsRasterLayer(url, "OpenStreetMap", "wms")
        QgsProject.instance().addMapLayer(map_layer)
    

  

    def remove_Vector_tiles_layer(self):
        """
        Removes the vector tile layer from the QGIS instance
        """
        now= time.time()
        QgsProject.instance().removeMapLayer(self.vt_layer)
        self.delete_vector_tiles_layer_times.append(time.time()-now)



tt= qviz(False)