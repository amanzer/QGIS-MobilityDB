"""

Known issues :

-Temporal Controller roller over(from frame 0 to the last frame) is not handled
-User moving the temporal controller tick by himself not handled

"""

# TODO : Include the PYQGIS imports for the plugin
from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
from collections import deque
from pympler import asizeof
import gc
from enum import Enum
import numpy as np
from shapely.geometry import Point
import math
import subprocess
import shutil
import os
import sys

class Time_granularity(Enum):
    # MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds, "name" : "MILLISECOND"}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds, "name" : "SECOND"}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes, "name" : "MINUTE"}
    # HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours, "name" : "HOUR"}
  


FPS_DEQUEUE_SIZE = 5 # Length of the dequeue to calculate the average FPS
TIME_DELTA_DEQUEUE_SIZE =  4 # Length of the dequeue to keep the keys to keep in the buffer


PERCENTAGE_OF_OBJECTS = 0.2 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA_SIZE = 300  # Number of frames associated to one Time delta

SRID = 4326
FPS = 30

DIRECTORY_PATH = os.getcwd()
MATRIX_DIRECTORY_PATH = f'{DIRECTORY_PATH}/matrices'

# AIS Danish maritime dataset
DATABASE_NAME = "mobilitydb"
TPOINT_TABLE_NAME = "PyMEOS_demo"
TPOINT_ID_COLUMN_NAME = "MMSI"
TPOINT_COLUMN_NAME = "trajectory"
GRANULARITY = Time_granularity.MINUTE

# LIMA PERU drivers dataset

# DATABASE_NAME = "lima_demo"
# TPOINT_TABLE_NAME = "driver_paths"
# TPOINT_ID_COLUMN_NAME = "driver_id"
# TPOINT_COLUMN_NAME = "trajectory"
# GRANULARITY = Time_granularity.SECOND
class Time_deltas_handler:
    """
    Logic to handle the time deltas during the animation AND the data stored in memory.
    """
    def __init__(self, qviz):

        self.task_manager = QgsApplication.taskManager()
        self.db = Database_connector()
        pymeos_initialize()
        
        self.qviz = qviz

        self.generate_timestamps()
        self.initiate_temporal_controller_values()

        self.time_deltas_matrices = {} #TODO Sliding Numpy matrix => Hstack ?

        self.time_deltas_to_keep = deque(maxlen=TIME_DELTA_DEQUEUE_SIZE)
        self.time_deltas_to_keep.append(0)



        if os.path.exists(MATRIX_DIRECTORY_PATH):
            shutil.rmtree(MATRIX_DIRECTORY_PATH)
            os.makedirs(MATRIX_DIRECTORY_PATH)
        else:
            os.makedirs(MATRIX_DIRECTORY_PATH)

        # variables to keep track of the current state of the animation
        self.current_time_delta_key = 0
        self.current_time_delta_end = TIME_DELTA_SIZE - 1
        self.previous_frame = 0
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False
        # Initiate request for first batch
        time_delta_key = 0
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1



        task_features_gen = Qgis_features_generation_thread(f"Generating Qgis features for all objects","qViz", len(self.db.ids_list), self.qviz.vlayer.fields(), self.timestamps_strings[0], self.set_qgis_features, self.raise_error)
        self.task_manager.addTask(task_features_gen)

        task_matrix_gen = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps_strings, self.set_matrix, self.raise_error)

        # Start the animation when the first batch is fetched
        task_matrix_gen.taskCompleted.connect(self.initiate_animation)
        self.task_manager.addTask(task_matrix_gen)     

        # self.task_manager.allTasksFinished.connect(self.update_vlayer_features)
    
    def initiate_animation(self):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        # Request for second time delta

        
        # Load the matrix from an HDF5 file
        filename = f"{MATRIX_DIRECTORY_PATH}/matrix_{0}.npy"
        loaded_matrix = np.load(filename, allow_pickle=True)

        self.time_deltas_matrices[0] = loaded_matrix

        time_delta_key = TIME_DELTA_SIZE
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps_strings, self.set_matrix, self.raise_error)

        self.task_manager.addTask(task)   
        self.new_frame_features(0)
        # self.task_manager.allTasksFinished.connect(self.play)

    def generate_timestamps(self):
        """
        Generate the timestamps associated to the dataset and the granularity selected.
        """
        start_date = self.db.get_min_timestamp()
        end_date = self.db.get_max_timestamp()
        self.total_frames = math.ceil( (end_date - start_date) // GRANULARITY.value["timedelta"] ) + 1
        remainder_frames = (self.total_frames) % TIME_DELTA_SIZE
        self.total_frames +=  remainder_frames

        self.timestamps = [start_date + i * GRANULARITY.value["timedelta"] for i in range(self.total_frames)]
        self.timestamps = [dt.replace(tzinfo=None) for dt in self.timestamps]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]
  

    def initiate_temporal_controller_values(self):
        """
        Update the temporal controller values for the dataset
        """
        
        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        interval = QgsInterval(1, GRANULARITY.value["qgs_unit"])
        frame_rate = FPS
        
        self.qviz.set_temporal_controller_extent(time_range) 
        self.qviz.set_temporal_controller_frame_duration(interval)
        self.qviz.set_temporal_controller_frame_rate(frame_rate)


    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



    def update_cache(self, time_delta_key):
        """
        Only have a maximum of TIME_DELTA_DEQUEUE_SIZE time deltas in memory at all times.
        """
        pass
        
        if time_delta_key not in self.time_deltas_to_keep:
            self.time_deltas_to_keep.append(time_delta_key)
            
            # Remove all data associated to keys no longer in time_deltas_to_keep
            for key in list(self.time_deltas_matrices.keys()):
                if key not in self.time_deltas_to_keep:
                    del self.time_deltas_matrices[key]
                    filename = f"{MATRIX_DIRECTORY_PATH}/matrix_{key}.npy"
                    os.remove(filename)
                    self.log(f"File {filename} deleted")
                    # gc.collect() #TODO measure time impact

    
 
    def fetch_next_data(self, time_delta_key):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        if self.task_manager.countActiveTasks() != 0: # Only allow one request at a time
            return None
        if  time_delta_key in  self.time_deltas_matrices.keys():
            return None 
        # delta_key = self.timestamps_strings[time_delta_key]

        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        
        if end_frame  <= self.total_frames and beg_frame >= 0: #Either bound has to be valid 
            # self.qviz.pause()
            task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps_strings, self.set_matrix, self.raise_error)
            self.task_manager.addTask(task)        



    def set_qgis_features(self, params):
        qgis_features_list = params['qgis_features_list']

        self.qviz.set_qgis_features(qgis_features_list)
        


    def set_matrix(self, params):
        """
        Function called once the thread if completed.
        """
        filename = f"{MATRIX_DIRECTORY_PATH}/matrix_{params['key']}.npy" 

        self.time_deltas_matrices[params['key']] = np.load(filename, allow_pickle=True) # allow_pickle=True is required because the matrix contains WKT strings in the form of objects
     
    

    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")


    

    def new_frame_features(self, frame_number=0):
        """
        Handles the logic at each frame change.
        
        to keep track of : 
        - direction, 
        - last frame, 
        - t delta key 
        """

        if self.previous_frame - frame_number <= 0:
            self.direction = 1 # Forward
            if frame_number >= self.total_frames: # Reached the end of the animation, pause
                self.qviz.pause()
        else:
            self.direction = 0
            if frame_number <= 0: # Reached the beginning of the animation, pause
                self.qviz.pause()
            
        self.previous_frame = frame_number

        if frame_number % TIME_DELTA_SIZE == 0:
            if self.direction == 1:
                self.log(f"                                          FETCH NEXT BATCH  - forward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                if self.current_time_delta_end + 1  != self.total_frames:
                    # self.qviz.pause()
                    self.current_time_delta_key = frame_number
                    self.current_time_delta_end = (self.current_time_delta_key + TIME_DELTA_SIZE) - 1
                    self.log(f"                                          FETCH NEXT BATCH  - forward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    self.update_cache(self.current_time_delta_key)
                    self.fetch_next_data(self.current_time_delta_key+TIME_DELTA_SIZE)
                    # if self.task_manager.countActiveTasks() != 0:
                    #     self.qviz.pause()

                    self.update_vlayer_features()
                    self.changed_key = True
            else:
                self.log(f"                                          FETCH NEXT BATCH  - backward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
            
                self.update_vlayer_features()  
                if self.current_time_delta_key != 0:
                    # self.qviz.pause()
                    self.current_time_delta_key = self.current_time_delta_key - TIME_DELTA_SIZE
                    self.current_time_delta_end = frame_number-1
                    self.log(f"                                          FETCH NEXT BATCH  - backward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    
                    self.update_cache(self.current_time_delta_key)
                    self.fetch_next_data(self.current_time_delta_key-TIME_DELTA_SIZE)
                    self.changed_key = True
                    # if self.task_manager.countActiveTasks() != 0:
                    #     self.qviz.pause()
        else:
            if self.changed_key:
                if frame_number < self.current_time_delta_key:
                    self.current_time_delta_key = self.current_time_delta_key - TIME_DELTA_SIZE
                    self.current_time_delta_end = frame_number
                    self.changed_key = False
            self.update_vlayer_features()
            self.changed_key = False
        
   

    def update_vlayer_features(self):
        """
        Updates the features of the vector layer for the given frame number.
        """
        try:
            time_delta_key = self.current_time_delta_key
            frame_number = self.previous_frame
            frame_timestamp_str =  self.timestamps_strings[frame_number]
            datetime_obj = QDateTime.fromString(frame_timestamp_str, "yyyy-MM-dd HH:mm:ss")
            # datetime_obj = QDateTime(self.timestamps[frame_number])
            frame_index = frame_number- time_delta_key

            current_time_stamp_column = self.time_deltas_matrices[time_delta_key][:, frame_index]

            datetime_objs = {i: datetime_obj for i in range(current_time_stamp_column.shape[0])}
            attribute_changes = {fid: {0: datetime_objs[fid]} for fid in datetime_objs}


            new_geometries = {}  # Dictionary {feature_id: QgsGeometry}
            for i in range(current_time_stamp_column.shape[0]): #TODO : compare vs Nditer
                new_geometries[i] = QgsGeometry.fromWkt(current_time_stamp_column[i])


            self.qviz.vlayer.startEditing()
            self.qviz.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
            self.qviz.vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
            self.qviz.vlayer.commitChanges()
            iface.vectorLayerTools().stopEditing(self.qviz.vlayer)

        except Exception as e:
            self.log(f"Error updating the features for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class Qgis_features_generation_thread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description,project_title, total_objects,vlayer_fields, datetime_str, finished_fnc, failed_fnc):
        super(Qgis_features_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title
        
        self.total_objects = total_objects
        self.vlayer_fields = vlayer_fields
        self.datetime_str = datetime_str
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc

        self.result_params = None
        self.error_msg = None

    def finished(self, result):
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self):
        """
        Function that is executed in parallel to fetch all the subsets of Tpoints from the MobilityDB database,
        for the given time delta.
        """
        try:
            datetime_obj = QDateTime.fromString(self.datetime_str, "yyyy-MM-dd HH:mm:ss")
            vlayer_fields = self.vlayer_fields

            empty_point_wkt = Point().wkt  # "POINT EMPTY"
    
            qgis_features_list = []

            for i in range(self.total_objects):
                feat = QgsFeature(vlayer_fields)
                feat.setAttributes([datetime_obj])  # Set its attributes

                # Create geometry from WKT string
                geom = QgsGeometry.fromWkt(empty_point_wkt)
                feat.setGeometry(geom)  # Set its geometry
                qgis_features_list.append(feat)

            self.log(f"{self.total_objects} Qgis features created")

            self.result_params = {
                'qgis_features_list': qgis_features_list
            }
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class Matrix_generation_thread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description,project_title, beg_frame, end_frame, db, extent, timestamps, finished_fnc, failed_fnc):
        super(Matrix_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title
        
        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.db = db
        self.extent = extent
        self.timestamps = timestamps
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc

        self.result_params = None
        self.error_msg = None

    def finished(self, result):
        if result:
            self.finished_fnc(self.result_params)
        else:
            self.failed_fnc(self.error_msg)

    def run(self):
        """
        Function that is executed in parallel to fetch all the subsets of Tpoints from the MobilityDB database,
        for the given time delta.
        """
        try:
            now = time.time()
            x_min,y_min, x_max, y_max = self.extent
            
            arguments = [self.begin_frame, self.end_frame, PERCENTAGE_OF_OBJECTS, x_min, y_min, x_max, y_max]
            arguments = [str(arg) for arg in arguments]
            arguments += [self.timestamps[0], str(len(self.timestamps)), GRANULARITY.value["name"], MATRIX_DIRECTORY_PATH, DATABASE_NAME, TPOINT_TABLE_NAME, TPOINT_ID_COLUMN_NAME, TPOINT_COLUMN_NAME]
            
            # PATHS
            process_B_path = f"{DIRECTORY_PATH}/QGIS-MobilityDB/experiment8_subprocess/matrix_file.py"
            python_path = sys.executable

            command = [python_path, process_B_path, *arguments]
            result = subprocess.run(command, capture_output=True, text=True)
            # self.log(result.stdout.strip())
            TIME_total = time.time() - now
            self.log(f"file created in {TIME_total} s, frames for 30 FPS animation at this rate : { TIME_total * 30}" )
            self.result_params = {
                'key': self.begin_frame
            }
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)





class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self):
        try: 
            connection_params = {
            "host": "localhost",
            "port": 5432,
            "dbname": DATABASE_NAME,
            "user": "postgres",
            "password": "postgres"
            }
            self.table_name = TPOINT_TABLE_NAME
            self.id_column_name = TPOINT_ID_COLUMN_NAME
            self.tpoint_column_name = TPOINT_COLUMN_NAME                  
            self.connection = MobilityDB.connect(**connection_params)

            self.cursor = self.connection.cursor()

            self.cursor.execute(f"SELECT {self.id_column_name} FROM public.{self.table_name};")
            self.ids_list = self.cursor.fetchall()
            self.ids_list = self.ids_list[:int(len(self.ids_list)*PERCENTAGE_OF_OBJECTS)]
        except Exception as e:
            pass

  
    def get_subset_of_tpoints(self, pstart, pend, xmin, ymin, xmax, ymax):
        """
        For each object in the ids_list :
            Fetch the subset of the associated Tpoints between the start and end timestamps
            contained in the STBOX defined by the xmin, ymin, xmax, ymax.
        """
        try:
           
            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            ids_str = ', '.join(map(str, ids_list))
          
            query = f"""
                    SELECT 
                        atStbox(
                            a.{self.tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {xmin}, {ymin}, -- xmin, ymin
                                    {xmax}, {ymax}, -- xmax, ymax
                                    4326 -- SRID
                                ),
                                tstzspan('[{pstart}, {pend}]')
                            )
                        )
                    FROM public.{self.table_name} as a 
                    WHERE a.{self.id_column_name} in ({ids_str});
                    """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            self.log(e)


    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            
            self.cursor.execute(f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            pass

    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.cursor.execute(f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            pass


    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class QVIZ:
    """

    This class plays the role of the controller in the MVC pattern.
    It is used to manage the user interaction with the View, which is the QGIS UI.
    
    It handles the interactions with both the Temporal Controller and the Vector Layer.
    """
    def __init__(self):    
        self.create_vlayer()
        self.canvas = iface.mapCanvas()
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem(f"EPSG:{SRID}"))
        self.temporalController = self.canvas.temporalController()
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.handler = Time_deltas_handler(self)

    
        # self.dq_FPS = deque(maxlen=LEN_DEQUEUE_FPS)
        # for i in range(LEN_DEQUEUE_FPS):
        #     self.dq_FPS.append(0.033)

        self.fps_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        self.canvas.extentsChanged.connect(self.pause)
    
    def set_qgis_features(self, qgis_features_list):
        self.vlayer.startEditing()
        self.vlayer.addFeatures(qgis_features_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)

    def set_temporal_controller_extent(self, time_range):
        if self.temporalController:
            self.temporalController.setTemporalExtents(time_range)
    
    def set_temporal_controller_frame_duration(self, interval):
        if self.temporalController:
            self.temporalController.setFrameDuration(interval)
    
    def set_temporal_controller_frame_rate(self, frame_rate):
        if self.temporalController:
            self.temporalController.setFramesPerSecond(frame_rate)

    def set_temporal_controller_frame_number(self, frame_number):
        if self.temporalController:
            self.temporalController.setCurrentFrameNumber(frame_number)

    def get_canvas_extent(self):
        return self.extent
    

    def pause(self):
        """
        Pauses the temporal controller animation.
        """
        self.temporalController.pause()


 
    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """
        return sum(self.fps_record)/len(self.fps_record)
    

    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller to be the closest multiple of 5,
        favoring the lower value in case of an exact halfway.
        """
        # Calculating the optimal FPS based on the new frame time
        optimal_fps = 1 / new_frame_time
        # Ensure FPS does not exceed 60
        fps = min(optimal_fps, FPS)

        self.temporalController.setFramesPerSecond(fps)
        self.log(f"{fps} : FPS {optimal_fps}")
        self.fps_record.append(optimal_fps)

    
    def on_new_frame(self):
        """
       
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()

        curr_frame = self.temporalController.currentFrameNumber()

        self.handler.new_frame_features(curr_frame)
             
        self.update_frame_rate(time.time()-now)
    
  
    
    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setStartField("time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)


    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)
    
    def memory_usage(self, obj):
        """
        Returns the memory usage of the object in paramter, in mega bytes.
        """
        size_in_bytes = asizeof.asizeof(obj)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        self.log(f"Total size: {size_in_megabytes:.6f} MB")





tt = QVIZ()