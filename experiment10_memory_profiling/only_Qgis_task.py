"""

New additions to the base multiprocessing :

- Removed datetime attribute by a start and end attributes that no longer needs to be updated in ONF
- Using vlayer dataprovider, the creation of QGIS features initially is almost intant
        the bottleneck with previous method: 
        self.vlayer.startEditing()
        self.vlayer.addFeatures(qgis_features_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        is that it generates a warning for each feature (qgis cannot create accessible child interface for object)

"""

# TODO : Include the PYQGIS imports for the plugin

from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
from pympler import asizeof
from enum import Enum
import numpy as np
from shapely.geometry import Point
import math
import multiprocessing
import logging



# Enum classes


class Time_granularity(Enum):
    # MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds, "name" : "MILLISECOND"}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds, "name" : "SECOND", "steps" : 1}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes, "name" : "MINUTE", "steps" : 1}
    # HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours, "name" : "HOUR"}

    @classmethod
    def set_time_step(cls, steps):
        Time_granularity.SECOND.value["timedelta"] = timedelta(seconds=steps)
        Time_granularity.SECOND.value["steps"] = steps
        Time_granularity.MINUTE.value["timedelta"] = timedelta(minutes=steps)
        Time_granularity.MINUTE.value["steps"] = steps
        return cls

# class Direction(Enum):
#     FORWARD = 1
#     BACKWARD = 0

# Global parameters

TIME_DELTA_DEQUEUE_SIZE =  3 # Length of the dequeue to keep the keys to keep in the buffer
PERCENTAGE_OF_OBJECTS = 1 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA_SIZE = 60  # Number of frames associated to one Time delta
FPS = 100


# TODO : Use Qgis data provider to access database and tables
SRID = 4326
########## AIS Danish maritime dataset ##########
DATA_SRID = 4326
DATABASE_NAME = "mobilitydb"
TPOINT_TABLE_NAME = "PyMEOS_demo"
TPOINT_ID_COLUMN_NAME = "MMSI"
TPOINT_COLUMN_NAME = "trajectory"
GRANULARITY = Time_granularity.set_time_step(1).MINUTE

########## AIS Danish maritime dataset ##########
# DATA_SRID =  
# DATABASE_NAME = "DanishAIS"
# TPOINT_TABLE_NAME = "Ships"
# TPOINT_ID_COLUMN_NAME = "MMSI"
# TPOINT_COLUMN_NAME = "trip"
# GRANULARITY = Time_granularity.set_time_step(1).MINUTE

########## LIMA PERU drivers dataset ##########
# DATA_SRID = 4326
# DATABASE_NAME = "lima_demo"
# TPOINT_TABLE_NAME = "driver_paths"
# TPOINT_ID_COLUMN_NAME = "driver_id"
# TPOINT_COLUMN_NAME = "trajectory"
# GRANULARITY = Time_granularity.set_time_step(5).SECOND



def log(msg):
    """
    Function to log messages in the QGIS log window.
    """
    QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)


class Time_deltas_handler:
    """
    Logic to handle the time deltas during the animation AND the data stored in memory.
    """

    def __init__(self, qviz):
        # Uncomment these lines for debugging the multiprocessing module
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        # logger.warning('logging enabled')
        # m = multiprocessing.Manager()
        
        self.task_manager = QgsApplication.taskManager()
        pymeos_initialize()
        
        self.qviz = qviz
        self.db = Database_connector()
        self.generate_timestamps()
        self.initiate_temporal_controller_values()
       

        # variables to keep track of the current state of the animation
        self.current_time_delta_key = 0
        self.current_time_delta_end = TIME_DELTA_SIZE - 1
        self.previous_frame = 0
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False # variable used to handle the scenario where a user moves forward and backward on a time delta boundary tick
        self.extent = self.qviz.get_canvas_extent()
        self.objects_count = self.db.get_objects_count()
        self.objects_id_str = self.db.get_objects_str()
        dimensions = (3, self.objects_count, TIME_DELTA_SIZE)
        empty_point_wkt = Point().wkt
        self.matrices = np.full(dimensions, empty_point_wkt, dtype=object)

        # Create qgsi features for all objects
        self.generate_qgis_features(self.objects_count, self.qviz.vlayer.fields(), self.timestamps[0], self.timestamps[-1])

        # Initiate request for first batch
        time_delta_key = 0
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        self.last_recorded_time = time.time()

        task_matrix_gen = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.create_matrix, self.initiate_animation, self.raise_error)
        # task_matrix_gen.taskCompleted.connect(self.initiate_animation) # Start the animation when the first batch is fetched
        self.task_manager.addTask(task_matrix_gen)     

        # self.task_manager.allTasksFinished.connect(self.resume_animation)
    
    # Methods to handle initial setup 

    def generate_qgis_features(self,num_objects, vlayer_fields,  start_date, end_date):
        features_list =[]
        start_datetime_obj = QDateTime(start_date)
        end_datetime_obj = QDateTime(end_date)


        for i in range(num_objects):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([start_datetime_obj, end_datetime_obj])
            features_list.append(feat)
        
        self.qviz.set_qgis_features(features_list)
        log(f"{num_objects} Qgis features created")
        


    def initiate_animation(self, params):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        self.matrices[1] = params['matrix']
        self.set_frame_rate(params['time'])


        # Request for second time delta
        
        second_time_delta_key = TIME_DELTA_SIZE
        self.fetch_next_data(second_time_delta_key)
        self.update_vlayer_features()

        # self.new_frame_features(0)
        # self.task_manager.allTasksFinished.connect(self.resume_animation)


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
        interval = QgsInterval(GRANULARITY.value["steps"], GRANULARITY.value["qgs_unit"])
        frame_rate = FPS
        
        self.qviz.set_temporal_controller_extent(time_range) 
        self.qviz.set_temporal_controller_frame_duration(interval)
        self.qviz.set_temporal_controller_frame_rate(frame_rate)


    
    # Methods to handle the animation and t_delta logic

    def shift_matrices(self):
        if self.direction == 1 : #shift Left
            log(f"Shift left, matrix 2 becomes matrix 1, matrix 1 becomes matrix 0")
            self.matrices[0] = self.matrices[1]
            self.matrices[1] = self.matrices[2]
        else : #shift Right
            log(f"Shift right, matrix 0 becomes matrix 1, matrix 1 becomes matrix 2")
            self.matrices[2] = self.matrices[1]
            self.matrices[1] = self.matrices[0]

    def resume_animation(self):
        """
        PLays the animation in the current direction.
        """
        self.qviz.play(self.direction) #TODO
            

    def fetch_next_data(self, time_delta_key):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        # if self.task_manager.countActiveTasks() != 0: # Only allow one request at a time
        #     return None
     
        # delta_key = self.timestamps_strings[time_delta_key]

        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        log(f"Fetching data for time delta {beg_frame} : {end_frame}")
        if end_frame  <= self.total_frames and beg_frame >= 0: #Either bound has to be valid 
            self.last_recorded_time = time.time()
            # self.qviz.pause()
            task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.create_matrix, self.set_matrix, self.raise_error)
            self.task_manager.addTask(task)        


        
    def new_frame_features(self, frame_number=0):
        """
        Handles the logic at each frame change.
      
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
            if self.direction == 1: # Animation is going forward
                # log(f"------- FETCH NEXT BATCH  - forward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                if self.current_time_delta_end + 1  != self.total_frames:
                    self.current_time_delta_key = frame_number
                    self.current_time_delta_end = (self.current_time_delta_key + TIME_DELTA_SIZE) - 1
                    # log(f"------- FETCH NEXT BATCH  - forward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    self.shift_matrices()

                    if self.task_manager.countActiveTasks() != 0:
                        self.qviz.pause()

                    self.fetch_next_data(self.current_time_delta_key+TIME_DELTA_SIZE)                    
                    self.update_vlayer_features()
                    self.changed_key = True

                    
            else: # Animation is going backward
                # log(f"------- FETCH NEXT BATCH  - backward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                self.update_vlayer_features()  
                if self.current_time_delta_key != 0: 
                    self.current_time_delta_key = self.current_time_delta_key - TIME_DELTA_SIZE
                    self.current_time_delta_end = frame_number-1
                    # log(f"------- FETCH NEXT BATCH  - backward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    
                    self.shift_matrices()
                    if self.task_manager.countActiveTasks() != 0:
                        self.qviz.pause()

                    self.fetch_next_data(self.current_time_delta_key-TIME_DELTA_SIZE)
                    self.changed_key = True
                  
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
            frame_index = frame_number- time_delta_key
     

            current_time_stamp_column = self.matrices[1][:, frame_index]
    

            new_geometries = {}  # Dictionary {feature_id: QgsGeometry}
            for i in range(self.objects_count): #TODO : compare vs Nditer
                new_geometries[i] = QgsGeometry().fromWkt(current_time_stamp_column[i])


            self.qviz.vlayer.startEditing()
            # self.qviz.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
            self.qviz.vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
            self.qviz.vlayer.commitChanges()
            iface.vectorLayerTools().stopEditing(self.qviz.vlayer)

        except Exception as e:
            log(f"Error updating the features for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    # Methods to handle the QGIS threads

    def create_matrix(self, result_queue, begin_frame, end_frame, TIME_DELTA_SIZE, extent, timestamps, connection_params, table_name, id_column_name, tpoint_column_name, GRANULARITY, ids_str):
        """
        This functions runs in a parallel process to fetch the data from the MobilityDB database for the given time delta.
        It creates the numpy matrix and fills it with the positions of the objects for the given time delta.

        """
        try: 
            p_start = timestamps[begin_frame]
            p_end = timestamps[end_frame]
            start_date = timestamps[0]
            x_min,y_min, x_max, y_max = extent
            logs = ""
            
            # Part 1 : Fetch Tpoints from MobilityDB database
            connection = MobilityDB.connect(**connection_params)    
            cursor = connection.cursor()
        
            if GRANULARITY.value["name"] == "SECOND": # TODO : handle granularity of different time steps(5 seconds etc)
                time_value = 1 * GRANULARITY.value["steps"]
            elif GRANULARITY.value["name"] == "MINUTE":
                time_value = 60 * GRANULARITY.value["steps"]

            query = f"""WITH trajectories as (
                    SELECT 
                        atStbox(
                            a.{tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {x_min}, {y_min}, -- xmin, ymin
                                    {x_max}, {y_max}, -- xmax, ymax
                                    {DATA_SRID} -- SRID
                                ),
                                tstzspan('[{p_start}, {p_end}]')
                            )
                        ) as trajectory
                    FROM public.{table_name} as a 
                    WHERE a.{id_column_name} in ({ids_str})),

                    resampled as (

                    SELECT tsample(traj.trajectory, INTERVAL '{GRANULARITY.value["steps"]} {GRANULARITY.value["name"]}', TIMESTAMP '{start_date}')  AS resampled_trajectory
                        FROM 
                            trajectories as traj)
                
                    SELECT
                            EXTRACT(EPOCH FROM (startTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS start_index ,
                            EXTRACT(EPOCH FROM (endTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS end_index,
                            rs.resampled_trajectory
                    FROM resampled as rs ;"""

            cursor.execute(query)
            # logs += f"query : {query}\n"
            rows = cursor.fetchall()
            cursor.close()
            connection.close()

            # Part 2 : Creating and filling the numpy matrix
            logs += f"Number of rows : {len(rows)}\n"
            now_matrix =time.time()
            empty_point_wkt = Point().wkt  # "POINT EMPTY"
            matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
            
            for i in range(len(rows)):
                if rows[i][2] is not None:
                    try:
                        traj_resampled = rows[i][2]

                        start_index = rows[i][0] - begin_frame
                        end_index = rows[i][1] - begin_frame
                        values = np.array([point.wkt for point in traj_resampled.values()])
                        matrix[i, start_index:end_index+1] = values
                
                    except:
                        continue
                    
            logs += f"Matrix generation time : {time.time() - now_matrix}\n"
            logs += f"Matrix shape : {matrix.shape}\n"
            logs += f"Number of non empty points : {np.count_nonzero(matrix != 'POINT EMPTY')}\n"

            result_queue.put(0)
            result_queue.put(matrix)
            result_queue.put(logs)
        except Exception as e:
            result_queue.put(1)
            result_queue.put(e)
            result_queue.put(logs)
            return False


    def set_qgis_features(self, params):
        qgis_features_list = params['qgis_features_list']

        self.qviz.set_qgis_features(qgis_features_list)
        

    def set_frame_rate(self, matrix_generation_time):
        uninterrupted_animation = TIME_DELTA_SIZE / matrix_generation_time
        new_fps = min(uninterrupted_animation, FPS)
        self.qviz.set_fps(new_fps)


    def set_matrix(self, params):
        """
        Assign the new matrix to its tdelta key.
        """
        if self.direction == 1:
            log("new assigned to Matrix 2   ")
            self.matrices[2] = params['matrix']
        else:
            log("new assigned to Matrix 0   ")
            self.matrices[0] = params['matrix']
        TIME_matrix = params['time'] # TODO : Probably remove in the final version
        TIME_Qgs_Thread = time.time() - self.last_recorded_time
        self.set_frame_rate(TIME_Qgs_Thread)
      
        


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")



class Matrix_generation_thread(QgsTask):
    """
    This thread creates next time delta's the matrix containing the positions for all objects to show. 
    """
    def __init__(self, description,project_title, beg_frame, end_frame, objects_id_str, extent, timestamps, create_matrix_fnc, finished_fnc, failed_fnc):
        super(Matrix_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title

        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.objects_id_str = objects_id_str
        self.extent = extent
        self.timestamps = timestamps
        self.create_matrix = create_matrix_fnc
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
        Runs the new process to create the matrix for the given time delta.
        """
        try:
            
            now = time.time()
            connection_params= {
                "host": "localhost",
                "port": 5432,
                "dbname": DATABASE_NAME,
                "user": "postgres",
                "password": "postgres"
            }

            p_start = self.timestamps[self.begin_frame]
            p_end = self.timestamps[self.end_frame]
            start_date = self.timestamps[0]
            x_min,y_min, x_max, y_max = self.extent
            
            # Part 1 : Fetch Tpoints from MobilityDB database
            connection = MobilityDB.connect(**connection_params)    
            cursor = connection.cursor()
        
            if GRANULARITY.value["name"] == "SECOND": 
                time_value = 1 * GRANULARITY.value["steps"]
            elif GRANULARITY.value["name"] == "MINUTE":
                time_value = 60 * GRANULARITY.value["steps"]

            tpoint_column_name = TPOINT_COLUMN_NAME
            id_column_name = TPOINT_ID_COLUMN_NAME
            table_name = TPOINT_TABLE_NAME

            query = f"""WITH trajectories as (
                    SELECT 
                        atStbox(
                            a.{tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {x_min}, {y_min}, -- xmin, ymin
                                    {x_max}, {y_max}, -- xmax, ymax
                                    {DATA_SRID} -- SRID
                                ),
                                tstzspan('[{p_start}, {p_end}]')
                            )
                        ) as trajectory
                    FROM public.{table_name} as a 
                    WHERE a.{id_column_name} in ({self.objects_id_str})),

                    resampled as (

                    SELECT tsample(traj.trajectory, INTERVAL '{GRANULARITY.value["steps"]} {GRANULARITY.value["name"]}', TIMESTAMP '{start_date}')  AS resampled_trajectory
                        FROM 
                            trajectories as traj)
                
                    SELECT
                            EXTRACT(EPOCH FROM (startTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS start_index ,
                            EXTRACT(EPOCH FROM (endTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS end_index,
                            rs.resampled_trajectory
                    FROM resampled as rs ;"""

            cursor.execute(query)
            # logs += f"query : {query}\n"
            rows = cursor.fetchall()
            cursor.close()
            connection.close()

          
            # now_matrix =time.time()
            empty_point_wkt = Point().wkt  # "POINT EMPTY"
            matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
            
            for i in range(len(rows)):
                if rows[i][2] is not None:
                    try:
                        traj_resampled = rows[i][2]

                        start_index = rows[i][0] - self.begin_frame
                        end_index = rows[i][1] - self.begin_frame
                        values = np.array([point.wkt for point in traj_resampled.values()])
                        matrix[i, start_index:end_index+1] = values
                
                    except Exception as e:
                        log(e)
                        continue
            
            TIME_total = time.time() - now
            self.result_params = {
                    'matrix' : matrix,
                    'time' : TIME_total
                }

        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True






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
            self.objects_count = len(self.ids_list)

            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            self.objects_id_str = ', '.join(map(str, ids_list))


        except Exception as e:
            log(e)


    def get_objects_str(self):
        return self.objects_id_str


    def get_objects_count(self):
        return self.objects_count
        

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            log(query)
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
        self.fps = FPS

        self.fps_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        # self.canvas.extentsChanged.connect(self.test(3))
 

    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        # tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        # tp.setStartField("time")
        tp.setStartField("start_time")
        tp.setEndField("end_time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)

    
    def memory_usage(self, obj):
        """
        Returns the memory usage of the object in paramter, in mega bytes.
        """
        size_in_bytes = asizeof.asizeof(obj)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        log(f"Total size: {size_in_megabytes:.6f} MB")

    
    # Getters

    def get_canvas_extent(self):
        return self.extent
    

    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """
        return sum(self.fps_record)/len(self.fps_record)


    # Setters 

    #TODO : Need to define getters for when Temporal Controller state is changed by the user
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

    def set_qgis_features(self, features_list):
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)

    def set_fps(self, fps):
        self.fps = fps

    # Methods to handle the temporal controller
    
    def play(self, direction):
        """
        Plays the temporal controller animation in the given direction.
        """
        if direction == 1:
            self.temporalController.playForward()
        else:
            self.temporalController.playBackward()


    def pause(self):
        """
        Pauses the temporal controller animation.
        """
        self.temporalController.pause()


    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller to be the closest multiple of 5,
        favoring the lower value in case of an exact halfway.
        """
        # Calculating the optimal FPS based on the new frame time
        optimal_fps = 1 / new_frame_time
        # Ensure FPS does not exceed 60
        fps = min(optimal_fps, self.fps)

        self.temporalController.setFramesPerSecond(fps)
        log(f"{fps} : FPS {optimal_fps}")
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
    



tt = QVIZ()

