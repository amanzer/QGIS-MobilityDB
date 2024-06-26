"""
Multicore solution without framerate dips

"""

# TODO : Include the PYQGIS imports for the plugin

from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
from collections import deque
from pympler import asizeof
from enum import Enum
import numpy as np
from shapely.geometry import Point
import math
import multiprocessing
import logging
import psutil


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

TIME_DELTA_DEQUEUE_SIZE =  4 # Length of the dequeue to keep the keys to keep in the buffer
PERCENTAGE_OF_OBJECTS = 1 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA_SIZE = 60  # Number of frames associated to one Time delta
FPS = 25


# TODO : Use Qgis data provider to access database and tables
SRID = 4326
########## AIS Danish maritime dataset ##########
DATABASE_NAME = "mobilitydb"
TPOINT_TABLE_NAME = "PyMEOS_demo"
TPOINT_ID_COLUMN_NAME = "MMSI"
TPOINT_COLUMN_NAME = "trajectory"
GRANULARITY = Time_granularity.set_time_step(1).MINUTE

########## LIMA PERU drivers dataset ##########
# DATABASE_NAME = "lima_demo"
# TPOINT_TABLE_NAME = "driver_paths"
# TPOINT_ID_COLUMN_NAME = "driver_id"
# TPOINT_COLUMN_NAME = "trajectory"
# GRANULARITY = Time_granularity.set_time_step(1).SECOND


from chunk_fnc import process_chunk2

    

def log(msg):
    """
    Function to log messages in the QGIS log window.
    """
    try:
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)
    except Exception as e:
        print(f"Error logging message in logging: {msg}")

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

        """
        TODO : Possible alternative to Python dictionnary containing 3 matrices with deque that keeps
        track of the keys to keep in memory: 

        Create matrix of size n x m x 3, where n is the number of objects, m is the number of frames in a time delta.
        The 3rd dimension is used to store the 3 time deltas in memory, the middle 2d matrix will always be the current
        tdelta to show. At each new t_delta we slide the 3 matrices to the left, removing the oldest, and loading the 
        new one to the right of the last matrix.

        """
         
        self.time_deltas_matrices = {} 
        self.time_deltas_to_keep = deque(maxlen=TIME_DELTA_DEQUEUE_SIZE)
        self.time_deltas_to_keep.append(0)

       

        # variables to keep track of the current state of the animation
        self.current_time_delta_key = 0
        self.current_time_delta_end = TIME_DELTA_SIZE - 1
        self.previous_frame = 0
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False # variable used to handle situation where the user moves forward and backward on a time delta boundary
        # x_min,y_min, x_max, y_max = self.qviz.get_canvas_extent() # Extent of the canvas at start of animation
        self.extent = self.qviz.get_canvas_extent()
        self.objects_count = self.db.get_objects_count()
        self.objects_id_str = self.db.get_objects_str()
        self.total_ids = self.db.get_total_ids()

        # Create qgsi features for all objects
        task_features_gen = Qgis_features_generation_thread(f"Generating Qgis features for all objects","qViz", self.objects_count, self.qviz.vlayer.fields(), self.set_qgis_features, self.raise_error)
        self.task_manager.addTask(task_features_gen)

        # Initiate request for first batch
        time_delta_key = 0
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1

        task_matrix_gen = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.total_ids, self.create_matrix, self.set_matrix, self.raise_error)
        task_matrix_gen.taskCompleted.connect(self.initiate_animation) # Start the animation when the first batch is fetched
        self.task_manager.addTask(task_matrix_gen)     

        # self.task_manager.allTasksFinished.connect(self.resume_animation)
    
    # Methods to handle initial setup 

    def initiate_animation(self):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        # Request for second time delta

        time_delta_key = TIME_DELTA_SIZE
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.total_ids, self.create_matrix, self.set_matrix, self.raise_error)
        self.task_manager.addTask(task)   
        self.new_frame_features(0)
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

    def resume_animation(self):
        """
        PLays the animation in the current direction.
        """
        self.qviz.play(self.direction) #TODO
            

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
                                     self.objects_id_str, self.extent, self.timestamps, self.total_ids, self.create_matrix, self.set_matrix, self.raise_error)
            self.task_manager.addTask(task)        


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
            if self.direction == 1:
                # log(f"------- FETCH NEXT BATCH  - forward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                if self.current_time_delta_end + 1  != self.total_frames:
                    # self.qviz.pause()
                    self.current_time_delta_key = frame_number
                    self.current_time_delta_end = (self.current_time_delta_key + TIME_DELTA_SIZE) - 1
                    # log(f"------- FETCH NEXT BATCH  - forward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    self.update_cache(self.current_time_delta_key)
                    self.fetch_next_data(self.current_time_delta_key+TIME_DELTA_SIZE)
                    # if self.task_manager.countActiveTasks() != 0:
                    #     self.qviz.pause()
                    
                    self.update_vlayer_features()
                    self.changed_key = True
                    # if self.task_manager.countActiveTasks() != 0:
                    #     self.qviz.pause()
                    
            else:
                # log(f"------- FETCH NEXT BATCH  - backward - delta before : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
            
                self.update_vlayer_features()  
                if self.current_time_delta_key != 0:
                    # self.qviz.pause()
                    self.current_time_delta_key = self.current_time_delta_key - TIME_DELTA_SIZE
                    self.current_time_delta_end = frame_number-1
                    # log(f"------- FETCH NEXT BATCH  - backward - delta after : {self.current_time_delta_key} - delta end : {self.current_time_delta_end}")
                    
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
            frame_index = frame_number- time_delta_key
     

            current_time_stamp_column = self.time_deltas_matrices[time_delta_key][:, frame_index]

            datetime_obj = QDateTime(self.timestamps[frame_number])
            datetime_objs = {i: datetime_obj for i in range(self.objects_count)}
            attribute_changes = {fid: {0: datetime_objs[fid]} for fid in datetime_objs}


            new_geometries = {}  # Dictionary {feature_id: QgsGeometry}
            for i in range(self.objects_count): #TODO : compare vs Nditer
                new_geometries[i] = QgsGeometry.fromWkt(current_time_stamp_column[i])


            self.qviz.vlayer.startEditing()
            self.qviz.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
            self.qviz.vlayer.dataProvider().changeGeometryValues(new_geometries) # Updating geometries for all features
            self.qviz.vlayer.commitChanges()
            iface.vectorLayerTools().stopEditing(self.qviz.vlayer)

        except Exception as e:
            log(f"Error updating the features for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    # Methods to handle the QGIS threads

    def create_matrix(self, result_queue, begin_frame, end_frame, TIME_DELTA_SIZE, extent, timestamps, connection_params, table_name, id_column_name, tpoint_column_name, GRANULARITY, ids_str, total_ids):
        """
        This functions runs in a parallel process to fetch the data from the MobilityDB database for the given time delta.
        It creates the numpy matrix and fills it with the positions of the objects for the given time delta.

        """
        start_date = timestamps[0]
        logs = ""
        
        empty_point_wkt = Point().wkt  # "POINT EMPTY"
      

        # cpus = [[x, x+1] for x in range(2, 12, 2)]
        cpu_count = psutil.cpu_count()
        half_cpu_count = cpu_count // 2
        cpus = [i for i in range(half_cpu_count, cpu_count)]

        num_workers = len(cpus)-1
        # logs += f"Number of workers : {num_workers}\n"
        ids_per_process = int(np.ceil(len(total_ids)) / num_workers)
        
       

        # Create sub-lists of IDs for each process
        ids_sub_list = [total_ids[i:i+ids_per_process] for i in range(0, len(total_ids), ids_per_process)]


        pool = multiprocessing.Pool(num_workers)

        try:
            log(f"{cpus[0]}")
            worker_args = [(ids_sub_list[i], begin_frame, end_frame, TIME_DELTA_SIZE, start_date, empty_point_wkt, connection_params, GRANULARITY.value["steps"], GRANULARITY.value["name"], tpoint_column_name, table_name, id_column_name, extent, timestamps, cpus[i]) for i in range(len(ids_sub_list))]
            
            
            # pool.map(process_chunk2, worker_args)
            results = pool.map(process_chunk2, worker_args)
            
            dummy_matrix = np.full((len(total_ids), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)

            # check if one of results first argument is 1, if so, raise an error
            for i, (status, chunk_matrix, worker_logs) in enumerate(results):
                if status == 1:
                    raise ValueError(worker_logs)

                start_idx = i * ids_per_process
                end_idx = start_idx + len(chunk_matrix)
                dummy_matrix[start_idx:end_idx, :] = chunk_matrix
                logs += worker_logs


            result_queue.put(0)
            result_queue.put(dummy_matrix)
            result_queue.put(logs)
            return True

        except Exception as e:
            result_queue.put(1)
            result_queue.put(e)
            result_queue.put(logs)
            return False

        finally:
            pool.close()
            pool.join()

        


    def set_qgis_features(self, params):
        qgis_features_list = params['qgis_features_list']

        self.qviz.set_qgis_features(qgis_features_list)
        

    def set_matrix(self, params):
        """
        Assign the new matrix to its tdelta key.
        """
        self.time_deltas_matrices[params['key']] = params['matrix']
        
        uninterrupted_animation = TIME_DELTA_SIZE / params['time']
        new_fps = min(uninterrupted_animation, FPS)
        self.qviz.set_fps(new_fps)


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")






class Qgis_features_generation_thread(QgsTask):
    """
    This thread creates the qgs features for all the objects in the table.
    """

    def __init__(self, description,project_title, total_objects,vlayer_fields, finished_fnc, failed_fnc):
        super(Qgis_features_generation_thread, self).__init__(description, QgsTask.CanCancel)
        
        self.project_title = project_title
        self.total_objects = total_objects
        self.vlayer_fields = vlayer_fields

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
           
            datetime_obj = QDateTime(datetime.now())
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

            log(f"{self.total_objects} Qgis features created")

            self.result_params = {
                'qgis_features_list': qgis_features_list
            }
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True






class Matrix_generation_thread(QgsTask):
    """
    This thread creates next time delta's the matrix containing the positions for all objects to show. 
    """
    def __init__(self, description,project_title, beg_frame, end_frame, objects_id_str, extent, timestamps, total_ids, create_matrix_fnc, finished_fnc, failed_fnc):
        super(Matrix_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title

        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.objects_id_str = objects_id_str
        self.extent = extent
        self.timestamps = timestamps
        self.total_ids = total_ids
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

            result_queue = multiprocessing.Queue()
            pid = os.getpid()
            cpu_count = psutil.cpu_count()
            half_cpu_count = cpu_count // 2

            psutil.Process(pid).cpu_affinity([i for i in range(half_cpu_count)])
            log(f"QVIZ Process {pid} affinity : {psutil.Process(pid).cpu_affinity()}")
            
            # log(f"arguments : begin_frame : {self.begin_frame}, end_frame : {self.end_frame}, TIME_DELTA_SIZE : {TIME_DELTA_SIZE}, PERCENTAGE_OF_OBJECTS : {PERCENTAGE_OF_OBJECTS}, {self.extent}, len timestamps :{len(self.timestamps)}, granularity : {GRANULARITY.value},{len(self.objects_id_str)}")
            process = multiprocessing.Process(target=self.create_matrix, args=(result_queue, self.begin_frame, self.end_frame, TIME_DELTA_SIZE, self.extent, self.timestamps, connection_params, TPOINT_TABLE_NAME, TPOINT_ID_COLUMN_NAME, TPOINT_COLUMN_NAME, GRANULARITY, self.objects_id_str, self.total_ids))
            process.start()
            # log(f"Process started")

            return_value = result_queue.get()
            if return_value == 1:
                error = result_queue.get()
                log(f"Error inside new process: {error}")
                TIME_total = time.time() - now
                self.result_params = {
                    'matrix' : None,
                    'time' : TIME_total
                }
                return True
            else:
                # Retrieve the result from the queue
                result_matrix = result_queue.get()
                logs= result_queue.get()
                result_queue.close()
                process.join()  # Wait for the process to complete
                log(logs)
                # log(f"Retrieved matrix shape: {result_matrix.shape}, logs {logs}" )
                TIME_total = time.time() - now
                log(f"multiprocess terminated in {TIME_total} s" )
                self.result_params = {
                    'key': self.begin_frame,
                    'matrix' : result_matrix,
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


    def get_total_ids(self):
        return self.ids_list

    def get_objects_str(self):
        return self.objects_id_str


    def get_objects_count(self):
        return self.objects_count
        

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
        # self.canvas.extentsChanged.connect(self.pause)
    

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

    def set_qgis_features(self, qgis_features_list):
        self.vlayer.dataProvider().addFeatures(qgis_features_list)


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

