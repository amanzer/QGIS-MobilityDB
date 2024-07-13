"""
Solution B :

Divide Time range into subsets : time deltas
Buffering system to keep only the 3 time deltas in memory

Qgis Task to fetch the next time delta in background while the animation runs 

"""


from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
from pympler import asizeof
from enum import Enum
import numpy as np
from shapely.geometry import Point
import math
import pickle
import os
import psutil
# Enum classes
from matplotlib import pyplot as plt

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

PERCENTAGE_OF_OBJECTS = 0.5 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA_SIZE = 15  # Number of frames associated to one Time delta
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

    def __init__(self, qviz, start_tdelta_frame=(0,0)):

        # Uncomment these lines for debugging the multiprocessing module
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.INFO)
        # logger.warning('logging enabled')
        # m = multiprocessing.Manager()
        
        self.task_manager = QgsApplication.taskManager()
        pymeos_initialize()
        
        self.qviz = qviz
        self.extent = self.qviz.get_initial_canvas_extent()
        self.db = Database_connector(self.extent) # TODO remove extent in intial mmsi fetch
        self.generate_timestamps()


        self.initiate_temporal_controller_values()

        start_tdelta_key = start_tdelta_frame[0]
        start_frame = start_tdelta_frame[1]
        

        # variables to keep track of the current state of the animation
        self.current_time_delta_key = start_tdelta_key
        self.current_time_delta_end = start_tdelta_key + TIME_DELTA_SIZE - 1
        self.previous_frame = start_frame
        self.direction = 1 # 1 : forward, 0 : backward
        self.changed_key = False # variable used to handle the scenario where a user moves forward and backward on a time delta boundary tick
        
        self.objects_count = self.db.get_objects_count()
        self.objects_id_str = self.db.get_objects_str()
        # dimensions = (3, self.objects_count, TIME_DELTA_SIZE)
        # empty_point_wkt = Point().wkt
        # self.matrices = np.full(dimensions, empty_point_wkt, dtype=object)
        self.previous_matrix = None
        self.current_matrix = None
        self.next_matrix = None

        # Create qgsi features for all objects
        self.generate_qgis_features(self.db.get_objects_ids(), self.qviz.vlayer.fields(), self.timestamps[0], self.timestamps[-1])

        # Initiate request for first batch
        time_delta_key = start_tdelta_key
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        self.last_recorded_time = time.time()
        self.qgis_task_records = []

        task_matrix_gen = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.db, self.initiate_animation, self.raise_error)
        # task_matrix_gen.taskCompleted.connect(self.initiate_animation) # Start the animation when the first batch is fetched
        self.task_manager.addTask(task_matrix_gen)     

        # self.task_manager.allTasksFinished.connect(self.resume_animation)
    

    def get_current_time_delta_key(self):
        return self.current_time_delta_key
    
    def get_last_frame(self):
        return self.previous_frame

    # Methods to handle initial setup 

    def generate_qgis_features(self,object_ids, vlayer_fields,  start_date, end_date):
        features_list =[]
        start_datetime_obj = QDateTime(start_date)
        end_datetime_obj = QDateTime(end_date)
        num_objects = len(object_ids)
        # empty_geom = QgsGeometry.fromWkt("POINT EMPTY")
        
        self.geometries={}
        for i in range(1, num_objects+1):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([ object_ids[i-1][0],start_datetime_obj, end_datetime_obj])
            geom = QgsGeometry()
            self.geometries[i] = geom
            feat.setGeometry(geom)
            features_list.append(feat)
        
        self.qviz.vlayer.dataProvider().addFeatures(features_list)
        log(f"{num_objects} Qgis features created")

        


    def initiate_animation(self, params):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        self.current_matrix = params['matrix']

        matrix_time = time.time() - self.last_recorded_time
        log(f"Matrix generation time : {matrix_time}")
        self.qgis_task_records.append(matrix_time)
        # self.set_frame_rate(matrix_time)


        # Request for second time delta
        
        second_time_delta_key = TIME_DELTA_SIZE
        self.fetch_next_data(second_time_delta_key)
        self.update_vlayer_features()

        # self.new_frame_features(0)
        # self.resume_animation()
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
        self.previous_matrix = self.current_matrix
        self.current_matrix = self.next_matrix
        self.next_matrix = None
        # if self.direction == 1 : #shift Left
        #     log(f"Shift left, matrix 2 becomes matrix 1, matrix 1 becomes matrix 0")
        #     self.matrices[0] = self.matrices[1]
        #     self.matrices[1] = self.matrices[2]

        # else : #shift Right
        #     log(f"Shift right, matrix 0 becomes matrix 1, matrix 1 becomes matrix 2")
        #     self.matrices[2] = self.matrices[1]
        #     self.matrices[1] = self.matrices[0]

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
        pid = os.getpid()
        log(f"Qgis process pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1
        log(f"Fetching data for time delta {beg_frame} : {end_frame}")
        if end_frame  <= self.total_frames and beg_frame >= 0: #Either bound has to be valid 
            self.last_recorded_time = time.time()
            # self.qviz.pause()
            current_extent = self.qviz.get_current_canvas_extent()

            if current_extent != self.extent:
                self.extent = current_extent
                self.qviz.pause()
                iface.messageBar().pushMessage("Info", "Animation has paused to adapt to new canvas", level=Qgis.Info)

                
            task = Matrix_generation_thread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.objects_id_str, self.extent, self.timestamps, self.db, self.set_matrix, self.raise_error)
            self.task_manager.addTask(task)        


        
    def new_frame_features(self, frame_number=0):
        """
        Handles the logic at each frame change.
      
        """

        # diff = self.previous_frame - frame_number
        forward = self.previous_frame + 1
        backward = self.previous_frame - 1
        if frame_number == forward:
            # print("YOOHOO FORWARD")
            self.direction = 1 # Forward
            
            if frame_number == 460: # Reached the end of the animation, pause
                log("dsfjsdf")
                self.qviz.pause()
        elif frame_number == backward:
            # print("YOOHOO BACKWARD")
            self.direction = 0
            if frame_number <= 0: # Reached the beginning of the animation, pause
                self.qviz.pause()
        else:
            print("No new frame")
            print(f"frame_number : {frame_number}, previous_frame : {self.previous_frame}") 
            return False

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
            # frame_index = frame_number- time_delta_key
     

            # current_time_stamp_column = self.current_matrix[:, frame_index]
    
            # new_geometries = {}  
            # new_geometries = {QgsGeometry().fromWkt(point) for point in current_time_stamp_column}  # Dictionary {feature_id: QgsGeometry}
            for i in range(1, self.objects_count+1):
                try:
                    position = self.current_matrix[i-1][0].value_at_timestamp(self.timestamps[frame_number])
                    
                    # Updating the geometry of the feature in the vector layer
                    self.geometries[i].fromWkb(position.wkb) # = QgsGeometry.fromWkt(position.wkt)
                    # hits+=1
                except:
                    continue 
                # new_geometries[i] = QgsGeometry().fromWkt(current_time_stamp_column[i-1])
                # self.geometries[i].fromWkb(current_time_stamp_column[i-1].wkb)

            self.qviz.vlayer.startEditing()
            # self.qviz.vlayer.dataProvider().changeAttributeValues(attribute_changes) # Updating attribute values for all features
            self.qviz.vlayer.dataProvider().changeGeometryValues(self.geometries) # Updating geometries for all features
            self.qviz.vlayer.commitChanges()
            iface.vectorLayerTools().stopEditing(self.qviz.vlayer)

        except Exception as e:
            log(f"Error updating the features {e} for time_delta : {self.current_time_delta_key} and frame number : {self.previous_frame}")


    # Methods to handle the QGIS threads


    def delete(self):
        self.db.close()
        self.task_manager = None
        self.previous_matrix = None
        self.current_matrix = None
        self.next_matrix = None

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
       
        log("next matrix ready ")
        self.next_matrix = params['matrix']
      

        TIME_Qgs_Thread = time.time() - self.last_recorded_time
        log(f"Matrix generation time : {TIME_Qgs_Thread}")
        self.qgis_task_records.append(TIME_Qgs_Thread)
        # self.set_frame_rate(TIME_Qgs_Thread)
      
        


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
    def __init__(self, description,project_title, beg_frame, end_frame, objects_id_str, extent, timestamps, db, finished_fnc, failed_fnc):
        super(Matrix_generation_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title

        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.objects_id_str = objects_id_str
        self.extent = extent
        self.timestamps = timestamps
        self.db = db
       
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc
        pid = os.getpid()
        log(f"QgisThread init pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")
            
    
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
            pid = os.getpid()
            log(f"QgisThread run pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")

            rows = self.db.get_tgeompoints(self.timestamps[self.begin_frame], self.timestamps[self.end_frame], self.extent)
            # log(f"Number of rows : {len(rows)}\n")
      
            # empty_point_wkt = Point().wkt  # "POINT EMPTY"
            # matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
            # for i in range(matrix.shape[1]):
            #     for j in range(matrix.shape[0]):
            #         try:
            #             if rows[j][0] is not None:
            #                 position = rows[j][0].value_at_timestamp(self.timestamps[self.begin_frame + i])
            #                 matrix[j, i] = position.wkt
            #         except Exception as e:
            #             # log(f"{rows[j][0]}")                        
            #             # log(f"Error at row {j} : {e}\n for frame { self.begin_frame + i} ")
            #             continue
                



            self.result_params = {
                'matrix' : rows
            }
        except Exception as e:
            log(f"Error in run method : {e}")
            self.error_msg = str(e)
            return False
        return True







class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self, extent):
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
            x_min,y_min, x_max, y_max = extent
            self.cursor = self.connection.cursor()

            self.cursor.execute(f"""
                                WITH trajectories as (
                                    SELECT 
                                        atStbox(
                                            a.{self.tpoint_column_name}::tgeompoint,
                                            stbox(
                                                ST_MakeEnvelope(
                                                    {x_min}, {y_min}, -- xmin, ymin
                                                    {x_max}, {y_max}, -- xmax, ymax
                                                    {DATA_SRID} -- SRID
                                                )
                                            )
                                        ) as trajectory, a.{self.id_column_name} as id
                                    FROM public.{self.table_name} as a )

                                    SELECT tr.id                            
                                    FROM trajectories as tr where tr.trajectory is not null ;
                                """)
                                
            self.ids_list = self.cursor.fetchall()
            self.ids_list = self.ids_list[:int(len(self.ids_list)*PERCENTAGE_OF_OBJECTS)]
            self.objects_count = len(self.ids_list)

            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            self.objects_id_str = ', '.join(map(str, ids_list))


        except Exception as e:
            log(e)

    def get_objects_ids(self):
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
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            # log(query)
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


    def get_tgeompoints(self, p_start, p_end, extent):

        x_min,y_min, x_max, y_max = extent
        
        
        # Part 1 : Fetch Tpoints from MobilityDB database
      

        query = f"""WITH trajectories as (
                SELECT 
                    atStbox(
                        a.{TPOINT_COLUMN_NAME}::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                                {x_min}, {y_min}, -- xmin, ymin
                                {x_max}, {y_max}, -- xmax, ymax
                                {DATA_SRID} -- SRID
                            ),
                            tstzspan('[{p_start}, {p_end}]')
                        )
                    ) as trajectory
                FROM public.{TPOINT_TABLE_NAME} as a 
                WHERE a.{TPOINT_ID_COLUMN_NAME} in ({self.objects_id_str}))
            
                SELECT
                        rs.trajectory
                FROM trajectories as rs ;"""

        self.cursor.execute(query)
        # logs += f"query : {query}\n"
        return self.cursor.fetchall()


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
        self.temporalController.setCurrentFrameNumber(0)
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.handler = Time_deltas_handler(self, (0,0))
        self.fps = FPS

        self.fps_record = []
        self.onf_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        # self.canvas.extentsChanged.connect(self.test)
        # self.last_extent_change_time = time.time()

    def test(self):
        # if difference between last extent change and current time is greater than 1 millisecond
        now= time.time()
        if now - self.last_extent_change_time > 1: 
            self.last_extent_change_time = now
            log(f"Extent changed at ts : {time.time()}")
 

    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("id", QVariant.Int) ,QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
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



    def memory_usage(self): 
        """ 
        Returns the memory usage of the object in paramter, in mega bytes. 
        """ 

        size_in_bytes = asizeof.asizeof(self.handler.current_matrix) 
        size_in_megabytesa = size_in_bytes / (1024 * 1024) 
        print(f"Total size current: {size_in_megabytesa:.6f} MB") 


        size_in_bytes = asizeof.asizeof(self.handler.previous_matrix) 
        size_in_megabytesb = size_in_bytes / (1024 * 1024) 
        print(f"Total size previous: {size_in_megabytesb:.6f} MB") 


        size_in_bytes = asizeof.asizeof(self.handler.next_matrix) 
        size_in_megabytesc = size_in_bytes / (1024 * 1024) 
        print(f"Total size next: {size_in_megabytesc:.6f} MB") 


        print(f"Total size : {size_in_megabytesa + size_in_megabytesb + size_in_megabytesc:.6f} MB") 



    # Getters

    def get_current_canvas_extent(self):
        return self.canvas.extent().toRectF().getCoords()

    def get_initial_canvas_extent(self):
        return self.extent
    

    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """

        print(f"Average FPS : {sum(self.fps_record)/len(self.fps_record)} over {len(self.fps_record)} frames") 

        # print(f"Time to fetch : {self.TIME_fetch_tgeompoints}") 

        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionB/desktop_results/Solution_B_{TIME_DELTA_SIZE}_{PERCENTAGE_OF_OBJECTS}_fps_record.pickle", "wb") as file: 

            pickle.dump(self.fps_record, file) 

        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionB/desktop_results/Solution_B_{TIME_DELTA_SIZE}_{PERCENTAGE_OF_OBJECTS}_onf_record.pickle", "wb") as file: 

            pickle.dump(self.onf_record, file) 

        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionB/desktop_results/Solution_B_{TIME_DELTA_SIZE}_{PERCENTAGE_OF_OBJECTS}_qgis_record.pickle", "wb") as file: 

            pickle.dump(self.handler.qgis_task_records, file) 


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
        # self.onf_record.append(optimal_fps)
        # Ensure FPS does not exceed 60
        fps = min(optimal_fps, self.fps)

        self.temporalController.setFramesPerSecond(fps)
        log(f"{fps} : FPS {optimal_fps}")
        self.fps_record.append(fps)

    
    def on_new_frame(self):
        """
       
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        
        now = time.time()
        curr_frame = self.temporalController.currentFrameNumber()
        self.handler.new_frame_features(curr_frame)
        self.update_frame_rate(time.time()-now)
    
    def del_vlayer(self):
        self.temporalController.updateTemporalRange.disconnect(self.on_new_frame)
        self.handler.db.close()
        if self.vlayer:
            
            self.vlayer = None
    
    def reset_animation(self):
        start_tdelta_key = self.handler.get_current_time_delta_key()
        start_frame = self.handler.get_last_frame()

        self.handler.delete()
        QgsProject.instance().removeMapLayer(self.vlayer.id())
        self.create_vlayer()
        self.set_temporal_controller_frame_number(start_frame)
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.handler = Time_deltas_handler(self, (start_tdelta_key ,start_frame))
        self.fps_record = []




tt = QVIZ()

