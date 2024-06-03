"""

TIME DELTAS WITH FIXED STBOX :

-UPDATE QGS FEATURES EACH FRAME vs DEL/ADD
-USE NUMPY MATRIX TO STORE TIME DELTA DATA
-No prints

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




class Time_granularity(Enum):
    MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes}
    HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours}
  


FPS_DEQUEUE_SIZE = 5 # Length of the dequeue to calculate the average FPS
TIME_DELTA_DEQUEUE_SIZE = 3 # Length of the dequeue to keep the keys to keep in the buffer


PERCENTAGE_OF_OBJECTS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA_SIZE = 240 # Number of frames associated to one Time delta
GRANULARITY = Time_granularity.MINUTE
SRID = 4326


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
        self.time_deltas_to_keep.append(self.timestamps_strings[0])


        # variables to keep track of the current state of the animation
        self.current_time_delta_key = 0
        self.current_time_delta_end = TIME_DELTA_SIZE - 1
        self.previous_frame = 0
        self.direction = 1 # 1 : forward, 0 : backward

        # Initiate request for first batch
        time_delta_key = 0
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1

        task = QgisThread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps, self.on_thread_completed, self.raise_error)

        # Start the animation when the first batch is fetched
        task.taskCompleted.connect(self.initiate_animation)
        self.task_manager.addTask(task)     
        # self.task_manager.allTasksFinished.connect(self.update_vlayer_features)
    
    def initiate_animation(self):
        """
        Once the first batch is fetched, make the request for the second and play the animation for this first time delta
        """
        # Request for second time delta
        time_delta_key = TIME_DELTA_SIZE
        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1

        task = QgisThread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps, self.on_thread_completed, self.raise_error)

        self.task_manager.addTask(task)   
        self.new_frame_features(0)

    def generate_timestamps(self):
        """
        Generate the timestamps associated to the dataset and the granularity selected.
        """
        start_date = self.db.get_min_timestamp()
        end_date = self.db.get_max_timestamp()
        self.total_frames = math.ceil( (end_date - start_date) // GRANULARITY.value["timedelta"] )

        self.timestamps = [start_date + i * GRANULARITY.value["timedelta"] for i in range(self.total_frames)]
        self.timestamps = [dt.replace(tzinfo=None) for dt in self.timestamps]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]
  

    def initiate_temporal_controller_values(self):
        """
        Update the temporal controller values for the dataset
        """
        
        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        interval = QgsInterval(1, GRANULARITY.value["qgs_unit"])
        frame_rate = 30
        
        self.qviz.set_temporal_controller_extent(time_range) 
        self.qviz.set_temporal_controller_frame_duration(interval)
        self.qviz.set_temporal_controller_frame_rate(frame_rate)


    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



    def update_cache(self, time_delta_key, direction):
        """
        Only have a maximum of TIME_DELTA_DEQUEUE_SIZE time deltas in memory at all times.
        """
        pass
        # key = self.timestamps_strings[time_delta_key]

        # if key not in self.time_deltas_to_keep:
        #     if direction == "forward":
        #         self.time_deltas_to_keep.append(self.timestamps_strings[time_delta_key])
        #     elif direction == "back":
        #         self.time_deltas_to_keep.appendleft(self.timestamps_strings[time_delta_key])
            
        #     # Remove all data associated to keys no longer in time_deltas_to_keep
        #     for key in list(self.time_deltas_matrices.keys()):
        #         if key not in self.time_deltas_to_keep:
        #             del self.time_deltas_matrices[key]
                    # gc.collect() #TODO measure time impact

    
 
    def fetch_next_data(self, time_delta_key):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        if self.task_manager.countActiveTasks() != 0: # Only allow one request at a time
            return None
        
        # delta_key = self.timestamps_strings[time_delta_key]

        beg_frame = time_delta_key
        end_frame = (time_delta_key + TIME_DELTA_SIZE) -1

        if end_frame  <= (len(self.timestamps)) and beg_frame >= 0: #Either bound has to be valid 
            # self.qviz.pause()
            task = QgisThread(f"Data for time delta {time_delta_key} : {self.timestamps_strings[time_delta_key]}","qViz", beg_frame, end_frame,
                                     self.db, self.qviz.get_canvas_extent(), self.timestamps, self.on_thread_completed, self.raise_error)

            self.task_manager.addTask(task)        


    def on_thread_completed(self, params):
        """
        Store the time delta data fetched by the thread.
        """
        self.time_deltas_matrices[params['key']] = params['matrix']
     
    

    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")


    def generate_qgs_features(self):
        """
        This method creates the QGIS features for each coordinate associated to the given
        time delta and frame number.
        """
        datetime_obj = QDateTime.fromString(self.timestamps_strings[0], "yyyy-MM-dd HH:mm:ss")
        vlayer_fields = self.qviz.vlayer.fields()

        empty_point_wkt = Point().wkt  # "POINT EMPTY"
        # create a numpy array of size len(ids_list) with empty_point_wkt
        starting_points = np.full((1, len(self.db.ids_list)), empty_point_wkt, dtype=object)
    
        qgis_fields_list = []
        
        for wkt in np.nditer(starting_points, flags=['refs_ok']):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([datetime_obj])  # Set its attributes

            # Create geometry from WKT string
            geom = QgsGeometry.fromWkt(wkt.item())
            feat.setGeometry(geom)  # Set its geometry
            qgis_fields_list.append(feat)
        
        # TODO : measure time impact of passing the list back to the Controller class to update the vlayer 
        self.qviz.vlayer.startEditing()
        self.qviz.vlayer.addFeatures(qgis_fields_list) # Add list of features to vlayer
        self.qviz.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.qviz.vlayer)
        

    def new_frame_features(self, frame_number=0):
        """
        Handles the logic at each frame change.
        
        to keep track of : 
        - direction, 
        - last frame, 
        - t delta key 
        """

        if self.previous_frame - frame_number < 0:
            self.direction = 1
            if frame_number >= self.total_frames: # Reached the end of the animation, pause
                self.qviz.pause()
        else:
            self.direction = 0
            if frame_number <= 0: # Reached the beginning of the animation, pause
                self.qviz.pause()
            
        self.previous_frame = frame_number

        if frame_number == self.current_time_delta_end and self.direction == 1:
            self.log("FETCH NEXT BATCH forward")
            if self.current_time_delta_end + 1  != self.total_frames:
                # self.log("HHHHHHHH")
                self.current_time_delta_key = frame_number+1
                self.current_time_delta_end = (self.current_time_delta_key + TIME_DELTA_SIZE) - 1

                self.update_cache(self.current_time_delta_key, "forward")
                self.fetch_next_data(self.current_time_delta_key+TIME_DELTA_SIZE)

        elif frame_number == self.current_time_delta_key and self.direction == 0:
            self.log("FETCH NEXT BATCH Backward")
            if self.current_time_delta_key != 0:
                
                self.current_time_delta_key = self.current_time_delta_key - TIME_DELTA_SIZE
                self.current_time_delta_end = frame_number-1

                self.update_cache(self.current_time_delta_key, "back")
                self.fetch_next_data(self.current_time_delta_key-TIME_DELTA_SIZE)

        self.update_vlayer_features()
        
   

    def update_vlayer_features(self):
        """
        Updates the features of the vector layer for the given frame number.
        """
        try:
            time_delta_key = self.current_time_delta_key
            frame_number = self.previous_frame
            key =  self.timestamps_strings[frame_number]
            datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")

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



class QgisThread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description,project_title, beg_frame, end_frame, db, canvas_extent, timestamps, finished_fnc, failed_fnc):
        super(QgisThread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title
        
        self.begin_frame = beg_frame
        self.end_frame = end_frame
        self.db = db
        self.canvas_extent = canvas_extent
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
            x_min = self.canvas_extent.xMinimum()
            y_min = self.canvas_extent.yMinimum()
            x_max = self.canvas_extent.xMaximum()
            y_max = self.canvas_extent.yMaximum()
            p_start = self.timestamps[self.begin_frame]
            p_end = self.timestamps[self.end_frame]
            rows = self.db.get_subset_of_tpoints(p_start, p_end, x_min, y_min, x_max, y_max)    
      
            
            empty_point_wkt = Point().wkt  # "POINT EMPTY"
            matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
   
            time_ranges = self.timestamps
            now = time.time()
            for i in range(len(rows)):
                try:
                    traj = rows[i][0]
                    traj = traj.temporal_precision(GRANULARITY.value["timedelta"]) 
                    num_instants = traj.num_instants()
                    if num_instants == 0:
                        continue
                    elif num_instants == 1:
                        single_timestamp = traj.timestamps()[0].replace(tzinfo=None)
                        index = time_ranges.index(single_timestamp)
                        matrix[i][index] = traj.values()[0].wkt
                        count += 1
                    elif num_instants >= 2:
                        traj_resampled = traj.temporal_sample(start=time_ranges[0],duration= GRANULARITY.value["timedelta"])
                     
                        start_index = time_ranges.index( traj_resampled.start_timestamp().replace(tzinfo=None) ) - self.begin_frame
                        end_index = time_ranges.index( traj_resampled.end_timestamp().replace(tzinfo=None) ) - self.begin_frame
                   
                        trajectory_array = np.array([point.wkt for point in traj_resampled.values()])
                        matrix[i, start_index:end_index+1] = trajectory_array
                        count += 1
                except:
                    continue
            self.log(f"time to fill matrix :: { time.time() - now}")
            del rows
            # gc.collect()
    
            self.result_params = {
                'key': self.begin_frame,
                'matrix' : matrix
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
            "dbname": "mobilitydb",
            "user": "postgres",
            "password": "postgres"
            }
            self.table_name = "PyMEOS_demo"
            self.id_column_name = "MMSI"
            self.tpoint_column_name = "trajectory"                    
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
           
            mmsi_list2 = [mmsi[0] for mmsi in self.ids_list]
            ids_str = ', '.join(map(str, mmsi_list2))
          
            query = f"""
                    SELECT 
                        atStbox(
                            a.{self.tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {xmin}, {ymin}, -- xmin, ymin
                                    {xmax}, {ymax}, -- xmax, ymax
                                    {SRID} -- SRID
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
        

        self.handler = Time_deltas_handler(self)


        self.handler.generate_qgs_features()

        # self.data =  Data_in_memory(self.xmin, self.ymin, self.xmax, self.ymax)
        # self.data.task_manager.taskAdded.connect(self.pause)
        # self.data.generate_qgs_features(self.vlayer)
        # self.current_time_delta = 0
        self.last_frame = 0
        # self.total_frame = self.data.total_frames
        # self.dq_FPS = deque(maxlen=LEN_DEQUEUE_FPS)
        # for i in range(LEN_DEQUEUE_FPS):
        #     self.dq_FPS.append(0.033)

        self.fps_record = []
        # self.feature_number_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        self.canvas.extentsChanged.connect(self.pause)
        # self.data.update_temporal_controller_extent(self.temporalController)
    

    def set_temporal_controller_extent(self, time_range):
        if self.temporalController:
            self.temporalController.setTemporalExtents(time_range)
    
    def set_temporal_controller_frame_duration(self, interval):
        if self.temporalController:
            self.temporalController.setFrameDuration(interval)
    
    def set_temporal_controller_frame_rate(self, frame_rate):
        if self.temporalController:
            self.temporalController.setFramesPerSecond(frame_rate)

    def get_canvas_extent(self):
        return self.canvas.extent()
    

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
        fps = min(optimal_fps, 30)

        self.temporalController.setFramesPerSecond(fps)
        self.log(f"FPS : {fps} - Calculated FPS : {optimal_fps}")
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
 





tt = QVIZ()