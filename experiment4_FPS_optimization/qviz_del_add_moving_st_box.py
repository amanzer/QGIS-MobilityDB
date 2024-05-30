"""

- ADD/DEL QGS FEATURES EVERY FRAME

- UPDATE STBOX EVERY TIME DELTA

"""

# TODO : Include the PYQGIS imports for the plugin
from pymeos.db.psycopg import MobilityDB
import psycopg2
from pymeos import *
from datetime import datetime, timedelta
import time
from collections import deque
from pympler import asizeof
import gc
from enum import Enum
import numpy as np




class Time_granularity(Enum):
    MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes}
    HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours}
  

class Animation_direction(Enum):
    FORWARD = "1"
    BACKWARD = "0"

LEN_DEQUEUE_FPS = 5 # Length of the dequeue to calculate the average FPS
LEN_DEQUEUE_BUFFER = 2 # Length of the dequeue to keep the keys to keep in the buffer


PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
FRAMES_PER_TIME_DELTA = 60 # Number of frames associated to one Time delta
GRANULARITY = Time_granularity.MINUTE

class Data_in_memory:
    """
    This class handles the data stored in memory and the background threads that fetch the data from the MobilityDB database.
    
    It is the link between the QGIS UI (temporal Controller/Vector Layer controlled by the qviz class) and the MobilityDB database.
    
    """
    def __init__(self, st_box_extent):

        self.task_manager = QgsApplication.taskManager()
        self.db = MobilityDB_Database()
        pymeos_initialize()
        
        self.st_box_extent = st_box_extent

        self.ids_list = self.db.get_subset_of_ids(PERCENTAGE_OF_SHIPS) 
        
        self.generate_timestamps()

        self.coordinates_cache = {}
        self.time_delta_keys_in_memory = deque(maxlen=LEN_DEQUEUE_BUFFER)
        self.time_delta_keys_in_memory.append(self.timestamps_strings[0])
        task = QgisThread(f"Batch requested for time delta {0} - {self.timestamps_strings[0]}", 0, self.timestamps_strings[0],
                                     "qViz",self.db,self.ids_list, 0, FRAMES_PER_TIME_DELTA, self.st_box_extent , self.timestamps, self.on_thread_completed, self.raise_error)
        
        self.task_manager.addTask(task)     
    
    
    def generate_timestamps(self):
        """
        Generates the timestamps associated to each frame of the animation, based on the time range of the data.
        """
        start_date = self.db.get_min_timestamp()
        end_date = self.db.get_max_timestamp()
        self.total_frames = (end_date - start_date) // GRANULARITY.value["timedelta"]

        self.timestamps = [start_date + i * GRANULARITY.value["timedelta"] for i in range(self.total_frames)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]


    def update_temporal_controller_extent(self, temporalController):
        """
        Updates the extent of the temporal controller to match the time range of the data.
        """
        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        temporalController.setTemporalExtents(time_range)


    def update_cache_in_memory(self, time_delta_key, direction):
        """
        GOAL :
        We want the coordinates_cache dictionnary to only keep the values associated to the keys in the time_delta_keys_in_memory deque.
        With this we ensure only the data necessary to the animation is in memory.

        1. Updates the deque with the time_delta_key.
        2. Flushes the buffer to remove the keys/values not in the deque.

        """
        key = self.timestamps_strings[time_delta_key]

        if key in self.time_delta_keys_in_memory:
            return
        elif direction == "forward":
            self.time_delta_keys_in_memory.append(self.timestamps_strings[time_delta_key])
        elif direction == "back":
            self.time_delta_keys_in_memory.appendleft(self.timestamps_strings[time_delta_key])
        print(self.time_delta_keys_in_memory)
        self.flush_buffer()


    def flush_buffer(self):
        """
        Removes all keys/values associated to past time deltas from the buffer.

        Forecefully calls the garbage collector to free the memory.
        """
        #remove from buffer all the keys that are not in the keys_to_keep
        for key in list(self.coordinates_cache.keys()):
            if key not in self.time_delta_keys_in_memory:
                print("Deleting key : ", key)
                del self.coordinates_cache[key]
                gc.collect() 
        size_in_bytes = asizeof.asizeof(self.coordinates_cache)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        print(f"Total size of dictionary (including referenced objects): {size_in_megabytes:.6f} MB")
    

    def generate_qgis_points(self,current_time_delta, frame_number, vlayer_fields):
        """
        This method creates the QGIS features for each coordinate associated to the given
        time delta and frame number.
        """
        try : 
            time_delta_key = self.timestamps_strings[current_time_delta]
            
            key =  self.timestamps_strings[frame_number]
    
            qgis_fields_list = []
            
            datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
            now_value_at_ts_qgs_feature = time.time()

            current_batch = self.coordinates_cache[time_delta_key]
            current_frame_coords = current_batch[frame_number]
            # class 'shapely.geometry.point.Point
            #current_frame_coords is a disctionary that contains 

            for coords in current_frame_coords:
                feat = QgsFeature(vlayer_fields)
                feat.setAttributes([datetime_obj])  # Set its attributes
                x,y = coords
                geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                feat.setGeometry(geom) # Set its geometry
                qgis_fields_list.append(feat)

            
            
            print(f"QgsFeature generation time : {time.time() - now_value_at_ts_qgs_feature}")
            print(f"Timestamp {key}", end=" ")
            return qgis_fields_list
        except Exception as e:
            print(e)
            return []


    def fetch_data_with_thread(self, start_frame, end_frame, st_box_extent):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        delta_key = self.timestamps_strings[start_frame]

        if end_frame  <= (len(self.timestamps)) and start_frame >= 0:
            print(f"Fetching batch for {start_frame} to {end_frame} aka {self.timestamps_strings[start_frame]} to {self.timestamps_strings[end_frame]}")

            task = QgisThread(f"Batch requested for time delta {start_frame} - {self.timestamps_strings[start_frame]}", start_frame,delta_key,
                                     "qViz",self.db,self.ids_list, start_frame, end_frame, st_box_extent, self.timestamps, self.on_thread_completed, self.raise_error)

            self.task_manager.addTask(task)        


    def on_thread_completed(self, params):
        """
        Function called when a thread finishes its job to fetch the data from the MobilityDB database.
        """
        # check delta_key exists in buffer        
        self.coordinates_cache[params['delta_key']] = params['batch']
        # display stats from task 
        for stat in  params['stats']:
            print(stat)
        


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")


    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class QgisThread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, current_frame, delta_key, project_title,db,ids_list, pstart, pend, st_box_extent, timestamps, finished_fnc,
                 failed_fnc):
        super(QgisThread, self).__init__(description, QgsTask.CanCancel)
        self.current_frame = current_frame
        self.delta_key = delta_key
        self.project_title = project_title
        self.db = db
        self.ids_list = ids_list
        self.pstart = pstart
        self.pend = pend
        self.timestamps = timestamps
        self.st_box_extent = st_box_extent
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
            stats = []
            now = time.time()

            features = self.db.get_subset_of_tpoints(self.ids_list, self.timestamps[self.pstart], self.timestamps[self.pend], self.st_box_extent)
            stats.append(f"Time to fetch subTpoints from MobilityDB : {time.time()-now} s")

            now2 = time.time()
            batch_coords = {}           
            for key in range(self.pstart,self.pend +1):
                batch_coords[key] = []
                for tpoint_id in self.ids_list:
                    try:
                        coords = features[tpoint_id].value_at_timestamp(self.timestamps[key])
                        batch_coords[key].append((coords.x, coords.y))
                    except Exception as e:
                        continue
            
            del features
            gc.collect()
            stats.append(f"Time to get coordinates with Value_at_timestamp : {time.time()-now2} s")
            stats.append(f"Total time for task : {time.time()-now} s")
            self.result_params = {
                'delta_key': self.delta_key,
                'batch' : batch_coords,
                'stats': stats
            }
        except psycopg2.Error as e:
            self.error_msg = str(e)
            return False
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True



class MobilityDB_Database:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self):
        connection_params = {
        "host": "localhost",
        "port": 5432,
        "dbname": "mobilitydb",
        "user": "postgres",
        "password": "postgres"
        }
        try: 
            self.table_name = "PyMEOS_demo"
            self.id_column_name = "MMSI"
            self.tpoint_column_name = "trajectory"            
            self.SRID = 4326            
            self.connection = MobilityDB.connect(**connection_params)

            self.cursor = self.connection.cursor()

            self.cursor.execute(f"SELECT {self.id_column_name} FROM public.{self.table_name};")
            self.ids_list = self.cursor.fetchall()
        except Exception as e:
            print(e)

    def get_subset_of_ids(self, percentage=0.001):
        """
        Returns a subset of the objects ids in the table, based on the given percentage.
        """
        return self.ids_list[:int(len(self.ids_list)*percentage)]

    def get_subset_of_tpoints(self, ids_list, pstart, pend, st_box_extent):
        """
        For each object in the ids_list :
            Fetch the subset of the associated Tpoints between the start and end timestamps
            contained in the STBOX defined by the st_box_extent.
             
        """
        try:
            rows={}
            for id in ids_list:
                tpoint_id = id[0]
                query = f"""
                        SELECT 
                            atStbox(
                        a.{self.tpoint_column_name}::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                            {st_box_extent[0]}, {st_box_extent[1]}, -- xmin, ymin
                            {st_box_extent[2]}, {st_box_extent[3]}, -- xmax, ymax
                            {self.SRID} -- SRID
                            ),
                            tstzspan('[{pstart}, {pend}]')
                        )
                        )
                            FROM public.{self.table_name} as a 
                        WHERE a.{self.id_column_name} = '{tpoint_id}' ;
                        """
                self.cursor.execute(query)
                subset_tpoint = self.cursor.fetchone()
                if subset_tpoint[0]:
                    rows[id] = subset_tpoint[0]

            return rows
        except Exception as e:
            print(e)

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            
            self.cursor.execute(f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(e)

    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.cursor.execute(f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(e)


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
        # INITIATE UI ELEMENTS  
        self.create_vlayer()
        self.canvas = iface.mapCanvas()
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
        self.temporalController = self.canvas.temporalController()
        frame_rate = 30
        self.direction = Animation_direction.FORWARD
        self.temporalController.setFramesPerSecond(frame_rate)
        interval = QgsInterval(1, GRANULARITY.value["qgs_unit"])
        self.temporalController.setFrameDuration(interval)

        st_box_extent  = self.get_current_st_box_extent
        print(f"extent : {st_box_extent}")

        # CREATE DATA HANDLER
        self.data =  Data_in_memory(st_box_extent)
        self.data.task_manager.taskAdded.connect(self.pause)
        self.end_frame = len(self.data.timestamps)- FRAMES_PER_TIME_DELTA
        #self.data.task_manager.allTasksFinished.connect(self.play)


        # START ANIMATION
        self.current_time_delta = 0
        self.last_frame = 0
        self.update_vlayer_content
        
        # TODO : We ultimately want a stable FPS, meaning the fluctuations should be the raised/lowered to the closest stable value ie 20, 25, 30 etc 
        self.dq_FPS = deque(maxlen=LEN_DEQUEUE_FPS)
        for i in range(LEN_DEQUEUE_FPS):
            self.dq_FPS.append(0.033)

        self.fps_record = []
        self.feature_number_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        self.data.update_temporal_controller_extent(self.temporalController)
        
    
    def play(self):
        """
        TODO : self.direction has to be replaced by the animation state of the Temporal Controller
        Plays the temporal controller animation.
        """
        self.update_vlayer_content()
        if self.direction == Animation_direction.FORWARD:
            self.temporalController.playForward()
        else:
            self.temporalController.playBackward()
            

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
    
    def get_average_feature_number(self):
        """
        Returns the average number of features displayed on the map.
        """
        return sum(self.feature_number_record)/len(self.feature_number_record)

    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller.
        """
        # self.dq_FPS.append(new_frame_time)
        # avg_frame_time = (sum(self.dq_FPS)/LEN_DEQUEUE_FPS)
        # print(f"Average time for On_new_frame : {avg_frame_time}")
        optimal_fps = 1 / new_frame_time
        print(f"Optimal FPS : {optimal_fps} (FPS = 1/frame_gen_time)") 
        fps =  optimal_fps


        self.temporalController.setFramesPerSecond(fps)
        self.fps_record.append(fps)

    def get_animation_direction(self, curr_frame):
        """
        Returns the current direction of the animation.
        Udates the last_frame attribute.
        """

        if self.last_frame - curr_frame > 0:
            self.direction = Animation_direction.BACKWARD
            if curr_frame <= 0: # REACHED THE BEGINNING OF THE ANIMATION
                self.temporalController.setCurrentFrameNumber(0)
                print("Reached the beginning of the animation")
                self.pause()
        else:
            self.direction = Animation_direction.FORWARD
            if curr_frame >= self.end_frame : # REACHED THE END OF THE ANIMATION
                self.temporalController.setCurrentFrameNumber(self.end_frame)
                print("Reached the end of the animation")
                self.pause()

        self.last_frame = curr_frame
    
    def get_current_st_box_extent(self):
        """
        Returns the current extent of the canvas.
        """
        return np.array([self.canvas.extent().xMinimum(), 
                         self.canvas.extent().yMinimum(), 
                         self.canvas.extent().xMaximum(), 
                         self.canvas.extent().yMaximum()])

    def request_next_time_delta_thread(self, curr_frame):
        self.update_vlayer_content()
        print(f"$$$$NEXT_TIME_DELTA_REQUEST$$$$ \n [ Time delta  ({self.current_time_delta} : {self.data.timestamps_strings[self.current_time_delta]}) \n Frame : {curr_frame}")

        st_box_extent  = self.get_current_st_box_extent()
        print(f"extent : {st_box_extent}")

        if self.direction == Animation_direction.BACKWARD:
            # Going back in time
            self.current_time_delta = (curr_frame - FRAMES_PER_TIME_DELTA)
            start = curr_frame-(FRAMES_PER_TIME_DELTA)
            end = curr_frame
            self.data.update_cache_in_memory(curr_frame-FRAMES_PER_TIME_DELTA, self.direction)
            self.data.fetch_data_with_thread(start, end, st_box_extent) 

        elif self.direction == Animation_direction.FORWARD:
            # Going forward in time
            self.current_time_delta = curr_frame
            start = curr_frame
            end = curr_frame+FRAMES_PER_TIME_DELTA
            self.data.update_cache_in_memory(curr_frame, self.direction)
            self.data.fetch_data_with_thread(start, end, st_box_extent)


    def on_new_frame(self):
        """
        TODO : Divide this function into smaller functions to make it more readable
        
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        TIME_new_frame = time.time()

        curr_frame = self.temporalController.currentFrameNumber()
        print(f"\nFrame : {curr_frame}")
        
        self.get_animation_direction(curr_frame)

        if curr_frame % FRAMES_PER_TIME_DELTA == 0:
            self.request_next_time_delta_thread(curr_frame)
        else: 
            self.update_vlayer_content()
            print(self.direction)
            new_frame_time = time.time()-TIME_new_frame
            

            print(f"Time for on_new_frame : {new_frame_time}")
            self.update_frame_rate(new_frame_time)
    
    def update_vlayer_content(self):
        """
        Updates the content of the vector layer displayed on the map.
        """
        self.delete_vlayer_features() # Deletes all previous points
        self.add_vlayer_features(self.last_frame)

    
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

 

    def add_vlayer_features(self, currentFrameNumber=0):
        """
        Adds the points to the vector layer to be displayed for the current frame on the map.
        """
        now= time.time()

        qgis_fields_list = self.data.generate_qgis_points(self.current_time_delta,currentFrameNumber, self.vlayer.fields())
        number_of_features = len(qgis_fields_list)
        print(f" Added {number_of_features} features")
        self.vlayer.startEditing()
        self.vlayer.addFeatures(qgis_fields_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        print(f"add_valyer_features time : {time.time()-now}")
        self.feature_number_record.append(number_of_features)
        

    def delete_vlayer_features(self):
        now= time.time()
        self.vlayer.startEditing()
        delete_ids = [f.id() for f in self.vlayer.getFeatures()]
        self.vlayer.deleteFeatures(delete_ids)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)

        print(f"delete_vlayer_features time : {time.time()-now}")





tt = QVIZ()