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





class Time_granularity(Enum):
    MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes}
    HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours}
  


LEN_DEQUEUE_FPS = 5 # Length of the dequeue to calculate the average FPS
LEN_DEQUEUE_BUFFER = 2 # Length of the dequeue to keep the keys to keep in the buffer


PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
FRAMES_PER_TIME_DELTA = 120 # Number of frames associated to one Time delta
GRANULARITY = Time_granularity.MINUTE

class Data_in_memory:
    """
    This class handles the data stored in memory and the background threads that fetch the data from the MobilityDB database.
    
    It is the link between the QGIS UI (temporal Controller/Vector Layer controlled by the qviz class) and the MobilityDB database.
    
    """
    def __init__(self, xmin, ymin, xmax, ymax):

        self.task_manager = QgsApplication.taskManager()
        self.db = MobilityDB_Database()
        pymeos_initialize()
        
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

        self.ids_list = self.db.get_subset_of_ids(PERCENTAGE_OF_SHIPS) 
        
        self.generate_timestamps()

        self.buffer = {}
        self.keys_to_keep = deque(maxlen=LEN_DEQUEUE_BUFFER)
        self.keys_to_keep.append(self.timestamps_strings[0])
        task = QgisThread(f"Batch requested for time delta {0} - {self.timestamps_strings[0]}", 0, self.timestamps_strings[0],
                                     "qViz",self.db,self.ids_list, 0, FRAMES_PER_TIME_DELTA, self.xmin, self.ymin, self.xmax, self.ymax , self.timestamps, self.on_thread_completed, self.raise_error)
        
        task.taskCompleted.connect(self.second_task)
        self.task_manager.addTask(task)     

    def second_task(self):
        """
        Function called when the task to fetch the second batch of data from the MobilityDB database is finished.
        """
        self.fetch_data_with_thread(FRAMES_PER_TIME_DELTA, FRAMES_PER_TIME_DELTA*2, self.xmin, self.ymin, self.xmax, self.ymax) 
    
    def generate_timestamps(self):
        """
        TODO : FRAMES_PER_TIME_DELTA should be defined here depending on the granularity selected
    
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

    def update_keys_to_keep(self, current_frame, direction):
        """
        TODO : Rename the methods related to the buffer and the buffer itself to make it more clear

        We want the buffer dictionnary to only keep the values associated to the keys in the keys_to_keep deque.
        By doing this we can only keep in memory the data necessary to the animation.

        """
        key = self.timestamps_strings[current_frame]

        if key in self.keys_to_keep:
            return
        elif direction == "forward":
            self.keys_to_keep.append(self.timestamps_strings[current_frame])
        elif direction == "back":
            self.keys_to_keep.appendleft(self.timestamps_strings[current_frame])
    

    def flush_buffer(self):
        """
        Removes all keys/values associated to past time deltas from the buffer.

        Forecefully calls the garbage collector to free the memory.
        """
        #remove from buffer all the keys that are not in the keys_to_keep
        for key in list(self.buffer.keys()):
            if key not in self.keys_to_keep:
                del self.buffer[key]
                #gc.collect() 
        size_in_bytes = asizeof.asizeof(self.buffer)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        self.log(f"Total size of dictionary (including referenced objects): {size_in_megabytes:.6f} MB")

 
    def fetch_data_with_thread(self, start_frame, end_frame, xmin, ymin, xmax, ymax):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        if self.task_manager.countActiveTasks() != 0:
            return None
        delta_key = self.timestamps_strings[start_frame]

        if end_frame  <= (len(self.timestamps)) and start_frame >= 0:

            task = QgisThread(f"Batch requested for time delta {start_frame} - {self.timestamps_strings[start_frame]}", start_frame,delta_key,
                                     "qViz",self.db,self.ids_list, start_frame, end_frame, xmin, ymin, xmax, ymax, self.timestamps, self.on_thread_completed, self.raise_error)

            self.task_manager.addTask(task)        


    def on_thread_completed(self, params):
        """
        Function called when a thread finishes its job to fetch the data from the MobilityDB database.
        """
        # check delta_key exists in buffer        
        self.buffer[params['delta_key']] = params['batch']
        # display stats from task 
 
        


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")


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
   
            current_batch = self.buffer[time_delta_key]
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

           
            return qgis_fields_list
        except Exception as e:
            return []

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class QgisThread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, current_frame, delta_key, project_title,db,ids_list, pstart, pend, xmin, ymin, xmax, ymax, timestamps, finished_fnc,
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
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
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
     

            features = self.db.get_subset_of_tpoints(self.ids_list, self.timestamps[self.pstart], self.timestamps[self.pend], self.xmin, self.ymin, self.xmax, self.ymax)
        
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
            #gc.collect()
     
            self.result_params = {
                'delta_key': self.delta_key,
                'batch' : batch_coords
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
            pass

    def get_subset_of_ids(self, percentage=0.001):
        """
        Returns a subset of the objects ids in the table, based on the given percentage.
        """
        return self.ids_list[:int(len(self.ids_list)*percentage)]

    def get_subset_of_tpoints(self, ids_list, pstart, pend, xmin, ymin, xmax, ymax):
        """
        For each object in the ids_list :
            Fetch the subset of the associated Tpoints between the start and end timestamps
            contained in the STBOX defined by the xmin, ymin, xmax, ymax.
             
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
                            {xmin}, {ymin}, -- xmin, ymin
                            {xmax}, {ymax}, -- xmax, ymax
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
            pass

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
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
        self.temporalController = self.canvas.temporalController()
        frame_rate = 30
        self.direction = "forward"
        self.temporalController.setFramesPerSecond(frame_rate)
        interval = QgsInterval(1, GRANULARITY.value["qgs_unit"])
        self.temporalController.setFrameDuration(interval)


        # TODO : use self.canvas and reduce 4 float variables into 1 string
        self.xmin = iface.mapCanvas().extent().xMinimum()
        self.ymin = iface.mapCanvas().extent().yMinimum()
        self.xmax = iface.mapCanvas().extent().xMaximum()
        self.ymax = iface.mapCanvas().extent().yMaximum()
        self.data =  Data_in_memory(self.xmin, self.ymin, self.xmax, self.ymax)
        # self.data.task_manager.taskAdded.connect(self.pause)
        #self.data.task_manager.allTasksFinished.connect(self.play)
        self.current_time_delta = 0
        self.last_frame = 0
        self.update_vlayer_content
        
        self.dq_FPS = deque(maxlen=LEN_DEQUEUE_FPS)
        for i in range(LEN_DEQUEUE_FPS):
            self.dq_FPS.append(0.033)

        self.fps_record = []
        self.feature_number_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        #self.temporalController.stateChanged.connect(self.pause)
        self.canvas.extentsChanged.connect(self.pause)
        self.data.update_temporal_controller_extent(self.temporalController)
        
    def play(self):
        """
        TODO : self.direction has to be replaced by the animation state of the Temporal Controller
        Plays the temporal controller animation.
        """
        self.update_vlayer_content()
        if self.direction == "forward":
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
        self.dq_FPS.append(new_frame_time)
        avg_frame_time = (sum(self.dq_FPS)/LEN_DEQUEUE_FPS)
        optimal_fps = 1 / avg_frame_time
        # print(f"Average time for On_new_frame : {avg_frame_time}")
        # optimal_fps = 1 / new_frame_time
        # print(f"Optimal FPS : {optimal_fps} (FPS = 1/frame_gen_time)") 
        
         # Define the set of target FPS values
        target_fps_values = [15, 20, 30, 60]
        
        # Choose the closest target FPS value
        fps = min(target_fps_values, key=lambda x: abs(x - optimal_fps))
        
        # fps =  min(optimal_fps, 60)



        self.temporalController.setFramesPerSecond(fps)
        self.log(f"FPS : {fps} - Calculated FPS : {optimal_fps}")
        self.fps_record.append(fps)

    
    def on_new_frame(self):
        """
        TODO : Divide this function into smaller functions to make it more readable
        
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()

        curr_frame = self.temporalController.currentFrameNumber()


        if self.last_frame - curr_frame > 0:
            self.direction = "back"
            if curr_frame <= 0:
                self.pause()
        else:
            self.direction = "forward"
            if curr_frame >= 1300:
                self.pause()

        self.last_frame = curr_frame

        if curr_frame % FRAMES_PER_TIME_DELTA == 0:
            self.update_vlayer_content()
            # self.xmin = iface.mapCanvas().extent().xMinimum()
            # self.ymin = iface.mapCanvas().extent().yMinimum()
            # self.xmax = iface.mapCanvas().extent().xMaximum()
            # self.ymax = iface.mapCanvas().extent().yMaximum()
            if self.direction == "back":
                # Going back in time
                self.current_time_delta = (curr_frame - FRAMES_PER_TIME_DELTA)
                start = curr_frame-(2*FRAMES_PER_TIME_DELTA)
                end = curr_frame-FRAMES_PER_TIME_DELTA
                self.data.update_keys_to_keep(curr_frame-FRAMES_PER_TIME_DELTA, self.direction)
                self.data.flush_buffer()
                self.data.fetch_data_with_thread(start, end, self.xmin, self.ymin, self.xmax, self.ymax) 

            elif self.direction == "forward":
                # Going forward in time
                self.current_time_delta = curr_frame
                start = curr_frame + FRAMES_PER_TIME_DELTA
                end = curr_frame+(2*FRAMES_PER_TIME_DELTA)
                self.data.update_keys_to_keep(curr_frame, self.direction)
                self.data.flush_buffer()
                self.data.fetch_data_with_thread(start, end, self.xmin, self.ymin, self.xmax, self.ymax)
        else: 
            self.update_vlayer_content()
             
            self.update_frame_rate(time.time()-now)
    
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

        qgis_fields_list = self.data.generate_qgis_points(self.current_time_delta,currentFrameNumber, self.vlayer.fields())

        self.vlayer.startEditing()
        self.vlayer.addFeatures(qgis_fields_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        

    def delete_vlayer_features(self):

        self.vlayer.startEditing()
        delete_ids = [f.id() for f in self.vlayer.getFeatures()]
        self.vlayer.deleteFeatures(delete_ids)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)




tt = QVIZ()