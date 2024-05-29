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
    MICROSECOND = timedelta(microseconds=1)
    MILLISECOND = timedelta(milliseconds=1)
    SECOND = timedelta(seconds=1)
    MINUTE = timedelta(minutes=1)
    HOUR = timedelta(hours=1)
    DAY = timedelta(days=1)
    WEEK = timedelta(weeks=1)


LEN_DEQUEUE_FPS = 5 # Length of the dequeue to calculate the average FPS
LEN_DEQUEUE_BUFFER = 2 # Length of the dequeue to keep the keys to keep in the buffer


PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
FRAMES_PER_TIME_DELTA = 48 # Number of frames associated to one Time delta
GRANULARITY = Time_granularity.MINUTE

class Data_in_memory:
    """
    This class handles the data stored in memory and the background threads that fetch the data from the MobilityDB database.
    
    It is the link between the QGIS UI (temporal Controller/Vector Layer controlled by the qviz class) and the MobilityDB database.
    
    """
    def __init__(self, xmin, ymin, xmax, ymax):

        self.task_manager = QgsApplication.taskManager()
        self.db = mobDB()
        pymeos_initialize()
        
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

        self.mmsi_list = self.db.getMMSI(PERCENTAGE_OF_SHIPS) 
        
        self.generate_timestamps()

        self.buffer = {}
        self.keys_to_keep = deque(maxlen=LEN_DEQUEUE_BUFFER)
        self.keys_to_keep.append(self.timestamps_strings[0])
        task = QgisThread(f"Batch requested for time delta {0} - {self.timestamps_strings[0]}", 0, self.timestamps_strings[0],
                                     "qViz",self.db,self.mmsi_list, 0, FRAMES_PER_TIME_DELTA, self.xmin, self.ymin, self.xmax, self.ymax , self.timestamps, self.on_thread_completed, self.raise_error)
        
        self.task_manager.addTask(task)     
    

    def generate_timestamps(self):
        """
        TODO : FRAMES_PER_TIME_DELTA should be defined here depending on the granularity selected
        TODO : Granularity should be taken from Temporal Controller
        TODO : 
        Fetch min and max timestamps from the MobilityDB database

        SELECT MIN(startTimestamp(trajectory)) AS earliest_timestamp
        FROM pymeos_demo;

        SELECT MAX(endTimestamp(trajectory)) AS latest_timestamp
        FROM pymeos_demo;
        """
        start_date = datetime(2023, 6, 1, 0, 0, 0)
        end_date = datetime(2023, 6, 1, 23, 59, 58)
        self.total_frames = (end_date - start_date) // GRANULARITY.value

        self.timestamps = [start_date + i * GRANULARITY.value for i in range(self.total_frames)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]



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
        print(self.keys_to_keep)

    def flush_buffer(self):
        """
        Removes all keys/values associated to past time deltas from the buffer.

        Forecefully calls the garbage collector to free the memory.
        """
        #remove from buffer all the keys that are not in the keys_to_keep
        for key in list(self.buffer.keys()):
            if key not in self.keys_to_keep:
                print("Deleting key : ", key)
                del self.buffer[key]
                gc.collect() 
        size_in_bytes = asizeof.asizeof(self.buffer)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        print(f"Total size of dictionary (including referenced objects): {size_in_megabytes:.6f} MB")
    

    def fetch_data_with_thread(self, start_frame, end_frame, xmin, ymin, xmax, ymax):
        """
        Creates a thread to fetch the data from the MobilityDB database for the given time delta.
        """
        delta_key = self.timestamps_strings[start_frame]

        if end_frame  <= (len(self.timestamps)) and start_frame >= 0:
            print(f"Fetching batch for {start_frame} to {end_frame} aka {self.timestamps_strings[start_frame]} to {self.timestamps_strings[end_frame]}")

            task = QgisThread(f"Batch requested for time delta {start_frame} - {self.timestamps_strings[start_frame]}", start_frame,delta_key,
                                     "qViz",self.db,self.mmsi_list, start_frame, end_frame, xmin, ymin, xmax, ymax, self.timestamps, self.on_thread_completed, self.raise_error)

            self.task_manager.addTask(task)        


    def on_thread_completed(self, params):
        """
        Function called when a thread finishes its job to fetch the data from the MobilityDB database.
        """
        # check delta_key exists in buffer        
        self.buffer[params['delta_key']] = params['batch']
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

            
            
            print(f"QgsFeature generation time : {time.time() - now_value_at_ts_qgs_feature}")
            print(f"Timestamp {key}", end=" ")
            return qgis_fields_list
        except Exception as e:
            print(e)
            return []

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class QgisThread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    Parameters include : the time delta, STBOX paramters, Time range...
    
    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, current_frame, delta_key, project_title,db,mmsi_list, pstart, pend, xmin, ymin, xmax, ymax, timestamps, finished_fnc,
                 failed_fnc):
        super(QgisThread, self).__init__(description, QgsTask.CanCancel)
        self.current_frame = current_frame
        self.delta_key = delta_key
        self.project_title = project_title
        self.db = db
        self.mmsi_list = mmsi_list
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
            stats = []
            now = time.time()

            features = self.db.getTrajectories(self.mmsi_list, self.timestamps[self.pstart], self.timestamps[self.pend], self.xmin, self.ymin, self.xmax, self.ymax)
            stats.append(f"Time to fetch subTpoints from MobilityDB : {time.time()-now} s")

            now2 = time.time()
            batch_coords = {}           
            for key in range(self.pstart,self.pend +1):
                batch_coords[key] = []
                for mmsi in self.mmsi_list:
                    try:
                        coords = features[mmsi].value_at_timestamp(self.timestamps[key])
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



class mobDB:
    """
    Singleton class used to connect to the MobilityDB database and retrieve the MMSI of ships and their trajectories.
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
            
            self.connection = MobilityDB.connect(**connection_params)

            self.cursor = self.connection.cursor()

            self.cursor.execute(f"SELECT MMSI FROM public.PyMEOS_demo;")
            self.mmsi_list = self.cursor.fetchall()
        except Exception as e:
            print(e)

    def getMMSI(self, percentage=0.001):
        """
        Fetch the MMSI of the ships in the database.
        """
        return self.mmsi_list[:int(len(self.mmsi_list)*percentage)]

    def getTrajectories(self, mmsi_list, pstart, pend, xmin, ymin, xmax, ymax):
        """
        Fetch the trajectories of the ships in the mmsi_list between the start and end timestamps.
        """
        SRID = 4326
        
        try:
            rows={}
            for mmsi in mmsi_list:
                ship_mmsi = mmsi[0]
                query = f"""
                        SELECT 
                            atStbox(
                        a.trajectory::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                            {xmin}, {ymin}, -- xmin, ymin
                            {xmax}, {ymax}, -- xmax, ymax
                            {SRID} -- SRID
                            ),
                            tstzspan('[{pstart}, {pend}]')
                        )
                        )
                            FROM public.PyMEOS_demo as a 
                        WHERE a.MMSI = {ship_mmsi} ;
                        """
                self.cursor.execute(query)
                #self.cursor.execute(f"SELECT attime(a.trajectory::tgeompoint,span('{pstart}'::timestamptz, '{pend}'::timestamptz, true, true))::tgeompoint FROM public.PyMEOS_demo as a WHERE a.MMSI = {ship_mmsi} ;")
                trajectory = self.cursor.fetchone()
                if trajectory[0]:
                    rows[mmsi] = trajectory[0]

            return rows
        except Exception as e:
            print(e)

    def get_min_max_timestamps(self):
        """
        SELECT MIN(startTimestamp(trajectory)) AS earliest_timestamp
        FROM pymeos_demo;

        SELECT MAX(endTimestamp(trajectory)) AS latest_timestamp
        FROM pymeos_demo;
        """
        pass

    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()


class qviz:
    """

    This class plays the role of the controller in the MVC pattern.
    It is used to manage the user interaction with the View, which is the QGIS UI.
    
    It handles the interactions with both the Temporal Controller and the Vector Layer.
    """
    def __init__(self):    
        self.create_vlayer()
        self.canvas = iface.mapCanvas()
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem("EPSG:0"))
        self.temporalController = self.canvas.temporalController()
        frame_rate = 30
        self.direction = "forward"
        self.temporalController.setFramesPerSecond(frame_rate)

        # TODO : use self.canvas and reduce 4 float variables into 1 string  
        self.xmin = -3.01733170955181862
        self.ymin =  48.69190684846805084
        self.xmax = 22.73323493966157827
        self.ymax = 66.54173145758187502
        print(f"Extents : {self.xmin}, {self.ymin}, {self.xmax}, {self.ymax}")
        self.data =  Data_in_memory(self.xmin, self.ymin, self.xmax, self.ymax)
        self.data.task_manager.taskAdded.connect(self.pause)
        self.data.task_manager.allTasksFinished.connect(self.play)
        self.current_time_delta = 0
        self.last_frame = 0
        self.update_vlayer_content
        
        self.dq_FPS = deque(maxlen=LEN_DEQUEUE_FPS)
        for i in range(LEN_DEQUEUE_FPS):
            self.dq_FPS.append(0.033)

        self.fps_record = []
        self.feature_number_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
    

    
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
        # self.dq_FPS.append(new_frame_time)
        # avg_frame_time = (sum(self.dq_FPS)/LEN_DEQUEUE_FPS)
        # print(f"Average time for On_new_frame : {avg_frame_time}")
        optimal_fps = 1 / new_frame_time
        print(f"Optimal FPS : {optimal_fps} (FPS = 1/frame_gen_time)") 
        fps =  optimal_fps


        self.temporalController.setFramesPerSecond(fps)
        self.fps_record.append(fps)

    
    def on_new_frame(self):
        """
        TODO : Divide this function into smaller functions to make it more readable
        
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()

        curr_frame = self.temporalController.currentFrameNumber()
        print(f"\n\n\n\n\n\ncurr_frame : {curr_frame}")
        if curr_frame == 240:
            self.pause()
            return
        if self.last_frame - curr_frame > 0:
            self.direction = "back"
            if curr_frame <= 0:
                self.update_vlayer_content()
                print(self.direction)
                new_frame_time = time.time()-now
                #print(f"Time for on_new_frame : {t}")
                self.update_frame_rate(new_frame_time)
        else:
            self.direction = "forward"
            if curr_frame >= len(self.data.timestamps)-1:
                self.update_vlayer_content()
                print(self.direction)
                new_frame_time = time.time()-now
                #print(f"Time for on_new_frame : {t}")
                self.update_frame_rate(new_frame_time)

        self.last_frame = curr_frame

        if curr_frame % FRAMES_PER_TIME_DELTA == 0:
            self.update_vlayer_content()
            print(f"DOTHRAKIS ARE COMING\n Time delta : {self.current_time_delta} : {self.data.timestamps_strings[self.current_time_delta]} \n Frame : {curr_frame}")
            # TODO : use self.canvas and reduce 4 float variables into 1 string
            self.xmin = iface.mapCanvas().extent().xMinimum()
            self.ymin = iface.mapCanvas().extent().yMinimum()
            self.xmax = iface.mapCanvas().extent().xMaximum()
            self.ymax = iface.mapCanvas().extent().yMaximum()
            print(f"Extents : {self.xmin}, {self.ymin}, {self.xmax}, {self.ymax}")
            if self.direction == "back":
                # Going back in time
                self.current_time_delta = (curr_frame - FRAMES_PER_TIME_DELTA)
                start = curr_frame-(FRAMES_PER_TIME_DELTA)
                end = curr_frame
                self.data.update_keys_to_keep(curr_frame-FRAMES_PER_TIME_DELTA, self.direction)
                self.data.flush_buffer()
                self.data.fetch_data_with_thread(start, end, self.xmin, self.ymin, self.xmax, self.ymax) 

            elif self.direction == "forward":
                # Going forward in time
                self.current_time_delta = curr_frame
                start = curr_frame
                end = curr_frame+FRAMES_PER_TIME_DELTA
                self.data.update_keys_to_keep(curr_frame, self.direction)
                self.data.flush_buffer()
                self.data.fetch_data_with_thread(start, end, self.xmin, self.ymin, self.xmax, self.ymax)
        else: 
            self.update_vlayer_content()
            print(self.direction)
            new_frame_time = time.time()-now
            

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





tt = qviz()