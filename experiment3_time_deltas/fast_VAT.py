"""
Whole purpose of this file is to test how the performance is impacted if Value_At_Timestamp takes no time to execute.

"""


from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
import math

import psycopg2
from collections import deque


PERCENTAGE_OF_SHIPS = 0.01 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA = 48 # 48 ticks of data are loaded at once
LEN_DEQUEUE = 10 # Length of the dequeue to calculate the average FPS


class Data_in_memory:
    """
    MVC : This is the model

    This class plays the role of the model in the MVC pattern. 
    It is used to store the data in memory and to create the link betwseen
    the QGIS UI (temporal Controller/Vector Layer) and the MobilityDB database.
    
    """
    def __init__(self):
        #Metrics to measure 
        self.STATS_value_at_timestamp = [] # Time to get the value for a timestamp by pymeos
        self.STATS_qgis_features = [] # Time to create the QGIS features    


        self.task_manager = QgsApplication.taskManager()
        self.db = mobDB()
        pymeos_initialize()
        
        self.mmsi_list = self.db.getMMSI(PERCENTAGE_OF_SHIPS)
        
        self.steps = 1440

        start_date = datetime(2023, 6, 1, 0, 0, 0)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(self.steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]

        self.buffer = {}

        self.fillBuffer()
      

    def fillBuffer(self):
        """
        Fill the buffer with the first TIME_DELTA timestamps
        """
        task = ParallelTask(f"Filling the buffer", self.timestamps,self.timestamps_strings,
                                     "qViz",self.db,self.mmsi_list, self.steps , self.finish, self.raise_error)
        
        self.task_manager.addTask(task) 


    def finish(self, params):
        """
        Function called when the task to fetch the data from the MobilityDB database is finished.
        """
        # check delta_key exists in buffer        
        self.buffer = params['buffer']


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")


    def setNextBatch(self, batch):
        self.setNextBatch = batch

    def generate_qgis_points(self,current_time_delta, frame_number, vlayer_fields):
        """
        Provides the UI with the features to display on the map for the Timestamp associated
        to the given frame number.

        """
        time_delta_key = self.timestamps_strings[current_time_delta]
        
        
        key =  self.timestamps_strings[frame_number]
 
        qgis_fields_list = []
        
        datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
        try: 
            current_batch = self.buffer[time_delta_key]
            
            for mmsi in self.mmsi_list:
                try :
                    
                    
                    now_qgis_features = time.time()
                    feat = QgsFeature(vlayer_fields)   # Create feature
                    feat.setAttributes([datetime_obj])  # Set its attributes
                    x,y = 0, 0
                    geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                    feat.setGeometry(geom) # Set its geometry
                    qgis_fields_list.append(feat)
                    self.STATS_qgis_features.append(time.time()-now_qgis_features)
                except Exception as e: 
                    continue
        except Exception as e:
            pass 
        
        
        print(f"Added {len(qgis_fields_list)} features to timestamp {key}")
        return qgis_fields_list
    
    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)


class ParallelTask(QgsTask):
    """
    This class is used to fetch the data from the MobilityDB database in parallel
    using Qgis's threads. This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, timestamps, timestamps_strings, project_title,db,mmsi_list, steps, finished_fnc,
                 failed_fnc):
        super(ParallelTask, self).__init__(description, QgsTask.CanCancel)
        self.timestamps = timestamps
        self.timestamps_strings = timestamps_strings
        self.project_title = project_title
        self.db = db
        self.mmsi_list = mmsi_list
        self.steps = steps
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
        try:
            deltas = {}
            for i in range( (math.ceil(self.steps/ TIME_DELTA)) ):
                delta_i = i * TIME_DELTA         
                delta_i_plus_one = delta_i + TIME_DELTA
                if delta_i_plus_one == self.steps:
                    delta_i_plus_one = self.steps - 1 
                pstart = self.timestamps[delta_i]
                pend = self.timestamps[delta_i_plus_one]

                key = self.timestamps_strings[delta_i]

                deltas[key] = self.db.getTrajectories(self.mmsi_list, pstart, pend)
            
            self.result_params = {
                'buffer': deltas,
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

    def getTrajectories(self, mmsi_list, pstart, pend):
        """
        Fetch the trajectories of the ships in the mmsi_list between the start and end timestamps.
        """
        try:
            rows={}
            for mmsi in mmsi_list:
                ship_mmsi = mmsi[0]
                self.cursor.execute(f"SELECT attime(a.trajectory::tgeompoint,span('{pstart}'::timestamptz, '{pend}'::timestamptz, true, true))::tgeompoint FROM public.PyMEOS_demo as a WHERE a.MMSI = {ship_mmsi} ;")
                trajectory = self.cursor.fetchone()
                if trajectory[0]:
                    rows[mmsi] = trajectory[0]

            return rows
        except Exception as e:
            print(e)


    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()


class qviz:
    """
    MVC : This is the controller

    This class plays the role of the controller in the MVC pattern.
    It is used to manage the user interaction with the View, which is the QGIS UI.
    
    It handles the interactions with both the Temporal Controller and the Vector Layer.
    """
    def __init__(self):
        self.on_new_frame_times = []
        self.removePoints_times = []
        self.update_features_times = []
        self.number_of_points_stored_in_layer = []    

        self.createVectorLayer()
        self.canvas = iface.mapCanvas()
        self.temporalController = self.canvas.temporalController()
        frame_rate = 30
        self.temporalController.setFramesPerSecond(frame_rate)

        self.data =  Data_in_memory()
        self.current_time_delta = 0
        self.last_frame = 0
        self.dq_FPS = deque(maxlen=LEN_DEQUEUE)
        self.dq_FPS.append(1)
        #self.on_new_frame()
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        
   



    def get_stats(self):
        """
        Returns the statistics of the time taken by each function.
        """
        avg_value_at_timestamp = sum(self.data.STATS_value_at_timestamp)/len(self.data.STATS_value_at_timestamp)
        #avg_qgis_features = sum(self.data.STATS_qgis_features)/len(self.data.STATS_qgis_features)
        # show average in seconds
        print(f"Number of times value_at_timestamp was called: {len(self.data.STATS_value_at_timestamp)}")
        print(f"Average time to get value at timestamp: {avg_value_at_timestamp}s")
        print(f"Max time to get value at timestamp: {max(self.data.STATS_value_at_timestamp)}s")
        print(f"Min time to get value at timestamp: {min(self.data.STATS_value_at_timestamp)}s")
        

        print(f"Update features times: {sum(self.update_features_times) / len(self.update_features_times)}")
        print(f"On new frame times: {sum(self.on_new_frame_times) / len(self.on_new_frame_times)}, number of times it was called {len(self.on_new_frame_times)}")
    

    def updateFrameRate(self, time):
        """
        Updates the frame rate of the temporal controller.
        """
        self.dq_FPS.append(time)
        avg_frame_time = (sum(self.dq_FPS)/LEN_DEQUEUE)
        print(avg_frame_time)
        print(1 / avg_frame_time) 
        fps = min(30, (1 / avg_frame_time))


        self.temporalController.setFramesPerSecond(fps)


    def on_new_frame(self):
        """
        Function called every time the temporal controller frame is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()
        curr_frame = self.temporalController.currentFrameNumber()
        print(curr_frame)

        if self.last_frame - curr_frame > 0:
            direction = "back" 
        else:
            direction = "forward"
        self.last_frame = curr_frame

        if curr_frame % TIME_DELTA == 0:
            if direction == "back":
                # Going back in time
                self.current_time_delta = (curr_frame - TIME_DELTA)
            elif direction == "forward":
                # Going forward in time
                self.current_time_delta = curr_frame
            print("DOTHRAKIS ARE COMING")
            print(self.current_time_delta)
            print(curr_frame)
            print(self.data.timestamps_strings[self.current_time_delta])           
            self.last_frame = curr_frame
            #self.features = self.getFeatures()

        self.removePoints() # Deletes all previous points
        self.addPoints(curr_frame)
        print(direction)
        t = time.time()-now
        self.on_new_frame_times.append(t)
        self.updateFrameRate(t)
        
    
    
    def createVectorLayer(self):
        """
        Creates a vector layer in memory to store the points to be displayed on the map.
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

  

    def addPoints(self, currentFrameNumber=0):
        """
        Adds the points to the vector layer to be displayed for the current frame on the map.
        """
        #self.updateTimestamps()
        #self.features.update(self.timestamps)
        now= time.time()

        self.qgis_fields_list = self.data.generate_qgis_points(self.current_time_delta,currentFrameNumber, self.vlayer.fields())
        
        
        self.vlayer.startEditing()
        self.vlayer.addFeatures(self.qgis_fields_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        self.update_features_times.append(time.time()-now)
        self.number_of_points_stored_in_layer.append(len(self.qgis_fields_list))

    def removePoints(self):
        now= time.time()
        self.vlayer.startEditing()
        delete_ids = [f.id() for f in self.vlayer.getFeatures()]
        self.vlayer.deleteFeatures(delete_ids)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        self.removePoints_times.append(time.time()-now)






tt = qviz()