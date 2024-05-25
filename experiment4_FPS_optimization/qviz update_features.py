"""
in this version, we load the next time delta values in parallel using QGIS's 
threads, and we ALSO do the value_at_timestamps call in it.

"""


from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
import math

import psycopg2
from collections import deque


PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA = 48 # Parameter that defines the size of the batch of data to fetch from the MobilityDB database
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

        
        self.fetchMobilityDB(0, self.timestamps_strings[0], self.mmsi_list, self.timestamps[0], self.timestamps[TIME_DELTA-1])


    def generate_qgis_features(self, vlayer_fields):
        """
        Generates the qgis features to be added to the vector layer.
        Creates the hashmap for all features ids and mmsi values.
        """
        features = []
        feature_ids = {}
        datetime_obj = QDateTime.fromString(self.timestamps_strings[0], "yyyy-MM-dd HH:mm:ss")
        for i in range(len(self.mmsi_list)):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([datetime_obj])  # Set its attributes
            empty_geometry = QgsGeometry()
            feat.setGeometry(empty_geometry) # Set its geometry
            features.append(feat)
            feature_ids[self.mmsi_list[i]] = i+1
        return features, feature_ids

            
    def fetchMobilityDB(self,current_frame,delta_key, mmsi_list, pstart, pend):
        task = ParallelTask(f"Next batch is being requested: {current_frame}", current_frame,delta_key,
                                     "qViz",self.db,mmsi_list, pstart, pend, self.timestamps, self.finish, self.raise_error)

        self.task_manager.addTask(task)        

    


    
    def fetch_batch(self, start_frame, end_frame):
        delta_key = self.timestamps_strings[start_frame]
        if delta_key not in self.buffer and start_frame <= self.steps - TIME_DELTA:
            self.fetchMobilityDB(start_frame, delta_key, self.mmsi_list, self.timestamps[start_frame], self.timestamps[end_frame])



    def finish(self, params):
        """
        Function called when the task to fetch the data from the MobilityDB database is finished.
        """
        # check delta_key exists in buffer        
        self.buffer[params['delta_key']] = params['batch']


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

    
    def generate_qgis_points2(self,current_time_delta, frame_number, vlayer_fields):
        """
        Provides the UI with the features to display on the map for the Timestamp associated
        to the given frame number.

        """
        time_delta_key = self.timestamps_strings[current_time_delta]
        
        
        key =  self.timestamps_strings[frame_number]
 
        qgis_fields_list = []
        
        datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
        now_value_at_ts_qgs_feature = time.time()
        current_frame_coords = {}
        try: 
            # Get the subsets of the Tpoints for the current time delta 
            current_batch = self.buffer[time_delta_key]
            
            current_frame_coords = current_batch[frame_number]
           
        except Exception as e:
            pass 
        
        
        print(f"Added {len(qgis_fields_list)} features to timestamp {key}")
        print(f"time for QgsFeature generation : {time.time() - now_value_at_ts_qgs_feature}")
        return current_frame_coords, datetime_obj



    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)



class ParallelTask(QgsTask):
    """
    This class is used to fetch the data from the MobilityDB database in parallel
    using Qgis's threads. This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, current_frame, delta_key, project_title,db,mmsi_list, pstart, pend, timestamps, finished_fnc,
                 failed_fnc):
        super(ParallelTask, self).__init__(description, QgsTask.CanCancel)
        self.current_frame = current_frame
        self.delta_key = delta_key
        self.project_title = project_title
        self.db = db
        self.mmsi_list = mmsi_list
        self.pstart = pstart
        self.pend = pend
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
            features = self.db.getTrajectories(self.mmsi_list, self.pstart, self.pend)

            batch_coords = {}
            start = self.current_frame            
            for i in range(TIME_DELTA+1):
                key = start + i
                batch_coords[key] = {}
                for mmsi in self.mmsi_list:
                    try:
                        coords = features[mmsi].value_at_timestamp(self.timestamps[key])
                        batch_coords[key][mmsi] = (coords.x, coords.y)
                    except Exception as e:
                        continue


            self.result_params = {
                'pymeos_data_batch': features,
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

        self.createVectorLayer()
        self.canvas = iface.mapCanvas()
        self.temporalController = self.canvas.temporalController()
        frame_rate = 30
        self.temporalController.setFramesPerSecond(frame_rate)

        self.data =  Data_in_memory()
        self.generate_features()
        self.current_time_delta = 0
        self.last_frame = 0
        #self.on_new_frame()
        
        self.dq_FPS = deque(maxlen=LEN_DEQUEUE)
        for i in range(LEN_DEQUEUE):
            self.dq_FPS.append(0.033)
        self.fps_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        
        # To start we fetch the first 2 time deltas in advance
        
        self.data.fetch_batch(0, TIME_DELTA)
        self.data.fetch_batch(TIME_DELTA, 2*TIME_DELTA)

        
    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """
        return sum(self.fps_record)/len(self.fps_record)
    

    def generate_features(self):
        """
        Generates the features to be displayed on the map.
        """
        self.features, self.features_ids = self.data.generate_qgis_features(self.vlayer.fields())

        self.vlayer.startEditing()
        self.vlayer.addFeatures(self.features)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        


    def updateFrameRate(self, time):
        """
        Updates the frame rate of the temporal controller.
        """
        self.dq_FPS.append(time)
        avg_frame_time = (sum(self.dq_FPS)/LEN_DEQUEUE)
        print(f"Average time for On_new_frame : {avg_frame_time}")
        optimal_fps = 1 / avg_frame_time
        print(f"Optimal FPS : {optimal_fps} (FPS = 1/frame_gen_time)") 
        fps = min(30, optimal_fps)


        self.temporalController.setFramesPerSecond(fps)
        self.fps_record.append(fps)

    
    def on_new_frame(self):
        """
        Function called every time the temporal controller frame is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()
        curr_frame = self.temporalController.currentFrameNumber()
        print(f"\n\n\n\n\n\ncurr_frame : {curr_frame}")

        if self.last_frame - curr_frame > 0:
            direction = "back" 
        else:
            direction = "forward"
        self.last_frame = curr_frame

        if curr_frame % TIME_DELTA == 0:            
            print(f"DOTHRAKIS ARE COMING\n Time delta : {self.current_time_delta} : {self.data.timestamps_strings[self.current_time_delta]} \n Frame : {curr_frame}")
            if direction == "back":
                # Going back in time
                self.current_time_delta = (curr_frame - TIME_DELTA)
                start = curr_frame-(2*TIME_DELTA)
                end = curr_frame-TIME_DELTA
                self.data.fetch_batch(start, end)   

            elif direction == "forward":
                # Going forward in time
                self.current_time_delta = curr_frame
                start = curr_frame+TIME_DELTA
                end = curr_frame+(2*TIME_DELTA)
                self.data.fetch_batch(start, end)   
            
                     
            self.last_frame = curr_frame
      

        self.update_positions(curr_frame)
   
        print(direction)
        t = time.time()-now
        self.updateFrameRate(t)
    

    def update_positions(self, currentFrameNumber):
        """
        Updates the positions of the points on the map.
        """
        now= time.time()
        new_positions, date_obj = self.data.generate_qgis_points2(self.current_time_delta,currentFrameNumber, self.vlayer.fields())
   
        self.vlayer.startEditing()
        
        for mmis, coords in new_positions.items():
            feature_id = self.features_ids[mmis]
            geometry = QgsGeometry.fromPointXY(QgsPointXY(coords[0], coords[1]))
            self.vlayer.changeGeometry(feature_id, geometry)
            self.vlayer.changeAttributeValue(feature_id, 0, date_obj)
            
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
        
        # len of dicitonnary
        print(f"Updated {len(new_positions)} features to frame {currentFrameNumber}")

        print(f"time for update_positions: {time.time() - now}")


    
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

  





tt = qviz()