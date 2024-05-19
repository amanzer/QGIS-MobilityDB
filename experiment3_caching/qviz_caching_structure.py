from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time

import psycopg2


PERCENTAGE_OF_SHIPS = 0.5 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA = 48 # 48 ticks of data are loaded at once


class ParallelTask(QgsTask):
    """
    This class is used to fetch the data from the MobilityDB database in parallel
    using Qgis's threads. This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, current_frame, delta_key, project_title,db,mmsi_list, pstart, pend, finished_fnc,
                 failed_fnc):
        super(ParallelTask, self).__init__(description, QgsTask.CanCancel)
        self.current_frame = current_frame
        self.delta_key = delta_key
        self.project_title = project_title
        self.db = db
        self.mmsi_list = mmsi_list
        self.pstart = pstart
        self.pend = pend
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
            features = self.db.getTrajectories(self.mmsi_list, self.pstart, self.pend)
            
            self.result_params = {
                'pymeos_data_batch': features,
                'delta_key': self.delta_key,
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


class Data_in_memory:
    """
    MVC : This is the model

    This class plays the role of the model in the MVC pattern. 
    It is used to store the data in memory and to create the link betwseen
    the QGIS UI (temporal Controller/Vector Layer) and the MobilityDB database.
    
    """
    def __init__(self):
        self.task_manager = QgsApplication.taskManager()
        self.db = mobDB()
        pymeos_initialize()
        
        self.mmsi_list = self.db.getMMSI(PERCENTAGE_OF_SHIPS)
        
        self.current_batch = []
        self.future_batch = []
        self.steps = 1440

        start_date = datetime(2023, 6, 1, 0, 0, 0)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(self.steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]

        self.buffer = {}

        
        self.fetchMobilityDB(0, self.timestamps_strings[0], self.mmsi_list, self.timestamps[0], self.timestamps[TIME_DELTA-1])


    def fetchMobilityDB(self,current_frame,delta_key, mmsi_list, pstart, pend):
        task = ParallelTask(f"Next batch is being requested: {current_frame}", current_frame,delta_key,
                                     "qViz",self.db,mmsi_list, pstart, pend, self.finish, self.raise_error)

        self.task_manager.addTask(task)        

    

    def fetchNextBatch(self, current_frame):
        delta_i = current_frame + TIME_DELTA         
        delta_key = self.timestamps_strings[delta_i]
        if delta_key not in self.buffer:
            delta_i_plus_one = delta_i + TIME_DELTA - 1
            self.fetchMobilityDB(delta_i, delta_key, self.mmsi_list, self.timestamps[delta_i], self.timestamps[delta_i_plus_one])


    def finish(self, params):
        """
        Function called when the task to fetch the data from the MobilityDB database is finished.
        """
        # check delta_key exists in buffer        
        self.buffer[params['delta_key']] = params['pymeos_data_batch']


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
        key =  self.timestamps_strings[frame_number]
 
        qgis_fields_list = []
        
        datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
        
        current_batch = self.buffer[self.timestamps_strings[current_time_delta]]

        for mmsi in self.mmsi_list:
            try :
                val = current_batch[mmsi].value_at_timestamp(self.timestamps[frame_number])
                feat = QgsFeature(vlayer_fields)   # Create feature
                feat.setAttributes([datetime_obj])  # Set its attributes
                x,y = val.x, val.y
                geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                feat.setGeometry(geom) # Set its geometry
                qgis_fields_list.append(feat)

            except Exception as e: 
                continue
                
        print(f"Added {len(qgis_fields_list)} features to timestamp {key}")
        return qgis_fields_list

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
        self.current_time_delta = 0
        self.previous_frame = 0
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        #self.generatePoints()
        #self.addPoints()
        self.on_new_frame_times = []
        self.removePoints_times = []
        self.update_features_times = []
        self.number_of_points_stored_in_layer = []



    def get_stats(self):
        pass

    
    def on_new_frame(self):
        """
        Function called every time the temporal controller frame is changed. 
        It updates the content of the vector layer displayed on the map.
        """

        curr_frame = self.temporalController.currentFrameNumber()
        print(curr_frame)
        if curr_frame % TIME_DELTA == 0:
            if self.previous_frame-curr_frame > 0:
                # Going back in time
                self.current_time_delta = curr_frame - TIME_DELTA
            else:
                # Going forward in time
                self.current_time_delta = curr_frame
            
            self.data.fetchNextBatch(curr_frame)            
            self.previous_frame = curr_frame
            #self.features = self.getFeatures()

        self.removePoints() # Deletes all previous points
        self.addPoints(curr_frame)
        
    
    
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