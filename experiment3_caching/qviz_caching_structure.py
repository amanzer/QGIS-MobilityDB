from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time



PERCENTAGE_OF_SHIPS = 0.01 # To not overload the memory, we only take a percentage of the ships in the database
TIME_DELTA = 48 # 48 ticks of data are loaded at once

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
        return self.mmsi_list[:int(len(self.mmsi_list)*percentage)]

    def getTrajectories(self, mmsi_list, pstart, pend):
        try:
            rows={}
            for mmsi in mmsi_list:
                ship_mmsi = mmsi[0]
                self.cursor.execute(f"SELECT attime(a.trajectory::tgeompoint,span('{pstart}'::timestamptz, '{pend}'::timestamptz, true, true))::tgeompoint FROM public.PyMEOS_demo as a WHERE a.MMSI = {ship_mmsi} ;")
                trajectory = self.cursor.fetchone()
                rows[mmsi] = trajectory[0]

            return rows
        except Exception as e:
            print(e)


    def close(self):
        self.cursor.close()
        self.connection.close()


class pyMeos_data:
    def __init__(self):
        self.current_batch = []
        self.future_batch = []
        self.steps = 1440

        start_date = datetime(2023, 6, 1, 0, 0, 0)
        end_date = datetime(2023, 6, 1, 23, 59, 59)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(self.steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]


    def fetchMobilityDB(self, mmsi_list, pstart, pend):
        task1 = QgsTask.fromFunction('Fetch data', fetch_data,

                             on_finished=completed, timestamps=timestamps, qviz = tt)

        QgsApplication.taskManager().addTask(task1)        

    def updateBatch(self, frame_number):
        self.current_batch = self.future_batch
        pass

    def fetchNextBatch(self, frame_number):
        # Has to create a new Qgis task 
        #self.future_batch = Qgis task
        pass

    def setNextBatch(self, batch):
        self.setNextBatch = batch

    def getPoints(self, frame_number):
        key =  self.timestamps_strings[frame_number]
        # iterate over the output_data which is a dictionnary
        features= self.current_batch[key] 
        self.qgis_fields_list = []
        
        datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
        
        for i in range(len(features)):
            if len(features[i]) > 0:
                feat = QgsFeature(self.vlayer.fields())   # Create feature
                feat.setAttributes([datetime_obj])  # Set its attributes
                x,y = features[i]
                geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                feat.setGeometry(geom) # Set its geometry
                self.qgis_fields_list.append(feat)

        print(f"Added {len(features)} features to timestamp {key}")
        return self.qgis_fields_list

class qviz:
    """
    Main class used to create the temporal view and visualize the trajectories of ships.
    """
    def __init__(self):
        self.createPointsLayer()
        self.canvas = iface.mapCanvas()
        self.temporalController = self.canvas.temporalController()

        frame_rate = 30
        self.temporalController.setFramesPerSecond(frame_rate)

        # Define the new start and end dates
        #new_start_date = QDateTime.fromString("2023-06-01T00:00:00", Qt.ISODate)
        #new_end_date = QDateTime.fromString("2023-06-01T23:59:59", Qt.ISODate)

        # Create a QgsDateTimeRange object with the new start and end dates
        #new_temporal_range = QgsDateTimeRange(new_start_date, new_end_date)

        # Emit the updateTemporalRange signal with the new temporal range
        #self.temporalController.updateTemporalRange.emit(new_temporal_range)

       

        self.data =  pyMeos_data()

        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        #self.generatePoints()
        #self.addPoints()
        self.on_new_frame_times = []
        self.removePoints_times = []
        self.update_features_times = []
        self.number_of_points_stored_in_layer = []


    def getFeatures(self):
        db  = mobDB()
        percentage = PERCENTAGE_OF_SHIPS    

        pymeos_initialize()
        mmsi_list = db.getMMSI(percentage)
        rows = db.getTrajectories(mmsi_list, "2023-06-01 00:00:00", "2023-06-01 05:00:00")

        features = {str(dt): [] for dt in self.timestamps}

        for mmsi in mmsi_list:
            for datetime in self.timestamps:
                try :
                    val = rows[mmsi].value_at_timestamp(datetime)
                    features[datetime.strftime('%Y-%m-%d %H:%M:%S')].append((val.x, val.y))
                except Exception as e: 
                    val = None
        return features


    def get_stats(self):
        pass

    
    def on_new_frame(self):
        """
        Function called every time temporal controller frame is changed. It is used to update the features displayed on the map.
        """

        curr_frame = self.temporalController.currentFrameNumber()

        if curr_frame % TIME_DELTA == 0:
            self.data.updateBatch(curr_frame) # removes previous batch and switches to the new one
            self.data.fetchNextBatch(curr_frame)            
            #self.features = self.getFeatures()

        self.removePoints() # Deletes all previous points
        self.addPoints(curr_frame)
        
    
    
    def createPointsLayer(self):
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
        #self.updateTimestamps()
        #self.features.update(self.timestamps)
        now= time.time()

        self.qgis_fields_list = self.data.getPoints(currentFrameNumber)
        
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






tt = qviz(False)