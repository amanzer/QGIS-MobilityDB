"""

1. MobDB : Singleton used to connect to the MobilityDB database
2. FeaturesStore : ?
3. Qviz : Controls the temporal controller


"""

from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta




PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
FRAMES_FOR_30_FPS = 48 # Number of frames needed for a 30 FPS animation 



def log(msg):
    QgsMessageLog.logMessage(msg, 'qVIZ', level=Qgis.Info)


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

    def getTrajectories(self, mmsi_list):
        try:
            rows={}
            for mmsi in mmsi_list:
                ship_mmsi = mmsi[0]
                self.cursor.execute(f"SELECT * FROM public.PyMEOS_demo WHERE MMSI = {ship_mmsi} ;")
                _, trajectory, sog = self.cursor.fetchone()
                rows[mmsi] = trajectory

            return rows
        except Exception as e:
            print(e)


    def close(self):
        self.cursor.close()
        self.connection.close()
    


class featuresStore:
    """
    Creates Points from the trajectories of ships at different timestamps, is used to manage the features
    """

    def __init__(self, percentage=0.001, timestamps=[]):
        self.features = {}
        self.db  = mobDB()
        self.percentage = percentage
        pymeos_initialize()

        self.update(timestamps)



    def update(self, timestamps):
        self.timestamps = timestamps 
        self.features = {dt: [] for dt in timestamps}
        
        mmsi_list = self.db.getMMSI(self.percentage)
        self.rows = self.db.getTrajectories(mmsi_list)

        # features = {Timestamp1 : [(x1,y1), (x2,y2), ...], Timestamp2 : [(x1,y1), (x2,y2), ...], ...}
        self.features = {str(dt): [] for dt in self.timestamps}

        for mmsi in mmsi_list:
            for datetime in self.timestamps:
                try :
                    val = self.rows[mmsi].value_at_timestamp(datetime)
                    self.features[datetime.strftime('%Y-%m-%d %H:%M:%S')].append((val.x, val.y))
                except Exception as e: 
                    val = None
        
    


    def getFeatures(self):
        return self.features



class qviz:
    """
    Main class used to create the temporal view and visualize the trajectories of ships.
    """
    def __init__(self, features, map:bool):
        if map :
            self.createMapsLayer()
        self.createPointsLayer()
        self.canvas = iface.mapCanvas()
        self.temporalController = self.canvas.temporalController()

        

        #frame_rate = 30
        #self.temporalController.setFramesPerSecond(frame_rate)

        # Define the new start and end dates
        #new_start_date = QDateTime.fromString("2023-06-01T00:00:00", Qt.ISODate)
        #new_end_date = QDateTime.fromString("2023-06-01T23:59:59", Qt.ISODate)

        # Create a QgsDateTimeRange object with the new start and end dates
        #new_temporal_range = QgsDateTimeRange(new_start_date, new_end_date)

        # Emit the updateTemporalRange signal with the new temporal range
        #self.temporalController.updateTemporalRange.emit(new_temporal_range)

        self.steps = 1440

        start_date = datetime(2023, 6, 1, 0, 0, 0)
        end_date = datetime(2023, 6, 1, 23, 59, 59)
        time_delta = timedelta(minutes=1)
        self.timestamps = [start_date + i * time_delta for i in range(steps)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in timestamps]
                


        self.features = {}

        self.temporalController.updateTemporalRange.connect(self.layer_points)
        #self.generatePoints()
        #self.addPoints()
        
    def setFeatures(self, features):
        self.features = features


    def next_frames_points(self, timestamps):

        return {key: self.features[key] for key in timestamps if key in self.features}
    
    def layer_points(self):
        """
        
        
        """
        curr_frame = self.temporalController.currentFrameNumber()
        if curr_frame % FRAMES_FOR_30_FPS == 0:
            self.removePoints()
            self.update_features(curr_frame)
            print("Added points for next 48 frames")


    def createMapsLayer(self):
        url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        map_layer = QgsRasterLayer(url, "OpenStreetMap", "wms")
        QgsProject.instance().addMapLayer(map_layer)
    
    
    def createPointsLayer(self):
        self.vlayer = QgsVectorLayer("Point", "points_3", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setStartField("time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)

    def updateTimestamps(self):
        self.timestamps = []
        #self.dtrange_ends = []
        currentFrameNumber = self.temporalController.currentFrameNumber()
        for i in range(self.steps):
            dtrange = self.temporalController.dateTimeRangeForFrameNumber(currentFrameNumber+i)
            self.timestamps.append(dtrange.begin().toPyDateTime().replace(tzinfo=dtrange.begin().toPyDateTime().tzinfo))
            #self.dtrange_ends.append(dtrange.end())

    def update_features(self, currentFrameNumber=0):
        #self.updateTimestamps()
        #self.features.update(self.timestamps)

        self.features_list =[]
      
        # iterate over the output_data which is a dictionnary
        features=  self.next_frames_points(self.timestamps_strings[currentFrameNumber:currentFrameNumber+FRAMES_FOR_30_FPS])
        for keys, items in features.items():
            datetime_obj = QDateTime.fromString(keys, "yyyy-MM-dd HH:mm:ss")
            
            for i in range(len(items)):
                if len(items[i]) > 0:
                    feat = QgsFeature(self.vlayer.fields())   # Create feature
                    feat.setAttributes([datetime_obj])  # Set its attributes
                    x,y = items[i]
                    geom = QgsGeometry.fromPointXY(QgsPointXY(x,y)) # Create geometry from valueAtTimestamp
                    feat.setGeometry(geom) # Set its geometry
                    self.features_list.append(feat)

        self.vlayer.startEditing()
        self.vlayer.addFeatures(self.features_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)


    def removePoints(self):
        self.vlayer.startEditing()
        delete_ids = [f.id() for f in self.vlayer.getFeatures()]
        self.vlayer.deleteFeatures(delete_ids)
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)

