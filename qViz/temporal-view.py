
from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta


PERCENTAGE_OF_SHIPS = 0.1 # To not overload the memory, we only take a percentage of the ships in the database
FRAMES_FOR_30_FPS = 48 # Number of frames needed for a 30 FPS animation 


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
    

def fetchData():
    pass    


class featuresStore:
    """
    Creates Points from the trajectories of ships at different timestamps, is used to manage the features
    """

    def __init__(self, percentage=0.001, timestamps=[]):
        self.features = {}
        self.db  = mobDB()
        self.percentage = percentage
        pymeos_initialize()

        self.tm = QgsApplication.taskManager()

        task = MoveTTask(f"Move: Creating tgeom view {col}", query,
                                     self.project_title, self.db, col,
                                     self.add_tgeom_layer, self.raise_error)
        self.tm.addTask(task)
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



def log(msg):
    QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)



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



MESSAGE_CATEGORY = 'TaskFromFunction'


def doSomething(task, timestamps, qviz):

    """

    Raises an exception to abort the task.

    Returns a result if success.

    The result will be passed, together with the exception (None in

    the case of success), to the on_finished method.

    If there is an exception, there will be no result.

    """

    QgsMessageLog.logMessage('Started task {}'.format(task.description()),

                             MESSAGE_CATEGORY, Qgis.Info)

    db  = mobDB()
    percentage = PERCENTAGE_OF_SHIPS
    pymeos_initialize()


    mmsi_list = db.getMMSI(percentage)
    rows = db.getTrajectories(mmsi_list)

    # features = {Timestamp1 : [(x1,y1), (x2,y2), ...], Timestamp2 : [(x1,y1), (x2,y2), ...], ...}
    features = {str(dt): [] for dt in timestamps}

    for mmsi in mmsi_list:
        for datetime in timestamps:
            try :
                val = rows[mmsi].value_at_timestamp(datetime)
                features[datetime.strftime('%Y-%m-%d %H:%M:%S')].append((val.x, val.y))
            except Exception as e: 
                val = None
        # check task.isCanceled() to handle cancellation
        if task.isCanceled():

            stopped(task)

            return None

    qviz.setFeatures(features)  
    return {'features': features, 'task': task.description()}


def stopped(task):

    QgsMessageLog.logMessage(

        'Task "{name}" was canceled'.format(

            name=task.description()),

        MESSAGE_CATEGORY, Qgis.Info)


def completed(exception, result=None):

    """This is called when doSomething is finished.

    Exception is not None if doSomething raises an exception.

    result is the return value of doSomething."""

    if exception is None:

        if result is None:

            QgsMessageLog.logMessage(

                'Completed with no exception and no result '\

                '(probably manually canceled by the user)',

                MESSAGE_CATEGORY, Qgis.Warning)

        else:

            QgsMessageLog.logMessage(f"Task {result['task']} completed\nNumber of features: {len(result['features'])} )",

                MESSAGE_CATEGORY, Qgis.Info)

    else:

        QgsMessageLog.logMessage("Exception: {}".format(exception),

                                 MESSAGE_CATEGORY, Qgis.Critical)

        raise exception

start_date = datetime(2023, 6, 1, 0, 0, 0)
end_date = datetime(2023, 6, 1, 23, 59, 59)
time_delta = timedelta(minutes=1)
timestamps = [start_date + i * time_delta for i in range(steps)]
timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in timestamps]
# Create a few tasks


tt = qviz(False)
task1 = QgsTask.fromFunction('Fetch data', doSomething,

                             on_finished=completed, timestamps=timestamps)

QgsApplication.taskManager().addTask(task1)

task2 = QgsTask.fromFunction('Waste cpu 2', doSomething,

                             on_finished=completed, wait_time=3)



QgsApplication.taskManager().addTask(task2)



##################################################################################### 
###########################################TESTS#####################################
##################################################################################### 
class TestMobiDB:
    def __init__(self, host:str, port:int, db:str, user:str, password:str):
        beingTested = mobDB(host, port, db, user, password)
        mmsi_list=beingTested.getMMSI()
        assert len(mmsi_list) == 5
        assert len(beingTested.getTrajectories(mmsi_list)) == 5
        beingTested.close()
        print(f"No errors in {self.__class__.__name__}")

#TestMobiDB("localhost", 5432, "mobilitydb", "postgres", "postgres")

class TestFeaturesStore:
    def __init__(self):
        beingTested = featuresStore()
        beingTested.update([datetime.datetime(2023, 6, 1, 0, 0)])
        assert beingTested.getFeatures() == {'2023-06-01 00:00:00': [(12.3227, 56.1102)]}
        print(f"No errors in {self.__class__.__name__}")