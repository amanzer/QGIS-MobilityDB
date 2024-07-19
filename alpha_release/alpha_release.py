"""

Next implementation of the Move class:

extent = iface.mapCanvas().extent()
# Create a feature request with the extent filter
request = QgsFeatureRequest().setFilterRect(extent)
request.setLimit(500)
# Iterate over the features that intersect with the current extent
visible_features = [feature.id() for feature in vlayer.getFeatures(request)]

"""

from pymeos.db.psycopg import MobilityDB
from pymeos import *
import time






# SRID = 4326
# ########## AIS Danish maritime dataset ##########
# DATABASE_NAME = "mobilitydb"
# TPOINT_TABLE_NAME = "PyMEOS_demo_500"
# TPOINT_ID_COLUMN_NAME = "MMSI"
# TPOINT_COLUMN_NAME = "trajectory"


SRID = 3857
########## AIS Danish maritime dataset ##########
DATABASE_NAME = "stib"
TPOINT_TABLE_NAME = "trips_mdb"
TPOINT_ID_COLUMN_NAME = "trip_id"
TPOINT_COLUMN_NAME = "trip"



def log(msg):
    QgsMessageLog.logMessage(msg, 'Move', level=Qgis.Info)

class DatabaseController:
    """
    Singleton class to handle the mobilitydb connection.
    """
    def __init__(self, connection_parameters):
        try:
            self.connection_params = {
                "host": connection_parameters["host"],
                "port": connection_parameters["port"],
                "dbname": connection_parameters["dbname"],
                "user": connection_parameters["user"],
                "password": connection_parameters["password"],
                }
    

            self.table_name = connection_parameters["table_name"]
            self.id_column_name = connection_parameters["id_column_name"]
            self.tpoint_column_name = connection_parameters["tpoint_column_name"]     
            
        except Exception as e:
            log(f"Error in initiating Database Connector : {e}")

    def get_TgeomPoints(self):
        try:
            query = f"""
            SELECT {self.id_column_name}, {self.tpoint_column_name}, startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT 100000 ;
            """
            log(f"Query : {query}")
            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            res = self.cursor.fetchall()

            self.cursor.close()
            self.connection.close()

            return res
        except Exception as e:
            log(f"Error in fetching TgeomPoints : {e}")
            return None


   

    

class VectorLayerController:
    """
    Controller a in memory vector layer to view TgeomPoints.
    """
    def __init__(self, srid):
        self.vlayer = QgsVectorLayer(f"Point?crs=epsg:{srid}", "MobilityBD Data", "memory")
        # Define the fields
        fields = [
            QgsField("id", QVariant.String),
            QgsField("start_time", QVariant.DateTime),
            QgsField("end_time", QVariant.DateTime)
        ]
        self.vlayer.dataProvider().addAttributes(fields)
        self.vlayer.updateFields()
        # Define the temporal properties
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        tp.setMode(QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        tp.setStartField("start_time")
        tp.setEndField("end_time")

        self.vlayer.updateFields()
        QgsProject.instance().addMapLayer(self.vlayer)

    
    def get_vlayer_fields(self):
        """
        Get the fields of the vector layer.
        """
        if self.vlayer:
            return self.vlayer.fields()
        return None

    def delete_vlayer(self):
        """
        Delete the vector layer from the map.
        """
        pass

    def add_features(self, features_list):
        """
        Add features to the vector layer.
        """
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)






class fetch_data_thread(QgsTask):

    def __init__(self, description,project_title, database_connector, finished_fnc, failed_fnc):
        super(fetch_data_thread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector

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
        Runs the new process to create the matrix for the given time delta.
        """
        try:
            # start_time = self.start_date +  ( self.granularity_enum.value["timedelta"] * self.begin_frame) # TODO : 
            # end_time = self.start_date +  ( self.granularity_enum.value["timedelta"] * self.end_frame) # TODO :
            # rows = self.db.get_tgeompoints(start_time, end_time, self.extent, self.srid, self.n_objects)
            results = self.database_connector.get_TgeomPoints()

            tgeompoints = {} 
            for rows in results:
                tgeompoints[rows[0]] = rows[1:]

            
            self.result_params = {
                'TgeomPoints_list' : tgeompoints
            }
        except Exception as e:
            self.error_msg = str(e)
            return False
        return True




class MobilitydbLayerHandler:
    """
    Initializes and handles a layer to view MobilityDB data.
    """
    def __init__(self, iface, task_manager, srid, connection_parameters):
        self.iface = iface
        self.task_manager = task_manager
        self.vector_layer_controller = VectorLayerController(srid)
        self.database_controller = DatabaseController(connection_parameters)

        self.last_time_record = time.time()
        self.fetch_data_task = fetch_data_thread("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.on_fetch_data_finished, self.raise_error)
        self.task_manager.addTask(self.fetch_data_task)


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")

    def on_fetch_data_finished(self, result_params):
        """
        Callback function for the fetch data task.
        """
        try:
            self.TIME_fetch_tgeompoints = time.time() - self.last_time_record
            self.tgeompoints = result_params['TgeomPoints_list']
            
            vlayer_fields=  self.vector_layer_controller.get_vlayer_fields()

            features_list = []
            self.geometries = {}
            self.tpoints = {}
            index = 1
            for key, value in self.tgeompoints.items():
                if value[1] == None :
                    log(f"None value found for key : {key}")
                    continue
                feature = QgsFeature(vlayer_fields)
                feature.setAttributes([ key, QDateTime(value[1]), QDateTime(value[2])])
                geom = QgsGeometry()
                self.geometries[index] = geom
                feature.setGeometry(geom)
                features_list.append(feature)
                self.tpoints[index] = value[0]
                index += 1

            self.vector_layer_controller.add_features(features_list)
            self.objects_count = len(self.tgeompoints)
            log(f"Time taken to fetch TgeomPoints : {self.TIME_fetch_tgeompoints}")
            log(f"Number of TgeomPoints fetched : {self.objects_count}")
        except Exception as e:
            log(f"Error in on_fetch_data_finished : {e}")

    def new_frame(self, timestamp):
        """
        Update the layer to the new frame.
        """
        log(f"New Frame : {timestamp}")
        hits = 0
        for i in range(1, self.objects_count+1):
            # Fetching the position of the object at the current frame
            try:
                
                position = self.tpoints[i].value_at_timestamp(timestamp)
                # Updating the geometry of the feature in the vector layer
                self.geometries[i].fromWkb(position.wkb) # = QgsGeometry.fromWkt(position.wkt)
                hits+=1
            except:
                continue
        log(f"Number of hits : {hits}")
        self.vector_layer_controller.vlayer.startEditing()
        self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
        self.vector_layer_controller.vlayer.commitChanges()
        # self.iface.vectorLayerTools().stopEditing(self.vector_layer_controller.vlayer)
        # self.iface.mapCanvas().refresh() # TODO
        



class Move:
    def __init__(self):
        pymeos_initialize()
        self.iface= iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.temporal_controller.updateTemporalRange.connect(self.on_new_frame)
        self.frame = 0
        self.navigationMode = self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        # States for NavigationMode etc

        self.mobilitydb_layers= []
        self.execute()

    def execute(self):
        connection_parameters = {
                'host': "localhost",
                'port': 5432,
                'dbname': DATABASE_NAME,
                'user': "postgres",
                'password': "postgres",
                'table_name': TPOINT_TABLE_NAME,
                'id_column_name': TPOINT_ID_COLUMN_NAME,
                'tpoint_column_name': TPOINT_COLUMN_NAME,
            }

        self.mobilitydb_layers.append(MobilitydbLayerHandler(self.iface, self.task_manager, SRID, connection_parameters))


    def add_mobilitydb_layer(self, layer):
        self.mobilitydb_layers.append(layer)

    def on_new_frame(self):
        log("New Frame")
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()

        if current_frame == next_frame or current_frame == previous_frame:
            self.frame = current_frame
            for layer in self.mobilitydb_layers:
                layer.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
        else:
            self.frame = current_frame
            """
            Multiple scenarios :
            -Navigation Mode change
            -Date Range change 
            -Time granularity change
            -FPS change
            -Cumulative FPS change
            - Verify if other scenario also trigger this signal

            """
            pass

tt = Move()