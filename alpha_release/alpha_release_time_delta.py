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



LIMIT = 500
TIME_DELTA_SIZE = 30

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
            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            
        except Exception as e:
            log(f"Error in initiating Database Connector : {e}")

    def get_IDs_timestamps(self, limit):

        query_srid = f"""
        SELECT srid{self.tpoint_column_name} FROM public.{self.table_name} LIMIT 1 ;
        """
        self.cursor.execute(query_srid)
        srid = self.cursor.fetchall()[0][0]

        query = f"""
        SELECT {self.id_column_name}, startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT {limit} ;
        """
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        return res, srid


    def get_TgeomPoints(self, start_ts, end_ts, limit):
        
        try:

            query = f"""
            SELECT attime(a.{self.tpoint_column_name}::tgeompoint,span('{start_ts}'::timestamptz, '{end_ts}'::timestamptz, true, true))
            FROM public.{self.table_name} AS a  LIMIT {limit};
                    """
            # query = f"""
            # SELECT {self.id_column_name}, {self.tpoint_column_name}, startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT 100000 ;
            # """
            log(f"Query : {query}")
            self.cursor.execute(query)
            res = self.cursor.fetchall()

            self.cursor.close()
            self.connection.close()

            return res
        except Exception as e:
            log(f"Error in fetching TgeomPoints : {e}")
            return None

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
   

    

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





class initialize_qgis_features(QgsTask):
    def __init__(self, description,project_title, database_connector, finished_fnc, failed_fnc):
        super(initialize_qgis_features, self).__init__(description, QgsTask.CanCancel)
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
            results, srid  = self.database_connector.get_IDs_timestamps(LIMIT)

            
            self.result_params = {
                'Ids_timestamps' : results,
                'srid' : srid
            }
        except Exception as e:
            self.error_msg = str(e)
            return False
        return True


class fetch_time_delta_thread(QgsTask):

    def __init__(self, description,project_title, database_connector, begin_timestamp, end_timestamp, finished_fnc, failed_fnc):
        super(fetch_time_delta_thread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.begin_timestamp = begin_timestamp
        self.end_timestamp = end_timestamp


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
            tgeompoints = self.database_connector.get_TgeomPoints(self.begin_timestamp, self.end_timestamp, LIMIT)
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
        self.database_controller = DatabaseController(connection_parameters)
        
        self.last_time_record = time.time()
        task = initialize_qgis_features("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.create_vector_layer, self.raise_error)
        self.task_manager.addTask(task)


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")


    def start_animation(self, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2):
        self.next_begin =  begin_ts_2
        self.next_end = end_ts_2
        self.last_time_record = time.time()
        task = fetch_time_delta_thread("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, begin_ts_1, end_ts_1, self.initiate_animation, self.raise_error)
        self.task_manager.addTask(task)
    
    def initiate_animation(self, result_params):
        self.TIME_fetch_time_delta = time.time() - self.last_time_record
        self.current_tpoints = result_params['TgeomPoints_list']
        self.last_time_record
        task = fetch_time_delta_thread("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.next_begin, self.next_end, self.on_fetch_time_data_finished, self.raise_error)
        self.task_manager.addTask(task)

    def fetch_time_delta(self, begin_ts, end_ts):
        self.last_time_record = time.time()
        task = fetch_time_delta_thread("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, begin_ts, end_ts, self.on_fetch_time_delta_finished, self.raise_error)
        self.task_manager.addTask(task)

    def switch_time_delta(self, dir):
        if dir == 0 :
            self.previous_tpoints = self.current_tpoints
            self.current_tpoints = self.next_tpoints
            self.next_tpoints = None
        else: # Direction changed
            self.previous_tpoints = self.current_tpoints
            self.current_tpoints = self.previous_tpoints
            self.previous_tpoints = None

    def create_vector_layer(self, result_params):
        self.TIME_get_ids_timestamps = time.time() - self.last_time_record
        log(f"Time taken to fetch TgeomPoints : {self.TIME_get_ids_timestamps}")
        log(f"Number of TgeomPoints fetched : {self.objects_count}")
        srid = result_params['srid']
        ids_timestamps = result_params['Ids_timestamps']
        self.vector_layer_controller = VectorLayerController(srid)

         
        vlayer_fields=  self.vector_layer_controller.get_vlayer_fields()

        features_list = []
        self.geometries = {}

        for i in range(1, len(ids_timestamps)+1):
            feature = QgsFeature(vlayer_fields)
            feature.setAttributes([ ids_timestamps[i-1][0], QDateTime(ids_timestamps[i-1][1]), QDateTime(ids_timestamps[i-1][2])])
            geom = QgsGeometry()
            self.geometries[i] = geom
            feature.setGeometry(geom)
            features_list.append(feature)
            
        self.vector_layer_controller.add_features(features_list)
        
    #     self.initiate_animation()

    # def initiate_animation(self):
    #     """
    #     fetch first time delta, and start the fetch for the next time delta
    #     """
    #     begin_frame = 0
    #     end_frame = self.time_delta_size -1

    #     self.last_time_record = time.time()
    #     task = fetch_time_delta_thread("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.on_fetch_data_finished, self.raise_error)
    #     self.task_manager.addTask(task)



    def on_fetch_time_data_finished(self, result_params):
        """
        Callback function for the fetch data task.
        """
        try:
            self.TIME_fetch_time_delta = time.time() - self.last_time_record
            self.tpoints = result_params['TgeomPoints_list']
            
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
        self.time_delta_size = TIME_DELTA_SIZE 
        # self.mobilitydb_layers= []
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

        self.mobilitydb_layer_handler = MobilitydbLayerHandler(self.iface, self.task_manager, SRID, connection_parameters)
        self.launch_animation()


    def launch_animation(self):
        self.current_time_delta_key = 0
        self.current_time_delta_end = self.time_delta_size - 1

        self.total_frames = self.temporal_controller.totalFrameCount()
        self.tdelta_change = False
        
        begin_frame = self.current_time_delta_key
        end_frame = self.current_time_delta_end 
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame + self.time_delta_size).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame + self.time_delta_size).begin().toPyDateTime()
        
        self.mobilitydb_layer_handler.start_animation(begin_ts_1, end_ts_1, begin_ts_2, end_ts_2)




    def update_layers(self, current_frame):
        self.mobilitydb_layer_handler.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())


    def fetch_next_time_deltas(self, begin_frame, end_frame):
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        self.mobilitydb_layer_handler.fetch_time_delta(begin_ts, end_ts)



    def on_new_frame(self):
        log("New Frame")
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()

        if current_frame == next_frame or current_frame == previous_frame:
            self.frame = current_frame
            
            if self.frame % self.time_delta_size == 0:
                if self.frame == next_frame:
                    if self.current_time_delta_end + 1 != self.total_frames:
                        self.current_time_delta_key = self.frame
                        self.current_time_delta_end = self.frame + self.time_delta_size - 1
                        self.mobilitydb_layer_handler.shift_time_deltas(0)

                        if self.task_manager.countActiveTasks() != 0:
                            self.temporal_controller.pause()

                        self.fetch_next_time_deltas()
                        self.update_layers(self.frame)
                        self.tdelta_change = True
                else: 
                    self.update_layers(self.frame)
                    if self.current_time_delta_key != 0:
                        self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                        self.current_time_delta_end = self.frame - 1

                        self.mobilitydb_layer_handler.shift_time_deltas(1)
                        if self.task_manager.countActiveTasks() != 0:
                            self.temporal_controller.pause()

                        self.fetch_next_time_deltas()
                        self.tdelta_change = True
            else:
                if self.tdelta_change:
                    if self.frame < self.current_time_delta_key:
                        self.current_time_delta_key = self.current_time_delta_key - self.time_delta_size
                        self.current_time_delta_end = self.frame
                        self.tdelta_change = False
                self.update_layers(self.frame)
                self.tdelta_change = False

            
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