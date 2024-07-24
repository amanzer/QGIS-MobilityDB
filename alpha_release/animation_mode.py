"""

Animated mode : Time delta solution

(frame tick = the slide switch to control the visible frame on the temporal controller)

To improve stability : Removed the capability to manually move the frame tick(forces a time delta reload) and play in REVERSE
Current version depends on a reload button that user has to click after changing the configuration of the temporal controller

These limitations are applied to create a stable functionning version :
- Resolves the issues related to unpredictable temporal controller signals due to user interaction(harder to debug and keep track of the time deltas state)
- Qgis Task manager is difficult to manage in this context, as user can trigger multiple time deltas search calls by playing with the frame tick or 
    by going backward/forward quickly.
- The concept of time deltas is based around the frames, if a user changes the time steps or the time unit of the temporal controller, the time deltas
    will no longer represent the same time range. The reload button is a way to reset the time deltas to the new configuration of the temporal controller.
    This is also done via a button to give the user time to think about the changes he made to the temporal controller(instant reload on signal is frustrating).
    

"""

from pymeos.db.psycopg import MobilityDB
from pymeos import *
import time
from shapely.geometry import Point


LIMIT = 600000
TIME_DELTA_SIZE = 30


# # ########## AIS Danish maritime dataset ##########
# DATABASE_NAME = "mobilitydb"
# TPOINT_TABLE_NAME = "PyMEOS_demo"
# TPOINT_ID_COLUMN_NAME = "MMSI"
# TPOINT_COLUMN_NAME = "trajectory"


######## AIS Danish maritime dataset ##########
DATABASE_NAME = "stib"
TPOINT_TABLE_NAME = "trips_mdb"
TPOINT_ID_COLUMN_NAME = "trip_id"
TPOINT_COLUMN_NAME = "trip"


class Waiting_tdelta:
    def __init__(self):
        self.waiting = False
    
    def set_waiting(self, status):
        self.waiting = status

    def is_waiting(self):
        return self.waiting

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

    def get_IDs_timestamps(self, limit):
        """
        Fetch the IDs and the start/end timestamps of the tgeompoints.
        """
        query_srid = ""
        query = ""
        try: 
            connection = MobilityDB.connect(**self.connection_params)
            cursor = connection.cursor()
            query_srid = f"""
            SELECT srid({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT 1 ;
            """
            cursor.execute(query_srid)
            srid = cursor.fetchall()[0][0]

            query = f"""
            SELECT {self.id_column_name}, startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT {limit} ;
            """
            
            cursor.execute(query)
            
            results= []
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)
            # results = cursor.fetchall()

            cursor.close()
            connection.close()

            return results, srid
        except Exception as e:
            log(f"Error in fetching IDs and timestamps : {e} \n query_srid : {query_srid} \n query : {query}")
            return None


    def get_TgeomPoints(self, start_ts, end_ts, limit):
        """
        Fetch the TgeomPoints for the given time range.
        """
        try:
            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()

            query = f"""
            SELECT attime(a.{self.tpoint_column_name}::tgeompoint,span('{start_ts}'::timestamptz, '{end_ts}'::timestamptz, true, true))
            FROM public.{self.table_name} AS a  LIMIT {limit};
                    """
            # query = f"""
            # SELECT {self.id_column_name}, {self.tpoint_column_name}, startTimestamp({self.tpoint_column_name}), endTimestamp({self.tpoint_column_name}) FROM public.{self.table_name} LIMIT 100000 ;
            # """
            # log(f"Query : {query}")
            self.cursor.execute(query)
            results= []
            while True:
                rows = self.cursor.fetchmany(1000)
                if not rows:
                    break
                results.extend(rows)

            self.cursor.close()
            self.connection.close()
            return results
        except Exception as e:
            log(f"Error in fetching time delta TgeomPoints : {e} \n query : {query}")
            return None


    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.
        """
        try:
            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            res= self.cursor.fetchone()[0]
            self.cursor.close()
            self.connection.close()
            return res
        except Exception as e:
            log(f"Error in fetching min timestamp : {e} \n query : {query}")


    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.connection = MobilityDB.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            query = f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            res = self.cursor.fetchone()[0]
            self.cursor.close()
            self.connection.close()
            return res
        except Exception as e:
            log(f"Error in fetching max timestamp : {e} \n query : {query}")
   
    # def __del__(self):
    #     self.cursor.close()
    #     self.connection.close()

    

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



    def add_features(self, features_list):
        """
        Add features to the vector layer.
        """
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)


    def __del__(self):
        if self.vlayer:
            QgsProject.instance().removeMapLayer(self.vlayer)
            self.vlayer = None




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
            log(f"Error in fetching IDs and timestamps : {e}")
            self.error_msg = str(e)
            return False
        return True


class fetch_time_delta_thread(QgsTask):

    def __init__(self, description,project_title, database_connector, begin_timestamp, end_timestamp, limit, id, finished_fnc, failed_fnc):
        super(fetch_time_delta_thread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
    
        self.begin_timestamp = begin_timestamp
        self.end_timestamp = end_timestamp
        self.limit = limit
        self.id = id

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
            tgeompoints = self.database_connector.get_TgeomPoints(self.begin_timestamp, self.end_timestamp, self.limit)
            self.result_params = {
                'id' : self.id,
                'TgeomPoints_list' :  tgeompoints
            }
        except Exception as e:
            log(f"Error in fetching time delta TgeomPoints : {e}")
            self.error_msg = str(e)
            return False
        return True




class MobilitydbLayerHandler:
    """
    Initializes and handles a layer to view MobilityDB data.
    """
    def __init__(self, iface, task_manager, database_controller, limit, waiting_tdelta, start_ts, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2):
        self.iface = iface
        self.task_manager = task_manager
        self.database_controller = database_controller
        self.limit = limit
        self.waiting_tdelta= waiting_tdelta
        self.waiting_tdelta.set_waiting(True)

        self.vlayer_created = False
        self.previous_tpoints = None
        self.current_tpoints = None
        self.next_tpoints = None
        
        self.geometries = {}
        self.id=0
        self.start_ts = start_ts

        self.begin_ts = begin_ts_1
        self.end_ts = end_ts_1
        self.next_begin_ts = begin_ts_2
        self.next_end_ts = end_ts_2

        self.last_time_record = time.time()
        task = initialize_qgis_features("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.create_vector_layer, self.raise_error)
        self.task_manager.addTask(task)
 

    def reset_layer(self):
        self.current_tpoints = None
        self.next_tpoints = None
        self.previous_tpoints = None
        self.waiting_tdelta.set_waiting(False)   

        if len(self.geometries) > 0:
            empty_geom = Point().wkb
            for i in range(1, self.objects_count+1):
                self.geometries[i].fromWkb(empty_geom)
            
            self.vector_layer_controller.vlayer.startEditing()
            self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
            self.vector_layer_controller.vlayer.commitChanges()



    def reload_animation(self, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2):
        self.current_tpoints = None
        self.next_tpoints = None
        self.next_begin_ts = begin_ts_2
        self.next_end_ts = end_ts_2

        self.last_time_record = time.time()
        self.task = fetch_time_delta_thread(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, begin_ts_1, end_ts_1, self.limit, self.id, self.fetch_second_time_delta, self.raise_error)
        self.task_manager.addTask(self.task)

    def fetch_second_time_delta(self, result_params):
        try:
            self.TIME_time_delta_fetch = time.time() - self.last_time_record
            log(f"Time taken to fetch time delta TgeomPoints : {self.TIME_time_delta_fetch}")


            self.current_tpoints = result_params['TgeomPoints_list']
            self.new_frame(self.start_ts)            
            # iface.messageBar().pushMessage("Info", "Upcoming TgeomPoints have been loaded", level=Qgis.Info)
            self.fetch_time_delta(self.next_begin_ts, self.next_end_ts)
            self.waiting_tdelta.set_waiting(False)
            iface.messageBar().pushMessage("Info", "Vector layer created, first time delta loaded, animation can play", level=Qgis.Info)
            log("Vector layer created, first time delta loaded, animation can play")
        except Exception as e:
            log(f"Error in fetch_second_time_delta : {e}")


    def fetch_time_delta(self, begin_ts, end_ts):
        """
        Fetch the TgeomPoints for the given time delta.
        """
        self.last_time_record = time.time()
        self.task = fetch_time_delta_thread(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, begin_ts, end_ts, self.limit, self.id, self.on_fetch_time_data_finished, self.raise_error)
        self.task_manager.addTask(self.task)


    def on_fetch_time_data_finished(self, result_params):
        """
        Callback function for the fetch data task.
        """
        try:
            self.TIME_fetch_time_delta = time.time() - self.last_time_record
            if self.id == result_params['id']:
                log(f"Time taken to fetch time delta TgeomPoints : {self.TIME_fetch_time_delta}")

                
                if self.waiting_tdelta.is_waiting():
                    iface.messageBar().pushMessage("Info", "Data loaded, restarting animation", level=Qgis.Info)
                    self.waiting_tdelta.set_waiting(False)
                    self.previous_tpoints = self.current_tpoints
                    self.current_tpoints = result_params['TgeomPoints_list']
                    self.next_tpoints = None
                    self.temporal_controller.playForward()
                else:
                    log("Next time delta is ready")
                    self.next_tpoints = result_params['TgeomPoints_list']
            else:
                log("Thread from previous configuration terminated")
            
        except Exception as e:
            log(f"Error in on_fetch_data_finished : {e}")

    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")




    


    def switch_time_delta(self):
        if not self.waiting_tdelta.is_waiting():
            self.previous_tpoints = self.current_tpoints
            self.current_tpoints = self.next_tpoints
            self.next_tpoints = None
        else:
            log("Waiting for next time delta to load")



    def create_vector_layer(self, result_params):
        self.TIME_get_ids_timestamps = time.time() - self.last_time_record
        log(f"Time taken to fetch Ids and start/end timestamps: {self.TIME_get_ids_timestamps}")
        ids_timestamps = result_params['Ids_timestamps']
        
        self.objects_count = len(ids_timestamps)
        srid = result_params['srid']
        log(f"Number of TgeomPoints fetched : {self.objects_count} | SRID : {srid}")
        
        self.vector_layer_controller = VectorLayerController(srid)

         
        vlayer_fields=  self.vector_layer_controller.get_vlayer_fields()

        features_list = []
        self.geometries = {}

        for i in range(1, self.objects_count + 1):
            feature = QgsFeature(vlayer_fields)
            feature.setAttributes([ ids_timestamps[i-1][0], QDateTime(ids_timestamps[i-1][1]), QDateTime(ids_timestamps[i-1][2])])
            geom = QgsGeometry()
            self.geometries[i] = geom
            feature.setGeometry(geom)
            features_list.append(feature)
            
        self.vector_layer_controller.add_features(features_list)
        self.vlayer_created = True

        self.last_time_record = time.time()
        self.task = fetch_time_delta_thread(f"Fetching Time Delta", "Move - MobilityDB", self.database_controller, self.begin_ts, self.end_ts, self.limit, self.id, self.fetch_second_time_delta, self.raise_error)
        self.task_manager.addTask(self.task)

    


    def new_frame(self, timestamp):
        """
        Update the layer to the new frame.
        """
        log(f"New Frame : {timestamp}")
        try:
            if self.current_tpoints:
                log(f"New Frame : {timestamp}")
                hits = 0
                
                empty_geom = Point().wkb
                for i in range(1, self.objects_count+1):
                    # Fetching the position of the object at the current frame
                    try:
                        position = self.current_tpoints[i-1][0].value_at_timestamp(timestamp)
                        # Updating the geometry of the feature in the vector layer
                        self.geometries[i].fromWkb(position.wkb) # = QgsGeometry.fromWkt(position.wkt)
                        hits+=1
                    except:
                        self.geometries[i].fromWkb(empty_geom)

                log(f"Number of hits : {hits}")
                self.vector_layer_controller.vlayer.startEditing()
                self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
                self.vector_layer_controller.vlayer.commitChanges()
                self.iface.vectorLayerTools().stopEditing(self.vector_layer_controller.vlayer)
                self.iface.mapCanvas().refresh() # TODO
            else:
                log("No TgeomPoints loaded yet")
        except Exception as e:
            log(f"Error in new_frame: {e} \ Make sure the current time delta exists")




class Move:
    def __init__(self):
        pymeos_initialize()
        self.iface= iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.waiting_tdelta = Waiting_tdelta()

        # Attributes 
        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0
        self.is_move_disabled = False

        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.temporal_controller.updateTemporalRange.connect(self.on_new_frame)

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
        
        self.database_connector = DatabaseController(connection_parameters)
      
        time_range = QgsDateTimeRange(self.database_connector.get_min_timestamp(), self.database_connector.get_max_timestamp())
        start_ts = self.temporal_controller.dateTimeRangeForFrameNumber(0).begin().toPyDateTime()
        self.temporal_controller.setTemporalExtents(time_range)

        self.key = 0
        
        begin_frame = self.key*self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        self.next_key = self.key + 1

        self.mobilitydb_layer_handler = MobilitydbLayerHandler(self.iface, self.task_manager, self.database_connector, LIMIT, self.waiting_tdelta, start_ts, begin_ts_1, end_ts_1, begin_ts_2, end_ts_2)

        # log("Creating animation for the following settings : \n")
        # log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
        # log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
        # log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
        # log(f"Frame duration : {self.temporal_controller.frameDuration()}")
        # log(f"isLooping : {self.temporal_controller.isLooping()}")
        # log(f"FPS : {self.temporal_controller.framesPerSecond()}")
        # log(f"Cumulative range : {self.temporal_controller.temporalRangeCumulative()}")

        # log(f"\n Current Frame : {self.temporal_controller.currentFrameNumber()}")

        log("Executing")
        
        # self.launch_animation()

        # log(f"Animation state : {self.temporal_controller.animationState()}")

        


    def launch_animation(self, key=0):
        self.key = key
        
        begin_frame = self.key*self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        self.next_key = self.key + 1
        # self.mobilitydb_layer_handler.start_animation(begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2)
        self.mobilitydb_layer_handler.reload_animation(begin_ts_1, end_ts_1, begin_ts_2, end_ts_2)
        log(f"Launching animation, first two time deltas \n First time delta : {begin_frame} to {end_frame} | {begin_ts_1} to {end_ts_1}  \n Second time delta: {self.begin_frame} to {self.end_frame} | {begin_ts_2} to {end_ts_2} ")



    def update_layers(self, current_frame):
        try:
            self.mobilitydb_layer_handler.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
            # log(f"Update geometries for frame : {current_frame} with key {self.key}")

            fps = 1 / (time.time() - self.onf_time)
            self.temporal_controller.setFramesPerSecond(fps)
            log(f"FPS : {fps}")
        except Exception as e:
            log(f"Error in updating layers : {e}")

    def switch_time_deltas(self):
        self.mobilitydb_layer_handler.switch_time_delta()
        # log(f"Switching time deltas : {self.key} ")
      

    def fetch_next_time_deltas(self, begin_frame, end_frame):
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        self.mobilitydb_layer_handler.fetch_time_delta(begin_ts, end_ts)
        # log(f"Fetching Next Time Deltas : {begin_frame} to {end_frame} | {begin_ts} to {end_ts}")



    def on_new_frame(self):
        self.onf_time = time.time()
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()
        is_forward = (current_frame == next_frame)
        is_backward = (current_frame == previous_frame)

        log(f"$$New signal variables :\nCurrent Frame : {current_frame} | Next Frame : {next_frame} | Previous Frame : {previous_frame} | Forward : {is_forward} | Backward : {is_backward} ")
        
        if is_forward:
            log("Forward signal")
            if not self.waiting_tdelta.is_waiting():
                self.frame = current_frame
                key = self.frame // self.time_delta_size

                if key == self.next_key and ((self.end_frame + self.time_delta_size) < self.total_frames): # Fetch Next time delta
                    if self.task_manager.countActiveTasks() != 0:
                        iface.messageBar().pushMessage("Info", "Animation paused, waiting for next time delta to load", level=Qgis.Info)
                        self.waiting_tdelta.set_waiting(True)
                        self.temporal_controller.pause()
                    
                    self.key = key
                    self.next_key = self.key + 1
                    self.switch_time_deltas()                        
                    self.begin_frame = self.begin_frame + self.time_delta_size
                    self.end_frame = self.end_frame + self.time_delta_size
                    self.fetch_next_time_deltas(self.begin_frame, self.end_frame)
                    log(f"next time delta : {self.begin_frame} to {self.end_frame} | self.key : {self.key} ")
                self.update_layers(self.frame)
            else:
                log("Waiting for next time delta to load")
                self.temporal_controller.pause()
                self.temporal_controller.setCurrentFrameNumber(self.frame)
        elif is_backward:
            log("Backward navigation only in the same time delta")
            key = current_frame // self.time_delta_size
            if key == self.key:
                self.frame = current_frame
                self.update_layers(self.frame)
            else:
                log("Different time delta")
                self.temporal_controller.pause()
                self.temporal_controller.setCurrentFrameNumber(self.frame)
        else:    
            """
            Multiple scenarios :
            -Navigation Mode change
            -Date Range change 
            -Time granularity change
            -FPS change
            -Cumulative FPS change
            - Verify if other scenario also trigger this signal

            """ 
            # self.frame = current_frame
            is_configuration_changed = False
            self.temporal_controller.pause()
            self.temporal_controller.setCurrentFrameNumber(self.frame)

            # log("\n\n#### Signal is not for frame change ####\n\n")
            # log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
            # log(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
            # log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
            # log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
            # log(f"isLooping : {self.temporal_controller.isLooping()}")
            # log(f"FPS : {self.temporal_controller.framesPerSecond()}")
            # log(f"Frame duration : {self.temporal_controller.frameDuration()}")
            # log(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
            # log(f"Animation state : {self.temporal_controller.animationState()}")
    
            # log(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

            if self.temporal_controller.navigationMode() != self.navigationMode: # Navigation Mode change 
                is_configuration_changed = True
                if self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Animated:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    log("Navigation Mode Animated")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Disabled:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Disabled
                    log("Navigation Mode Disabled")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.Movie:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Movie
                    log("Navigation Mode Movie")
                elif self.temporal_controller.navigationMode() == QgsTemporalNavigationObject.NavigationMode.FixedRange:
                    self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated
                    log("Navigation Mode FixedRange")

            elif self.temporal_controller.frameDuration() != self.frameDuration: # Frame duration change 
                is_configuration_changed = True
                log(f"Frame duration has changed from {self.frameDuration} with {self.total_frames} frames")
                self.frameDuration = self.temporal_controller.frameDuration()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.frameDuration} with {self.total_frames} frames")

            
            elif self.temporal_controller.temporalExtents() != self.temporalExtents:
                is_configuration_changed = True
                log(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
                self.temporalExtents = self.temporal_controller.temporalExtents()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.temporalExtents} with {self.total_frames} frames")   

            elif self.temporal_controller.temporalRangeCumulative() != self.cumulative_range:
                is_configuration_changed = True
                log(f"cumulative range has changed from {self.cumulative_range}")
                self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
                log(f"to {self.cumulative_range} with {self.total_frames} frames")
            
            if not is_configuration_changed:
                """
                Timeline has been skipped or other signal
                if the current frame is in the same time delta, we set the current frame back to self.frame, otherwise :
                1. Press load button again to reload the time deltas
                2. We set the current_frame back to self.frame

                """

                log("Timeline skipped or other signal")
                
                key = current_frame // self.time_delta_size
                if key == self.key:
                    log("Same time delta")
                    self.frame = current_frame
                    self.update_layers(self.frame)
                else:
                    log("Different time delta")
                    self.temporal_controller.pause()
                    self.temporal_controller.setCurrentFrameNumber(self.frame)
            else:
                log("Configuration changed, Press the load button to reload the animation")

                # self.reload_button()


    def reload_button(self):
        # log("Changing internal configuration of temporal controller to match the new configuration \n")
        # log("Restating the animation")
        self.mobilitydb_layer_handler.id+=1

        self.frame = 0
        self.key = 0
        self.next_key = 0
        self.begin_frame = 0
        self.end_frame = 0

        self.mobilitydb_layer_handler.reset_layer()
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.cumulative_range = self.temporal_controller.temporalRangeCumulative()
        self.total_frames = self.temporal_controller.totalFrameCount()
        # self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        # self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.temporal_controller.setCurrentFrameNumber(0)
        self.temporal_controller.pause()
        # if self.mobilitydb_layer_handler.vlayer_created:
        #     self.task_manager.cancelAll()
        
        self.launch_animation(self.key)
        
tt = Move()