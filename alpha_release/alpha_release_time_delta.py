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
from shapely.geometry import Point


LIMIT = 600000
TIME_DELTA_SIZE = 30


# ########## AIS Danish maritime dataset ##########
# DATABASE_NAME = "mobilitydb"
# TPOINT_TABLE_NAME = "PyMEOS_demo"
# TPOINT_ID_COLUMN_NAME = "MMSI"
# TPOINT_COLUMN_NAME = "trajectory"


######### AIS Danish maritime dataset ##########
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
            res = cursor.fetchall()

            cursor.close()
            connection.close()

            return res, srid
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
            res = self.cursor.fetchall()
            self.cursor.close()
            self.connection.close()
            return res
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

    def __init__(self, description,project_title, database_connector, key, begin_timestamp, end_timestamp, finished_fnc, failed_fnc):
        super(fetch_time_delta_thread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.database_connector = database_connector
        self.key = key
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
                'TgeomPoints_list' : (self.key, tgeompoints)
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
    def __init__(self, iface, task_manager, database_controller):
        self.iface = iface
        self.task_manager = task_manager
        self.database_controller = database_controller
        
        self.last_time_record = time.time()
        task = initialize_qgis_features("Fetching MobilityDB Data", "MobilityDB Data", self.database_controller, self.create_vector_layer, self.raise_error)
        self.task_manager.addTask(task)
        self.key_to_assign = None
        


    def cancel_tasks(self):
        try:
            if self.task:
                self.task.cancel()
        except Exception as e:
            log(f"Error in canceling task : {e}")

    def reload_animation(self, begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2):
        self.previous_tpoints = None
        self.current_tpoints = None
        self.next_tpoints = None
        self.next_begin_ts = begin_ts_2
        self.next_end_ts = end_ts_2
        self.key = key2 - 1
        self.next_key = key2
        self.last_time_record = time.time()
        self.task = fetch_time_delta_thread(f"Fetching Time Delta {self.key}", "Move - MobilityDB", self.database_controller, self.key, begin_ts_1, end_ts_1, self.fetch_second_time_delta, self.raise_error)
        self.task_manager.addTask(self.task)

    def fetch_second_time_delta(self, result_params):
        try:
            self.TIME_time_delta_fetch = time.time() - self.last_time_record
            log(f"Time taken to fetch time delta {self.key} TgeomPoints : {self.TIME_time_delta_fetch}")

            self.current_tpoints = result_params['TgeomPoints_list']
            iface.messageBar().pushMessage("Info", "TgeomPoints have been fetched, you can play the animation", level=Qgis.Info)
            self.fetch_time_delta(self.next_key, self.next_begin_ts, self.next_end_ts)

        except Exception as e:
            log(f"Error in fetch_second_time_delta : {e}")

    def on_fetch_time_data_finished(self, result_params):
        """
        Callback function for the fetch data task.
        """
        try:
            self.TIME_fetch_time_delta = time.time() - self.last_time_record

            tpoints = result_params['TgeomPoints_list']
            if self.key_to_assign is not None: 
                self.key_to_assign = None
                if self.key_to_assign == tpoints[0]:
                    self.previous_tpoints = self.current_tpoints
                    self.current_tpoints = tpoints
                    self.next_tpoints = None
                    log(f"Time taken to fetch time delta {tpoints[0]} TgeomPoints : {self.TIME_fetch_time_delta}")
                else:
                    # THIS SHOULD NOT HAPPEN THROW ERROR
                    raise(f"Error in switch_time_delta, given key {self.key_to_assign} does not match the next key {self.next_tpoints[0]}, error in temporal controller code, timeline was skipped")
                    pass
            else:
                self.next_tpoints = tpoints
                log(f"Time taken to fetch time delta {tpoints[0]} TgeomPoints : {self.TIME_fetch_time_delta}")
            
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




    def fetch_time_delta(self,key,  begin_ts, end_ts):
        """
        Fetch the TgeomPoints for the given time delta.
        """
        self.last_time_record = time.time()
        self.task = fetch_time_delta_thread(f"Fetching Time Delta {key}", "Move - MobilityDB", self.database_controller, key, begin_ts, end_ts, self.on_fetch_time_data_finished, self.raise_error)
        self.task_manager.addTask(self.task)

    def switch_time_delta(self, key):
        if self.next_tpoints is None:
            #wait for the current task to finish
            self.key_to_assign = key
        else:
            self.key_to_assign = None
            if key == self.next_tpoints[0]:
                self.previous_tpoints = self.current_tpoints
                self.current_tpoints = self.next_tpoints
                self.next_tpoints = None
                self.key = key
            else:
                # THIS SHOULD NOT HAPPEN THROW ERROR
                raise(f"Error in switch_time_delta, given key {key} does not match the next key {self.next_tpoints[0]}, error in temporal controller code, timeline was skipped")
                pass



    def create_vector_layer(self, result_params):
        self.TIME_get_ids_timestamps = time.time() - self.last_time_record
        log(f"Time taken to fetch TgeomPoints : {self.TIME_get_ids_timestamps}")
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
    


    def new_frame(self, timestamp):
        """
        Update the layer to the new frame.
        """
        try:
            log(f"New Frame : {timestamp}")
            hits = 0
            tpoints = self.current_tpoints[1]
            empty_geom = Point().wkb
            for i in range(1, self.objects_count+1):
                # Fetching the position of the object at the current frame
                try:
                    position = tpoints[i-1][0].value_at_timestamp(timestamp)
                    # Updating the geometry of the feature in the vector layer
                    self.geometries[i].fromWkb(position.wkb) # = QgsGeometry.fromWkt(position.wkt)
                    hits+=1
                except:
                    self.geometries[i].fromWkb(empty_geom)
            log(f"Number of hits : {hits}")
            self.vector_layer_controller.vlayer.startEditing()
            self.vector_layer_controller.vlayer.dataProvider().changeGeometryValues(self.geometries)
            self.vector_layer_controller.vlayer.commitChanges()
            # self.iface.vectorLayerTools().stopEditing(self.vector_layer_controller.vlayer)
            # self.iface.mapCanvas().refresh() # TODO
        except Exception as e:
            log(f"Error in new_frame: {e} \ Make sure the current time delta exists")



class Move:
    def __init__(self):
        pymeos_initialize()
        self.iface= iface
        self.task_manager = QgsTaskManager()
        self.canvas = self.iface.mapCanvas()
        self.temporal_controller = self.canvas.temporalController()
        self.frame = 0
        self.key = 0
        self.direction = True            

        self.navigationMode = QgsTemporalNavigationObject.NavigationMode.Animated 
        self.temporal_controller.setNavigationMode(QgsTemporalNavigationObject.NavigationMode.Animated)
        self.frameDuration = self.temporal_controller.frameDuration()
        self.temporalExtents = self.temporal_controller.temporalExtents()
        self.total_frames = self.temporal_controller.totalFrameCount()
        self.cumulativeRange = self.temporal_controller.temporalRangeCumulative()
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

        self.database_controller = DatabaseController(connection_parameters)
        time_range = QgsDateTimeRange(self.database_controller.get_min_timestamp(), self.database_controller.get_max_timestamp())
        self.temporal_controller.setTemporalExtents(time_range)
        self.mobilitydb_layer_handler = MobilitydbLayerHandler(self.iface, self.task_manager, self.database_controller)
        self.launch_animation()
        log(f"State of the temporal controller : ")
        log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
        log(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
        log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
        log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
        log(f"isLooping : {self.temporal_controller.isLooping()}")
        log(f"FPS : {self.temporal_controller.framesPerSecond()}")
        log(f"Frame duration : {self.temporal_controller.frameDuration()}")
        log(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
        log(f"Animation state : {self.temporal_controller.animationState()}")

        log(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

        log("Executing")


    def launch_animation(self, key=0):
        # self.task_manager.cancelAll() # Cancel all tasks and reload the time deltas

        self.key = key
        
        begin_frame = self.key*self.time_delta_size
        end_frame = (begin_frame + self.time_delta_size) - 1
        
        begin_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts_1 = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        
        self.begin_frame = begin_frame + self.time_delta_size
        self.end_frame = end_frame + self.time_delta_size

        begin_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.begin_frame).begin().toPyDateTime()
        end_ts_2 = self.temporal_controller.dateTimeRangeForFrameNumber(self.end_frame).begin().toPyDateTime()
        
        key2 = self.key + 1
        self.direction = True # Forward
        self.mobilitydb_layer_handler.reload_animation(begin_ts_1, end_ts_1, key2, begin_ts_2, end_ts_2)

        log(f" Launch animation \n First time delta : {begin_frame} to {end_frame} | {begin_ts_1} to {end_ts_1}  \n Second time delta: {self.begin_frame} to {self.end_frame} | {begin_ts_2} to {end_ts_2} ")



    def update_layers(self, current_frame):
        self.mobilitydb_layer_handler.new_frame( self.temporal_controller.dateTimeRangeForFrameNumber(current_frame).begin().toPyDateTime())
        log(f"Update geometries for frame : {current_frame} with key {self.key}")

    def switch_time_deltas(self):
        # self.mobilitydb_layer_handler.switch_time_delta()
        self.mobilitydb_layer_handler.switch_time_delta(self.key)
  
        log("Switch Time Deltas(if current_key does not exist, load 2 time deltas : current and future in the current direction)")

    def fetch_next_time_deltas(self,key, begin_frame, end_frame):
        begin_ts = self.temporal_controller.dateTimeRangeForFrameNumber(begin_frame).begin().toPyDateTime()
        end_ts = self.temporal_controller.dateTimeRangeForFrameNumber(end_frame).begin().toPyDateTime()
        self.mobilitydb_layer_handler.fetch_time_delta(key, begin_ts, end_ts)
        log(f"Fetching Next Time Deltas : {begin_frame} to {end_frame} | {begin_ts} to {end_ts}")



    def on_new_frame(self):
        log(f"$$start")
        recommended_fps_time = time.time()
        # Verify which signal is emitted
        next_frame= self.frame + 1
        previous_frame= self.frame - 1

        current_frame = self.temporal_controller.currentFrameNumber()
        is_forward = (current_frame == next_frame)
        is_backward = (current_frame == previous_frame)

        log(f"Current Frame : {current_frame} | Next Frame : {next_frame} | Previous Frame : {previous_frame} | Forward : {is_forward} | Backward : {is_backward} | direction {self.direction}")

        if (is_forward or is_backward ) and (self.navigationMode == QgsTemporalNavigationObject.NavigationMode.Animated):
            log("!!! Signal came from Frame change")
            self.frame = current_frame
            
            key = self.frame // self.time_delta_size
            
            if is_forward:
                if self.direction: # Stays in the same direction
                    if key == self.key + 1 and ((self.end_frame + self.time_delta_size) < self.total_frames): # Fetch Next time delta
                        self.key = key
                        self.switch_time_deltas()                        
                        self.begin_frame = self.begin_frame + self.time_delta_size
                        self.end_frame = self.end_frame + self.time_delta_size
                        self.fetch_next_time_deltas(self.key+1,self.begin_frame, self.end_frame)
                        log(f"forward -> same direction -> next time delta : {self.begin_frame} to {self.end_frame} | direction {self.direction}")

                    self.update_layers(self.frame)

                    fps = 1/(time.time() - recommended_fps_time)
                    log(f"FPS recommendation : {fps}")
                    self.temporal_controller.setFramesPerSecond(fps)
                        
                else: # Direction changed
                    # TODO : Reset the Buffer of time deltas
                    self.direction = True # Reverse
                    log(f"forward -> direction has changed ! : {self.begin_frame} to {self.end_frame} | direction {self.direction}")
                    pass

            elif is_backward:
                if not self.direction: # Stays in the same direction
                    if key == self.key - 1 and (self.begin_frame - self.time_delta_size >= 0): # Fetch Next time delta
                        self.key = key
                        self.switch_time_deltas()
                        self.begin_frame = self.begin_frame - self.time_delta_size
                        self.end_frame = self.end_frame - self.time_delta_size
                        self.fetch_next_time_deltas(self.key-1, self.begin_frame, self.end_frame)
                        log(f"backward -> same direction -> next time delta : {self.begin_frame} to {self.end_frame} | direction {self.direction}")

                    self.update_layers(self.frame)
                    fps = 1/(time.time() - recommended_fps_time)
                    log(f"FPS recommendation : {fps}")
                    self.temporal_controller.setFramesPerSecond(fps)
                else: # Direction changed
                    # TODO : Reset the Buffer of time deltas
                    self.direction = False # Forward
                    log(f"backward -> direction has changed ! : {self.begin_frame} to {self.end_frame}  | direction {self.direction} ")
                    pass

            
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
            log("\n\n#### Signal is not for frame change ####\n\n")
            log(f"TotalFrameCount : {self.temporal_controller.totalFrameCount()}")
            log(f"temporalRangeCumulative : {self.temporal_controller.temporalRangeCumulative()}")
            log(f"temporalExtents : {self.temporal_controller.temporalExtents()}")
            log(f"NavigationMode : {self.temporal_controller.navigationMode()}")
            log(f"isLooping : {self.temporal_controller.isLooping()}")
            log(f"FPS : {self.temporal_controller.framesPerSecond()}")
            log(f"Frame duration : {self.temporal_controller.frameDuration()}")
            log(f"Available temporal range : {self.temporal_controller.availableTemporalRanges()}")
            log(f"Animation state : {self.temporal_controller.animationState()}")
    
            log(f"Current Frame : {self.temporal_controller.currentFrameNumber()}")

            self.temporal_controller.pause()
            iface.messageBar().pushMessage("Info", "Animation Paused : Temporal Controller settings where changed", level=Qgis.Info)

            if self.temporal_controller.navigationMode() != self.navigationMode: # Navigation Mode change -> For now only allow animated mode
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


            elif self.temporal_controller.frameDuration() != self.frameDuration: # Frame duration change ==> Restart animation
                # log(f"Frame duration has changed from {self.frameDuration} with {self.total_frames} frames")
                self.frameDuration = self.temporal_controller.frameDuration()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.frameDuration} with {self.total_frames} frames")

                log(f"Delete all time deltas from layers, reload 2 time deltas for the current key ") # TODO : even load 3 time deltas ? to allow both directions for the user.
                self.reload_time_deltas()
            
            elif self.temporal_controller.temporalExtents() != self.temporalExtents:
                log(f"temporal extents have changed from {self.temporalExtents} with {self.total_frames} frames")
                self.temporalExtents = self.temporal_controller.temporalExtents()
                self.total_frames = self.temporal_controller.totalFrameCount()
                log(f"to {self.temporalExtents} with {self.total_frames} frames")   
            elif self.temporal_controller.temporalRangeCumulative() != self.cumulativeRange:
                log(f"Cumulative range has changed from {self.cumulativeRange}")
                self.cumulativeRange = self.temporal_controller.temporalRangeCumulative()
                log(f"to {self.cumulativeRange}")
            else:
                # Not handled : FPS change/cumulative range(no signal sent), animation state => Not handled, loop state => Not handled
                
                # Currently we assume this just means the user has skipped through the timeline
                key = current_frame // self.time_delta_size
                if key != self.key:
                    self.reload_time_deltas()
                    log(f"Reload time deltas for the new key {key}")
                else:
                    log("timeline skipped in the same key")


    def cancel_all_tasks(self):
        self.mobilitydb_layer_handler.cancel_tasks()


    def reload_time_deltas(self):
        iface.messageBar().pushMessage("Info", "Please wait for TGeomPoints to be reloaded", level=Qgis.Info)
        self.frame = self.temporal_controller.currentFrameNumber()
        self.key = self.frame // self.time_delta_size
        for task in self.task_manager.tasks():
            task.waitForFinished()
        # self.movelayer_handler.delete_all_time_deltas()
        log("Reload Time deltas")
        self.cancel_all_tasks()
        self.launch_animation(self.key)
        
tt = Move()