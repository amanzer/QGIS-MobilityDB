"""
Solution A :

We fetch all the TgeomPoints at the start
Create the Vector Layer and Temporal Controller

During the animation, apply ValueAtTimestamp to all tgeompoints in ONF

"""


from pymeos.db.psycopg import MobilityDB
from pymeos import *
from datetime import datetime, timedelta
import time
from pympler import asizeof
from enum import Enum
import numpy as np
from shapely.geometry import Point
import math
import os
import psutil
# Enum classes
from matplotlib import pyplot as plt
import pickle

class Time_granularity(Enum):
    # MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds, "name" : "MILLISECOND"}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds, "name" : "SECOND", "steps" : 1}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes, "name" : "MINUTE", "steps" : 1}
    # HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours, "name" : "HOUR"}

    @classmethod
    def set_time_step(cls, steps):
        Time_granularity.SECOND.value["timedelta"] = timedelta(seconds=steps)
        Time_granularity.SECOND.value["steps"] = steps
        Time_granularity.MINUTE.value["timedelta"] = timedelta(minutes=steps)
        Time_granularity.MINUTE.value["steps"] = steps
        return cls

# class Direction(Enum):
#     FORWARD = 1
#     BACKWARD = 0

# Global parameters

PERCENTAGE_OF_OBJECTS = 0.5 # To not overload the memory, we only take a percentage of the ships in the database
FPS = 100


# TODO : Use Qgis data provider to access database and tables
SRID = 4326
########## AIS Danish maritime dataset ##########
DATA_SRID = 4326
DATABASE_NAME = "mobilitydb"
TPOINT_TABLE_NAME = "PyMEOS_demo"
TPOINT_ID_COLUMN_NAME = "MMSI"
TPOINT_COLUMN_NAME = "trajectory"
GRANULARITY = Time_granularity.set_time_step(1).MINUTE

########## AIS Danish maritime dataset ##########
# DATA_SRID =  
# DATABASE_NAME = "DanishAIS"
# TPOINT_TABLE_NAME = "Ships"
# TPOINT_ID_COLUMN_NAME = "MMSI"
# TPOINT_COLUMN_NAME = "trip"
# GRANULARITY = Time_granularity.set_time_step(1).MINUTE

########## LIMA PERU drivers dataset ##########
# DATA_SRID = 4326
# DATABASE_NAME = "lima_demo"
# TPOINT_TABLE_NAME = "driver_paths"
# TPOINT_ID_COLUMN_NAME = "driver_id"
# TPOINT_COLUMN_NAME = "trajectory"
# GRANULARITY = Time_granularity.set_time_step(5).SECOND



def log(msg):
    """
    Function to log messages in the QGIS log window.
    """
    QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)




class Fetch_tpoints_thread(QgsTask):
    """
    This thread creates next time delta's the matrix containing the positions for all objects to show. 
    """
    def __init__(self, description,project_title, db_params, extent, finished_fnc, failed_fnc):
        super(Fetch_tpoints_thread, self).__init__(description, QgsTask.CanCancel)

        self.project_title = project_title

 
        self.extent = extent
        self.db = db_params
        self.finished_fnc = finished_fnc
        self.failed_fnc = failed_fnc


        pid = os.getpid()
        log(f"QgisThread init pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")
            
    
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
            pid = os.getpid()
            log(f"QgisThread run pid : {pid} | affinity : {psutil.Process(pid).cpu_affinity()}")
            now = time.time()
            self.db.initiate_connection()

            start_date = self.db.get_min_timestamp()
            end_date = self.db.get_max_timestamp()
            self.total_frames = math.ceil( (end_date - start_date) // GRANULARITY.value["timedelta"] ) + 1
        

            self.timestamps = [start_date + i * GRANULARITY.value["timedelta"] for i in range(self.total_frames)]
            self.timestamps = [dt.replace(tzinfo=None) for dt in self.timestamps]
            self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]

          
            self.result_params = {
                'tgeompoints' : self.db.get_tgeompoints(self.extent),
                'timestamps' : self.timestamps,
                'timestamps_strings' : self.timestamps_strings,
                'start_time' : now

            }
        except Exception as e:
            log(f"Error in run method : {e}")
            self.error_msg = str(e)
            return False
        return True




class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self, params, extent):
        try: 
            self.table_name = TPOINT_TABLE_NAME
            self.id_column_name = TPOINT_ID_COLUMN_NAME
            self.tpoint_column_name = TPOINT_COLUMN_NAME                  
            self.connection = MobilityDB.connect(**params)
            self.extent = extent
            


        except Exception as e:
            log(f"{e}")

    def initiate_connection(self):
        try:
            x_min,y_min, x_max, y_max = self.extent
            self.cursor = self.connection.cursor()

            self.cursor.execute(f"""
                                WITH trajectories as (
                                    SELECT 
                                        atStbox(
                                            a.{self.tpoint_column_name}::tgeompoint,
                                            stbox(
                                                ST_MakeEnvelope(
                                                    {x_min}, {y_min}, -- xmin, ymin
                                                    {x_max}, {y_max}, -- xmax, ymax
                                                    {DATA_SRID} -- SRID
                                                )
                                            )
                                        ) as trajectory, a.{self.id_column_name} as id
                                    FROM public.{self.table_name} as a )

                                    SELECT tr.id                            
                                    FROM trajectories as tr where tr.trajectory is not null ;
                                """)
                                
            self.ids_list = self.cursor.fetchall()
            self.ids_list = self.ids_list[:int(len(self.ids_list)*PERCENTAGE_OF_OBJECTS)]
            self.objects_count = len(self.ids_list)

            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            self.objects_id_str = ', '.join(map(str, ids_list))
        except Exception as e:
            log(f"{e}")

    def get_objects_ids(self):
        return self.ids_list

    def get_objects_str(self):
        return self.objects_id_str


    def get_objects_count(self):
        return self.objects_count
        

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            query = f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};"
            self.cursor.execute(query)
            # log(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            pass


    def get_max_timestamp(self):
        """
        Returns the max timestamp of the tpoints columns.

        """
        try:
            self.cursor.execute(f"SELECT MAX(endTimestamp({self.tpoint_column_name})) AS latest_timestamp FROM public.{self.table_name};")
            return self.cursor.fetchone()[0]
        except Exception as e:
            pass


    def get_tgeompoints(self, extent):

        x_min,y_min, x_max, y_max = extent
        tpoint_column_name = TPOINT_COLUMN_NAME
        table_name = TPOINT_TABLE_NAME
        id_column_name = TPOINT_ID_COLUMN_NAME
        ids_str = self.objects_id_str
        p_start = self.get_min_timestamp()
        p_end = self.get_max_timestamp()
    

        query = f"""
                SELECT 
                        atStbox(
                            a.{tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {x_min}, {y_min}, -- xmin, ymin
                                    {x_max}, {y_max}, -- xmax, ymax
                                    {DATA_SRID} -- SRID
                                ),
                                tstzspan('[{p_start}, {p_end}]')
                            )
                        ) as trajectory
                    FROM public.{table_name} as a 
                    WHERE a.{id_column_name} in ({ids_str}) ;
                """
        log(query)
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def close(self):
        """
        Close the connection to the MobilityDB database.
        """
        self.cursor.close()
        self.connection.close()




class QVIZ:
    """

    This class plays the role of the controller in the MVC pattern.
    It is used to manage the user interaction with the View, which is the QGIS UI.
    
    It handles the interactions with both the Temporal Controller and the Vector Layer.
    """
    def __init__(self):    
        
        self.create_vlayer()
        self.canvas = iface.mapCanvas()
        self.canvas.setDestinationCrs(QgsCoordinateReferenceSystem(f"EPSG:{SRID}"))
        
        self.extent = self.canvas.extent().toRectF().getCoords()

        self.task_manager = QgsApplication.taskManager()
        pymeos_initialize()

        connection_params= {
                "host": "localhost",
                "port": 5432,
                "dbname": DATABASE_NAME,
                "user": "postgres",
                "password": "postgres"
            }
        
        self.db = Database_connector(connection_params, self.extent)
 

        task = Fetch_tpoints_thread("Fetching tpoints", "Fetching tpoints", self.db, self.extent, self.initiate_temporal_controller, self.raise_error)
        self.task_manager.addTask(task)
    

        self.avg_nb_objects = 0
        self.avg_count = 0

        self.fps_record = []
        self.vat_record = []
        self.geom_record = []
        
        # self.canvas.extentsChanged.connect(self.test(3))
 



    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
        """
        self.vlayer = QgsVectorLayer("Point", "MobilityBD Data", "memory")
        pr = self.vlayer.dataProvider()
        pr.addAttributes([QgsField("id", QVariant.Int), QgsField("start_time", QVariant.DateTime), QgsField("end_time", QVariant.DateTime)])
        self.vlayer.updateFields()
        tp = self.vlayer.temporalProperties()
        tp.setIsActive(True)
        # tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeInstantFromField)
        tp.setMode(qgis.core.QgsVectorLayerTemporalProperties.ModeFeatureDateTimeStartAndEndFromFields)
        # tp.setStartField("time")
        tp.setStartField("start_time")
        tp.setEndField("end_time")
        self.vlayer.updateFields()

        QgsProject.instance().addMapLayer(self.vlayer)
        
    

    def generate_qgis_features(self,object_ids, vlayer_fields,  start_date, end_date):
        features_list =[]
        start_datetime_obj = QDateTime(start_date)
        end_datetime_obj = QDateTime(end_date)
        num_objects = len(object_ids)
        # empty_geom = QgsGeometry.fromWkt("POINT EMPTY")
        
        self.geometries={}
        for i in range(1, num_objects+1):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([ object_ids[i-1][0],start_datetime_obj, end_datetime_obj])
            geom = QgsGeometry()
            self.geometries[i] = geom
            feat.setGeometry(geom)
            features_list.append(feat)
        
        self.vlayer.dataProvider().addFeatures(features_list)
        log(f"{num_objects} Qgis features created")

    
    def memory_usage(self, obj):
        """
        Returns the memory usage of the object in paramter, in mega bytes.
        """
        size_in_bytes = asizeof.asizeof(obj)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        print(f"Total size: {size_in_megabytes:.6f} MB")

    
    # Getters

    def get_canvas_extent(self):
        return self.extent
    

    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """
        print(f"Average FPS : {sum(self.fps_record)/len(self.fps_record)} over {len(self.fps_record)} frames")
        print(f"Time to fetch : {self.TIME_fetch_tgeompoints}")
        
        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionA/desktop_results/Solution_A_{PERCENTAGE_OF_OBJECTS}_fps_record.pickle", "wb") as file:
            pickle.dump(self.fps_record, file)
        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionA/desktop_results/Solution_A_{PERCENTAGE_OF_OBJECTS}_vat_record.pickle", "wb") as file:
            pickle.dump(self.vat_record, file)
        with open(f"/home/ali/QGIS-MobilityDB/refactored_code/solutionA/desktop_results/Solution_A_{PERCENTAGE_OF_OBJECTS}_geom_record.pickle", "wb") as file:
            pickle.dump(self.geom_record, file)


    # Setters 

    def set_temporal_controller_extent(self, time_range):
        if self.temporalController:
            self.temporalController.setTemporalExtents(time_range)
    

    def set_temporal_controller_frame_duration(self, interval):
        if self.temporalController:
            self.temporalController.setFrameDuration(interval)
    

    def set_temporal_controller_frame_rate(self, frame_rate):
        if self.temporalController:
            self.temporalController.setFramesPerSecond(frame_rate)


    def set_temporal_controller_frame_number(self, frame_number):
        if self.temporalController:
            self.temporalController.setCurrentFrameNumber(frame_number)

    def set_qgis_features(self, features_list):
        if self.vlayer:
            self.vlayer.dataProvider().addFeatures(features_list)

    def set_fps(self, fps):
        self.fps = fps

    # Methods to handle the temporal controller
    
    def play(self, direction):
        """
        Plays the temporal controller animation in the given direction.
        """
        if direction == 1:
            self.temporalController.playForward()
        else:
            self.temporalController.playBackward()


    def pause(self):
        """
        Pauses the temporal controller animation.
        """
        self.temporalController.pause()


    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller to be the closest multiple of 5,
        favoring the lower value in case of an exact halfway.
        """
        # Calculating the optimal FPS based on the new frame time
        optimal_fps = 1 / new_frame_time
        fps = min(optimal_fps, self.fps)

        self.temporalController.setFramesPerSecond(fps)
        log(f"{fps} : FPS {optimal_fps}")
        self.fps_record.append(optimal_fps)

    
    def on_new_frame(self):
        """
       
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        
        now = time.time()
        curr_frame = self.temporalController.currentFrameNumber()
        # log("ONF START")
        # self.new_geometries = {}
        # hits = 0
        for i in range(1, self.objects_count+1):
            # Fetching the position of the object at the current frame
            try:
                position = self.tpoints[i-1][0].value_at_timestamp(self.timestamps[curr_frame])
                
                # Updating the geometry of the feature in the vector layer
                self.geometries[i].fromWkb(position.wkb) # = QgsGeometry.fromWkt(position.wkt)
                # hits+=1
            except:
                continue
        # log(f"hits : {hits}")

        self.vat_record.append(time.time() - now)

        mid_time = time.time()
        self.vlayer.startEditing()
        self.vlayer.dataProvider().changeGeometryValues(self.geometries)
        self.vlayer.commitChanges()
        self.canvas.refresh()
        iface.vectorLayerTools().stopEditing(self.vlayer)

        self.geom_record.append(time.time() - mid_time)

        # log("END ONF")
        self.avg_count += 1
        self.avg_nb_objects += ( len(self.geometries) - self.avg_nb_objects ) / self.avg_count
        

        

        self.update_frame_rate(time.time()-now)
    
 
    

    def initiate_temporal_controller(self, params):
        self.TIME_fetch_tgeompoints = time.time() - params['start_time']
        self.tpoints = params['tgeompoints']
     
        self.timestamps = params['timestamps']
        self.timestamps_strings = params['timestamps_strings']
        self.objects_count = self.db.get_objects_count()
        


        self.temporalController = self.canvas.temporalController()
        self.temporalController.setCurrentFrameNumber(0)

        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        interval = QgsInterval(GRANULARITY.value["steps"], GRANULARITY.value["qgs_unit"])
        self.fps = 60
        

        self.set_temporal_controller_extent(time_range) 
        self.set_temporal_controller_frame_duration(interval)
        self.set_temporal_controller_frame_rate(self.fps)

        self.generate_qgis_features(self.db.get_objects_ids(), self.vlayer.fields(), self.timestamps[0], self.timestamps[-1])
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)


    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            log("Error: " + msg)
        else:
            log("Unknown error")




tt = QVIZ()