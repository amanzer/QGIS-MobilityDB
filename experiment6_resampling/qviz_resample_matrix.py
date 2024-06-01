"""
libraries to install:
    - pymeos
    - psycopg2
    - numpy
    - shapely
    - pympler

    
"""

# TODO : Include the PYQGIS imports for the plugin
from pymeos.db.psycopg import MobilityDB
import psycopg2
from pymeos import *
from datetime import datetime, timedelta
import time
from pympler import asizeof
from enum import Enum
import numpy as np
from shapely.geometry import Point





class Time_granularity(Enum):
    MILLISECOND = {"timedelta" : timedelta(milliseconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Milliseconds}
    SECOND = {"timedelta" : timedelta(seconds=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Seconds}
    MINUTE = {"timedelta" : timedelta(minutes=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Minutes}
    HOUR = {"timedelta" : timedelta(hours=1), "qgs_unit" : QgsUnitTypes.TemporalUnit.Hours}
  


SRID = 4326
PERCENTAGE_OF_OBJECTS = 0.1 # Percentage of objects to be displayed from the dataset
GRANULARITY = Time_granularity.MINUTE

class Data_in_memory:
    """
    This class handles the data stored in memory and the background threads that fetch the data from the MobilityDB database.
    
    It is the link between the QGIS UI (temporal Controller/Vector Layer controlled by the qviz class) and the MobilityDB database.
    
    """
    def __init__(self):

        self.task_manager = QgsApplication.taskManager()
        self.db = MobilityDB_Database()
        pymeos_initialize()
        
        self.ids_list = self.db.get_subset_of_ids(PERCENTAGE_OF_OBJECTS) 
        
        self.generate_timestamps()

        empty_point_wkt = Point().wkt  # "POINT EMPTY"
        self.matrix = np.full((len(self.ids_list), 1), empty_point_wkt, dtype=object)
            
        task = QgisThread(f"Generating the Matrix",
                                     "qViz",self.db,self.timestamps, self.ids_list, self.on_thread_completed, self.raise_error)
        self.task_manager.addTask(task)     


    def generate_timestamps(self):
        """
        Generates the timestamps associated to the frames of the temporal controller.
        """
        start_date = self.db.get_min_timestamp().replace(tzinfo=None)
        end_date = self.db.get_max_timestamp().replace(tzinfo=None)
        self.total_frames = (end_date - start_date) // GRANULARITY.value["timedelta"]

        self.timestamps = [start_date + i * GRANULARITY.value["timedelta"] for i in range(self.total_frames)]
        self.timestamps_strings = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in self.timestamps]


    def update_temporal_controller_extent(self, temporalController):
        """
        Updates the extent of the temporal controller to match the time range of the data.
        """
        time_range = QgsDateTimeRange(self.timestamps[0], self.timestamps[-1])
        temporalController.setTemporalExtents(time_range)


    def on_thread_completed(self, params):
        """
        Function called when a thread finishes its job to fetch the data from the MobilityDB database.
        """      
        self.matrix = params['matrix']
        self.log("Data fetched from the MobilityDB database")
        self.log(f"Time to fetch the data : {params['time']} seconds")
        size_in_bytes = asizeof.asizeof(self.matrix)
        size_in_megabytes = size_in_bytes / (1024 * 1024)
        self.log(f"Total size of matrix (including referenced objects): {size_in_megabytes:.6f} MB")



    def raise_error(self, msg):
        """
        Function called when the task to fetch the data from the MobilityDB database failed.
        """
        if msg:
            self.log("Error: " + msg)
        else:
            self.log("Unknown error")

    def update_vlayer_features(self, vlayer ,frame_number):
        """
        Updates the features of the vector layer for the given frame number.
        """
        now = time.time()
        key =  self.timestamps_strings[frame_number]
        datetime_obj = QDateTime.fromString(key, "yyyy-MM-dd HH:mm:ss")
        current_time_stamp_column =self.matrix[:, frame_number]

        datetime_objs = {i: datetime_obj for i in range(len(self.ids_list))}
        new_geometries = {}  # Dictionary {feature_id: QgsGeometry}

      
        for i in range(current_time_stamp_column.shape[0]): #TODO : compare vs Nditer
            # geometry = QgsGeometry.fromPointXY(QgsPointXY(coords[0], coords[1]))
            new_geometries[i] = QgsGeometry.fromWkt(current_time_stamp_column[i])

        vlayer.startEditing()
        # Updating attribute values for all features
        attribute_changes = {fid: {0: datetime_objs[fid]} for fid in datetime_objs}
        vlayer.dataProvider().changeAttributeValues(attribute_changes)
        # Updating geometries for all features
        vlayer.dataProvider().changeGeometryValues(new_geometries)


        vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(vlayer)

        self.log(f"Time to update the features : {time.time()-now} seconds")

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)


class QgisThread(QgsTask):
    """
    Creates a thread that fetches data from the MobilityDB database 
    
    It creates and fills the matrix.

    This allows to keep the UI responsive while the data is being fetched.
    """
    def __init__(self, description, project_title,db, timestamps, ids_list, finished_fnc,
                 failed_fnc):
        super(QgisThread, self).__init__(description, QgsTask.CanCancel)
        self.project_title = project_title
        self.db = db
        self.timestamps = timestamps
        self.ids_list = ids_list
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
        Creates the matrix of points for all frames and ids in ids_list
        """
        try:
            now = time.time()
            rows = self.db.get_subset_of_tpoints(self.ids_list)
            #print(f"Number of rows : {len(rows)}")
            time_ranges = self.timestamps


            empty_point_wkt = Point().wkt  # "POINT EMPTY"
            #Create a numpy matrix of size 1440x len(mmsi_list) with empty points
            matrix = np.full((len(self.ids_list), len(self.timestamps)), empty_point_wkt, dtype=object)
            
            for i, object_id in enumerate(self.ids_list):
                try:
                    traj = rows[object_id]
                    traj = traj.temporal_precision(GRANULARITY.value["timedelta"]) # precision is 1 minute so Two points in a minute will become 1
                    num_instants = traj.num_instants()
                    if num_instants == 0:
                        continue
                    elif num_instants == 1:
                        single_timestamp = traj.timestamps()[0].replace(tzinfo=None)
                        index = time_ranges.index(single_timestamp)
                        matrix[i][index] = traj.values()[0].wkt
                    elif num_instants >= 2:
                        traj_resampled = traj.temporal_sample(start=time_ranges[0],duration= GRANULARITY.value["timedelta"])

                        start_index = time_ranges.index( traj_resampled.start_timestamp().replace(tzinfo=None) )
                        end_index = time_ranges.index( traj_resampled.end_timestamp().replace(tzinfo=None) )

                        trajectory_array = np.array([point.wkt for point in traj_resampled.values()])
                        
                        matrix[i, start_index:end_index+1] = trajectory_array
                except:
                    pass
               
            total_time = time.time() - now

            self.result_params = {
                'matrix': matrix,
                'time': total_time
            }
        except psycopg2.Error as e:
            self.error_msg = str(e)
            return False
        except ValueError as e:
            self.error_msg = str(e)
            return False
        return True



class MobilityDB_Database:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self):
        connection_params = { #TODO Remove the hardcoded values and use the QGIS UI to set the connection parameters
        "host": "localhost",
        "port": 5432,
        "dbname": "mobilitydb",
        "user": "postgres",
        "password": "postgres"
        }
        try: 
            self.table_name = "PyMEOS_demo"
            self.id_column_name = "MMSI"
            self.tpoint_column_name = "trajectory"            
            self.SRID = SRID        
            self.connection = MobilityDB.connect(**connection_params)

            self.cursor = self.connection.cursor()

            self.cursor.execute(f"SELECT {self.id_column_name} FROM public.{self.table_name};")
            self.ids_list = self.cursor.fetchall()
        except Exception as e:
            pass

    def get_subset_of_ids(self, percentage=0.001):
        """
        Returns a subset of the objects ids in the table, based on the given percentage.
        """
        return self.ids_list[:int(len(self.ids_list)*percentage)]

    def get_subset_of_tpoints(self, ids_list):
        """
        For each object in the ids_list :
            Fetch the subset of the associated Tpoints between the start and end timestamps
            contained in the STBOX defined by the xmin, ymin, xmax, ymax.
        """
        try:
            rows={}
            for id in ids_list:
                object_id = id[0]
                self.cursor.execute(f"""SELECT {self.tpoint_column_name} 
                               FROM public.{self.table_name} 
                               WHERE {self.id_column_name} = '{object_id}' ;""")
                subset_tpoint = self.cursor.fetchone()
                if subset_tpoint[0]:
                    rows[id] = subset_tpoint[0]

            return rows
        except Exception as e:
            pass

    def get_min_timestamp(self):
        """
        Returns the min timestamp of the tpoints columns.

        """
        try:
            
            self.cursor.execute(f"SELECT MIN(startTimestamp({self.tpoint_column_name})) AS earliest_timestamp FROM public.{self.table_name};")
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
        self.temporalController = self.canvas.temporalController()
        frame_rate = 60
        self.direction = "forward"
        self.temporalController.setFramesPerSecond(frame_rate)
        interval = QgsInterval(1, GRANULARITY.value["qgs_unit"])
        self.temporalController.setFrameDuration(interval)


        self.data =  Data_in_memory()
        self.last_frame = 0
        self.generate_qgs_features(self.data.timestamps_strings[0], self.data.ids_list)

        self.fps_record = []
        self.feature_number_record = []
        self.temporalController.updateTemporalRange.connect(self.on_new_frame)
        #self.temporalController.stateChanged.connect(self.pause)
        self.canvas.extentsChanged.connect(self.pause)
        self.data.update_temporal_controller_extent(self.temporalController)
        

    def pause(self):
        """
        Pauses the temporal controller animation.
        """
        self.temporalController.pause()

 
    def get_average_fps(self):
        """
        Returns the average FPS of the temporal controller.
        """
        return sum(self.fps_record)/len(self.fps_record)
    

    def update_frame_rate(self, new_frame_time):
        """
        Updates the frame rate of the temporal controller to be the closest multiple of 5,
        favoring the lower value in case of an exact halfway.
        """
        # Calculating the optimal FPS based on the new frame time
        optimal_fps = 1 / new_frame_time
        
        # Round to the nearest multiple of 5
        closest_multiple_of_5 = int(optimal_fps / 5) * 5
     

        # Ensure FPS does not exceed 60
        fps = min(closest_multiple_of_5, 60)

        self.temporalController.setFramesPerSecond(fps)
        self.log(f"FPS : {fps} - Calculated FPS : {optimal_fps}")
        self.fps_record.append(optimal_fps)

    
    def on_new_frame(self):
        """    
        Function called every time the frame of the temporal controller is changed. 
        It updates the content of the vector layer displayed on the map.
        """
        now = time.time()

        curr_frame = self.temporalController.currentFrameNumber()

        self.data.update_vlayer_features(self.vlayer, curr_frame)
            
        self.update_frame_rate(time.time()-now)
    
    
    def create_vlayer(self):
        """
        Creates a Qgis Vector layer in memory to store the points to be displayed on the map.
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

    
    def generate_qgs_features(self, start_time ,ids_list): # TODO : Move to the Data_in_memory class
        datetime_obj = QDateTime.fromString(start_time, "yyyy-MM-dd HH:mm:ss")
        vlayer_fields = self.vlayer.fields()

        empty_point_wkt = Point().wkt  # "POINT EMPTY"
        # create a numpy array of size len(ids_list) with empty_point_wkt
        starting_points = np.full((1, len(ids_list)), empty_point_wkt, dtype=object)
    
        qgis_fields_list = []
        
        for wkt in np.nditer(starting_points, flags=['refs_ok']):
            feat = QgsFeature(vlayer_fields)
            feat.setAttributes([datetime_obj])  # Set its attributes

            # Create geometry from WKT string
            geom = QgsGeometry.fromWkt(wkt.item())
            feat.setGeometry(geom)  # Set its geometry
            qgis_fields_list.append(feat)
        
        self.vlayer.startEditing()
        self.vlayer.addFeatures(qgis_fields_list) # Add list of features to vlayer
        self.vlayer.commitChanges()
        iface.vectorLayerTools().stopEditing(self.vlayer)
            

    def log(self, msg):
        QgsMessageLog.logMessage(msg, 'qViz', level=Qgis.Info)




tt = QVIZ()