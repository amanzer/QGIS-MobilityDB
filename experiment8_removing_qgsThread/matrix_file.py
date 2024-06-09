"""

File used to create the matrices for the time deltas between the begin_frame and end_frame.

The matrices are saved in the /home/ali/matrices/ folder.

"""

import numpy as np
from shapely.geometry import Point
from pymeos.db.psycopg import MobilityDB

from pymeos import *
import os
import sys
from datetime import timedelta, datetime
from pymeos import *
import time


now = time.time()

FPS_DEQUEUE_SIZE = 5 # Length of the dequeue to calculate the average FPS
TIME_DELTA_DEQUEUE_SIZE =  10 # Length of the dequeue to keep the keys to keep in the buffer


args = sys.argv[1:]

begin_frame = int(args[0])
end_frame = int(args[1])
TIME_DELTA_SIZE = end_frame - begin_frame + 1
PERCENTAGE_OF_OBJECTS = float(args[2])


SRID = 4326



class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self):
        try: 
            connection_params = {
            "host": "localhost",
            "port": 5432,
            "dbname": "mobilitydb",
            "user": "postgres",
            "password": "postgres"
            }
            self.table_name = "PyMEOS_demo"
            self.id_column_name = "MMSI"
            self.tpoint_column_name = "trajectory"                    
            self.connection = MobilityDB.connect(**connection_params)

            self.cursor = self.connection.cursor()

            self.cursor.execute(f"SELECT {self.id_column_name} FROM public.{self.table_name};")
            self.ids_list = self.cursor.fetchall()
            self.ids_list = self.ids_list[:int(len(self.ids_list)*PERCENTAGE_OF_OBJECTS)]
        except Exception as e:
            pass

  
    def get_subset_of_tpoints(self, pstart, pend, xmin, ymin, xmax, ymax):
        """
        For each object in the ids_list :
            Fetch the subset of the associated Tpoints between the start and end timestamps
            contained in the STBOX defined by the xmin, ymin, xmax, ymax.
        """
        try:
           
            ids_list = [ f"'{id[0]}'"  for id in self.ids_list]
            ids_str = ', '.join(map(str, ids_list))
          
            query = f"""
                    SELECT 
                        atStbox(
                            a.{self.tpoint_column_name}::tgeompoint,
                            stbox(
                                ST_MakeEnvelope(
                                    {xmin}, {ymin}, -- xmin, ymin
                                    {xmax}, {ymax}, -- xmax, ymax
                                    4326 -- SRID
                                ),
                                tstzspan('[{pstart}, {pend}]')
                            )
                        )
                    FROM public.{self.table_name} as a 
                    WHERE a.{self.id_column_name} in ({ids_str});
                    """
            self.cursor.execute(query)
            # print(query)
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            # print(e)
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



file_name = f"/home/ali/matrices/matrix_{begin_frame}.npy"


  
Time_granularities = {"MILLISECOND" : timedelta(milliseconds=1),
                      "SECOND" : timedelta(seconds=1),
                      "MINUTE" : timedelta(minutes=1),
                      "HOUR" : timedelta(hours=1),
                    }


# check if file does't already exist

if not os.path.exists(file_name):
    pymeos_initialize()
    db = Database_connector()

    x_min = float(args[3])
    y_min = float(args[4])
    x_max = float(args[5])
    y_max = float(args[6])

    start_date = args[7]
    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')


    total_frames = int(args[8])
    GRANULARITY = Time_granularities[args[9]]

    timestamps = []
    for i in range(total_frames): 
        timestamps.append(start_date + i*GRANULARITY)



    p_start = timestamps[begin_frame]
    p_end = timestamps[end_frame]
    # print(p_start, p_end, x_min, y_min, x_max, y_max)
    rows = db.get_subset_of_tpoints(p_start, p_end, x_min, y_min, x_max, y_max)    
      
            
    empty_point_wkt = Point().wkt  # "POINT EMPTY"
    matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
   
    time_ranges = timestamps
    now = time.time()

    
    for i in range(len(rows)):
        try:
            traj = rows[i][0]
            traj = traj.temporal_precision(GRANULARITY) 
            num_instants = traj.num_instants()
            if num_instants == 0:
                continue
            elif num_instants == 1:
                single_timestamp = traj.timestamps()[0].replace(tzinfo=None)
                index = time_ranges.index(single_timestamp) - begin_frame
                matrix[i][index] = traj.values()[0].wkt
               
            elif num_instants >= 2:
                traj_resampled = traj.temporal_sample(start=time_ranges[0],duration= GRANULARITY)
                
                start_index = time_ranges.index( traj_resampled.start_timestamp().replace(tzinfo=None) ) - begin_frame
                end_index = time_ranges.index( traj_resampled.end_timestamp().replace(tzinfo=None) ) - begin_frame
        
                trajectory_array = np.array([point.wkt for point in traj_resampled.values()])
                matrix[i, start_index:end_index+1] = trajectory_array
       
        except:
            continue
    
    np.save(file_name, matrix)
    
    db.close()
    pymeos_finalize()
    total_time = time.time() - now
    frames_for_30_fps= 30 * total_time
    print(f"================================================================     Matrix {begin_frame} created in {total_time} seconds, {frames_for_30_fps} frames for 30 fps animation.")