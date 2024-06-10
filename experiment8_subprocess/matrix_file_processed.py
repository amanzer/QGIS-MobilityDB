"""

File used to create the matrices for the time deltas between the begin_frame and end_frame.

The matrices are saved in the /home/ali/matrices/ folder.


            # arguments :[ 
            # 0: begin_frame, 
            # 1: end_frame, 
            # 2: PERCENTAGE_OF_OBJECTS, 
            # 3: x_min, 4: y_min, 5: x_max, 6: y_max, 
            # 7: start_timestamp, 
            # 8: total_frames, 
            # 9: granularity, 
            # 10: matrix_directory_path, 
            # 11: database_name, 
            # 12: table_name, 
            # 13: id_column_name, 
            # 14: tpoint_column_name]

To measure the size of matrices folder : du -sh --block-size=MB matrices            

"""

import numpy as np
from shapely.geometry import Point
from pymeos.db.psycopg import MobilityDB

import os
import sys
from datetime import timedelta, datetime
from pymeos import *
import time

logs = ""
now = time.time()

FPS_DEQUEUE_SIZE = 5 # Length of the dequeue to calculate the average FPS
TIME_DELTA_DEQUEUE_SIZE =  10 # Length of the dequeue to keep the keys to keep in the buffer


args = sys.argv[1:]
logs += f"Args: {args}\n"
begin_frame = int(args[0])
end_frame = int(args[1])
TIME_DELTA_SIZE = end_frame - begin_frame + 1
PERCENTAGE_OF_OBJECTS = float(args[2])


SRID = 4326


DATABASE_NAME = args[11]
TPOINT_TABLE_NAME = args[12]
TPOINT_ID_COLUMN_NAME = args[13]
TPOINT_COLUMN_NAME = args[14]



class Database_connector:
    """
    Singleton class used to connect to the MobilityDB database.
    """
    
    def __init__(self):
        try: 
            connection_params = {
            "host": "localhost",
            "port": 5432,
            "dbname": DATABASE_NAME,
            "user": "postgres",
            "password": "postgres"
            }
            self.table_name = TPOINT_TABLE_NAME
            self.id_column_name = TPOINT_ID_COLUMN_NAME
            self.tpoint_column_name = TPOINT_COLUMN_NAME               
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
                    WHERE a.{self.id_column_name} in ({ids_str})
                    AND a.{self.tpoint_column_name} IS NOT NULL;
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


MATRIX_DIRECTORY_PATH = "/home/ali/matrices"
file_name = f"/home/ali/matrices/matrix_{begin_frame}.npy"



Time_granularities = {
                    # "MILLISECOND" : timedelta(milliseconds=1),
                    "SECOND" : timedelta(seconds=1),
                    "MINUTE" : timedelta(minutes=1),
                    #   "HOUR" : timedelta(hours=1),
                    }


# check if file does't already exist

if not os.path.exists(file_name):
    try:
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
                traj_resampled = rows[i][0]
                num_instants = traj_resampled.num_instants()

                start_index = time_ranges.index( traj_resampled.start_timestamp().replace(tzinfo=None) ) - begin_frame
                end_index = time_ranges.index( traj_resampled.end_timestamp().replace(tzinfo=None) ) - begin_frame

                trajectory_array = np.array([point.wkt for point in traj_resampled.values()])
                matrix[i, start_index:end_index+1] = trajectory_array
                # print(f"start_index: {start_index}, end_index: {end_index}, => {(end_index+1) - start_index}, len: {len(trajectory_array)}")
                        
            except:
                continue
        
        np.save(file_name, matrix)
        
        db.close()
        pymeos_finalize()
        total_time = time.time() - now
        frames_for_30_fps= 30 * total_time
        print(f"================================================================     Matrix {begin_frame} created in {total_time} seconds, {frames_for_30_fps} frames for 30 fps animation.")
        logs += f"time to create matrix {begin_frame}: {total_time} seconds\n"
    except Exception as e:
        logs += f"Error: {e}\n"
        with open(f"/home/ali/matrices/logs.txt", "a") as file:
            file.write(logs)



with open(f"/home/ali/matrices/logs.txt", "a") as file:
    file.write(logs)

