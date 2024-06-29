import numpy as np
from shapely.geometry import Point
import psutil
import os


def process_chunk2(args):
    try:
        from pymeos.db.psycopg import MobilityDB
        ids, begin_frame, end_frame, TIME_DELTA_SIZE, start_date, empty_point_wkt, connection_params, steps, name, tpoint_column_name, table_name, id_column_name, extent, timestamps, cpus = args

        pid = os.getpid()
        # cpu_count = psutil.cpu_count()
        psutil.Process(pid).cpu_affinity([cpus])
            

        ids_list_str = [ f"'{id[0]}'"  for id in ids]
        ids_str = ', '.join(map(str, ids_list_str))


        p_start = timestamps[begin_frame]
        p_end = timestamps[end_frame]
        start_date = timestamps[0]
        x_min,y_min, x_max, y_max = extent
        
        connection = MobilityDB.connect(**connection_params)    
        cursor = connection.cursor()

        if name == "SECOND": # TODO : handle granularity of different time steps(5 seconds etc)
            time_value = 1 * steps
        elif name == "MINUTE":
            time_value = 60 * steps

        query = f"""WITH trajectories as (
                SELECT 
                    atStbox(
                        a.{tpoint_column_name}::tgeompoint,
                        stbox(
                            ST_MakeEnvelope(
                                {x_min}, {y_min}, -- xmin, ymin
                                {x_max}, {y_max}, -- xmax, ymax
                                0 -- SRID
                            ),
                            tstzspan('[{p_start}, {p_end}]')
                        )
                    ) as trajectory
                FROM public.{table_name} as a 
                WHERE a.{id_column_name} in ({ids_str})),

                resampled as (

                SELECT tsample(traj.trajectory, INTERVAL '{steps} {name}', TIMESTAMP '{start_date}')  AS resampled_trajectory
                    FROM 
                        trajectories as traj)
            
                SELECT
                        EXTRACT(EPOCH FROM (startTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS start_index ,
                        EXTRACT(EPOCH FROM (endTimestamp(rs.resampled_trajectory) - '{start_date}'::timestamp))::integer / {time_value} AS end_index,
                        rs.resampled_trajectory
                FROM resampled as rs ;"""

        cursor.execute(query)
        # logs += f"query : {query}\n"
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # with open("log_process_chunk.txt", "a") as f:
        #     f.write(f"successfully created {len(rows)} rows  \n")


        chunk_matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
        logs = f"pid : {pid} assigned to cpu : {cpus} \n"

        for i in range(len(rows)):
            if rows[i][2] is not None:
                traj_resampled = rows[i][2]

                start_index = rows[i][0] - begin_frame
                end_index = rows[i][1] - begin_frame
                values = np.array([point.wkt for point in traj_resampled.values()])
                chunk_matrix[i, start_index:end_index+1] = values
            

        return 0, chunk_matrix, logs
    except Exception as e:
        return 1, None, f"Error in worker: {e}\n"
        # return None, None

    
 