import numpy as np
from shapely.geometry import Point
import h5py
from replacing_qgsthread_with_subprocess import Database_connector
from replacing_qgsthread_with_subprocess import GRANULARITY
from pymeos import *


args = sys.argv[1:]

begin_frame = int(args[0])
end_frame = int(args[1])
TIME_DELTA_SIZE = end_frame - begin_frame + 1

file_name = f"matrix_{begin_frame}.h5"
GRANULARITY = Time_granularity.MINUTE
# check if file does't already exist

if not os.path.exists(file_name):

    pymeos_initialize()
    db = Database_connector()

    x_min = float(args[2])
    y_min = float(args[3])
    x_max = float(args[4])
    y_max = float(args[5])

    timestamps = args[6:]
    timestamps = [dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") for ts in timestamps]

    db.get_subset_of_tpoints(time_delta_key)
    p_start = timestamps[begin_frame]
    p_end = timestamps[end_frame]

    rows = db.get_subset_of_tpoints(p_start, p_end, x_min, y_min, x_max, y_max)    
      
            
    empty_point_wkt = Point().wkt  # "POINT EMPTY"
    matrix = np.full((len(rows), TIME_DELTA_SIZE), empty_point_wkt, dtype=object)
   
    time_ranges = timestamps
    now = time.time()

    
    for i in range(len(rows)):
        try:
            traj = rows[i][0]
            traj = traj.temporal_precision(GRANULARITY.value["timedelta"]) 
            num_instants = traj.num_instants()
            if num_instants == 0:
                continue
            elif num_instants == 1:
                single_timestamp = traj.timestamps()[0].replace(tzinfo=None)
                index = time_ranges.index(single_timestamp) - begin_frame
                matrix[i][index] = traj.values()[0].wkt
               
            elif num_instants >= 2:
                traj_resampled = traj.temporal_sample(start=time_ranges[0],duration= GRANULARITY.value["timedelta"])
                
                start_index = time_ranges.index( traj_resampled.start_timestamp().replace(tzinfo=None) ) - begin_frame
                end_index = time_ranges.index( traj_resampled.end_timestamp().replace(tzinfo=None) ) - begin_frame
        
                trajectory_array = np.array([point.wkt for point in traj_resampled.values()])
                matrix[i, start_index:end_index+1] = trajectory_array
       
        except:
            continue
    
    #write matrix to hdf5 file 
    with h5py.File(file_name, 'w') as hf:
        hf.create_dataset("matrix", data=matrix)

    db.close()
    pymeos.finalize()
    
    