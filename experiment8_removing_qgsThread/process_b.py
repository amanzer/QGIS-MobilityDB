import numpy as np
from shapely.geometry import Point
import h5py




# Define constants
empty_point_wkt = Point().wkt  # "POINT EMPTY"
rows_length = 5000  # Assuming 'rows' is defined elsewhere
time_delta_size = 10000  # Assuming 'TIME_DELTA_SIZE' is defined elsewhere

# Create HDF5 file
with h5py.File('./matrix.h5', 'w') as hdf5_file:
    # Create dataset in the HDF5 file with the desired shape
    dataset = hdf5_file.create_dataset('matrix', shape=(rows_length, time_delta_size), dtype=h5py.string_dtype())
    
    # Fill the dataset with the default value
    dataset[:] = empty_point_wkt

