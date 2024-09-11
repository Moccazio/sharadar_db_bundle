import os
import shutil
from zipline.utils.paths import data_root

SHARADAR_BUNDLE_NAME = 'sharadar'
SHARADAR_BUNDLE_DIR = 'latest'

def create_data_dir(name, environ=None):
    """
    Returns a handle to data file.
    Creates containing directory, if needed.
    """
    dr = data_root(environ)

    if not os.path.exists(dr):
        os.makedirs(dr)

    return os.path.join(dr, name)

def get_data_dir():
    return os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR)

def get_cache_dir():
    return os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR, 'cache')

def get_directory_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def clear_cache_dir():
    """
    Clears the cache directory used by Sharadar if it exceeds 10GB.
    """
    cache_dir = get_cache_dir()  # Get the path to the cache directory
    cache_size = get_directory_size(cache_dir)  # Calculate the size of the cache directory

    # Check if the cache directory size exceeds 10GB (10 * 1024 * 1024 * 1024 bytes)
    if cache_size > 10 * 1024 * 1024 * 1024:
        shutil.rmtree(cache_dir)  # Remove the cache directory and all its contents
        os.makedirs(cache_dir)  # Recreate the cache directory