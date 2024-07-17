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

def clear_cache_dir():
    """
    Clears the cache directory used by Sharadar.
    """
    cache_dir = get_cache_dir()  # Get the path to the cache directory
    shutil.rmtree(cache_dir)  # Remove the cache directory and all its contents
    os.makedirs(cache_dir)  # Recreate the cache directory
