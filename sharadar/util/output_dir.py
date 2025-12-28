import os
import shutil
from zipline.utils.paths import data_root

SHARADAR_BUNDLE_NAME = 'sharadar'
SHARADAR_BUNDLE_DIR = 'latest'

# Keep your path helpers as they were
def create_data_dir(name, environ=None):
    dr = data_root(environ)
    if not os.path.exists(dr):
        os.makedirs(dr)
    return os.path.join(dr, name)

def get_data_dir():
    return os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR)

def get_cache_dir():
    path = os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR, 'cache')
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def smart_cleanup_cache(limit_gb=30, target_gb=20):
    """
    Smartly manages cache size. 
    If cache > limit_gb, it deletes the OLDEST files first until size < target_gb.
    
    Default for M3 Max: 
    - Allow up to 30GB (limit_gb) 
    - Cleanup down to 20GB (target_gb) to prevent frequent cleaning.
    """
    cache_dir = get_cache_dir()
    
    # 1. Calculate total size and list all files with their mtime (modification time)
    files = []
    total_size = 0
    
    try:
        with os.scandir(cache_dir) as entries:
            for entry in entries:
                if entry.is_file():
                    stat = entry.stat()
                    total_size += stat.st_size
                    # Store tuple: (modification_time, file_path, file_size)
                    files.append((stat.st_mtime, entry.path, stat.st_size))
    except FileNotFoundError:
        return # Cache doesn't exist yet, nothing to clean

    # Convert GB to Bytes
    limit_bytes = limit_gb * 1024 * 1024 * 1024
    target_bytes = target_gb * 1024 * 1024 * 1024

    # 2. Check if cleanup is needed
    if total_size <= limit_bytes:
        return

    # 3. Sort files by oldest first (mtime)
    files.sort(key=lambda x: x[0])
    
    print(f"Cache size ({total_size / (1024**3):.2f} GB) exceeds limit ({limit_gb} GB). Cleaning up...")

    # 4. Delete oldest files until we are under the target size
    deleted_count = 0
    for mtime, filepath, size in files:
        try:
            os.remove(filepath)
            total_size -= size
            deleted_count += 1
            if total_size <= target_bytes:
                break
        except OSError:
            pass # File might be in use or already deleted

    print(f"Cleanup complete. Deleted {deleted_count} old files. New size: {total_size / (1024**3):.2f} GB")

# Retain this alias if existing code relies on it, but point it to the smart version
def clear_cache_dir():
    # Pass 0 to force a cleanup down to target, or just run standard maintenance
    smart_cleanup_cache()