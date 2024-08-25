import os
import sqlite3
import sys
from sharadar.util.output_dir import get_data_dir

# Define the path to the database
prices_dbpath = os.path.join(get_data_dir(), "prices.sqlite")

# Connect to the database
prices_db = sqlite3.connect(prices_dbpath, isolation_level=None)
prices_db_cursor = prices_db.cursor()

# Begin a transaction
prices_db_cursor.execute("begin")

try:
    # Delete rows where the date is before 2000-01-03
    prices_db_cursor.execute("DELETE FROM prices WHERE date < '2000-01-03'")
    
    # Commit the transaction
    prices_db_cursor.execute("commit")

    # Optionally, you can vacuum the database to reclaim space
    prices_db_cursor.execute("VACUUM")
except:
    print("failed: " + str(sys.exc_info()[0]))
    prices_db_cursor.execute("rollback")

# Close the database connection
prices_db.close()