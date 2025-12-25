import sqlite3
import pandas as pd
from sharadar.util.output_dir import get_data_dir

# Connect to the prices DB
db_path = f"{get_data_dir()}/prices.sqlite"
conn = sqlite3.connect(db_path)

# Query last 5 rows for 20Y Bond (10240) and Unemployment (10440)
query = """
SELECT date, sid, close 
FROM prices 
WHERE sid IN (10240, 10440) 
ORDER BY date DESC 
LIMIT 10
"""

df = pd.read_sql_query(query, conn)
print(df)
conn.close()