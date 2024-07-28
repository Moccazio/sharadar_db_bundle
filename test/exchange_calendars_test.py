import pandas as pd
from zipline.utils.calendar_utils import get_calendar 

trading_calendar = get_calendar('XNYS')
start = pd.Timestamp.utcnow().normalize()
print(start)
trading_calendar.is_session(start.date())
start = trading_calendar.next_open(start)
print(start)