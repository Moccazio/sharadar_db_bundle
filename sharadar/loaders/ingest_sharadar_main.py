from sharadar.loaders.ingest_sharadar import _ingest
from exchange_calendars import get_calendar
import pandas as pd

start_date = '2022-10-28'
calendar = get_calendar('XNYS')

_ingest(start_date, calendar)
