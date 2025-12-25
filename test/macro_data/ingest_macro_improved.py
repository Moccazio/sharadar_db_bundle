"""
Macro Data Ingestion Script (Improved)

- Robust error handling
- Logging instead of print
- Configurable via CLI
- Modularized functions
- Type annotations
- Docstrings for clarity
"""
import os
import sys
import logging
from os import environ as env
from typing import Optional
from pandas.tseries.offsets import DateOffset
import pandas as pd
import pandas_datareader.data as pdr
from sharadar.util.output_dir import get_data_dir as get_output_dir
from sharadar.loaders.constant import METADATA_HEADERS, EXCHANGE_DF
from zipline.utils.calendar_utils import get_calendar
from sharadar.util.nasdaqdatalink_util import last_available_date

import nasdaqdatalink

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Config ---
DEFAULT_START_DATE = '1997-12-31'
NASDAQ_API_KEY_ENV = "NASDAQ_API_KEY"

# --- Utility Functions ---
def trading_date(date, cal) -> pd.Timestamp:
    """Return the same date if a trading session or the next valid one."""
    if isinstance(date, str):
        date = pd.Timestamp(date)
    if date.tz is not None:
        date = date.tz_localize(None)
    date = date.normalize()
    if not cal.is_session(date):
        date = cal.next_close(date)
        date = date.tz_localize(None).normalize()
    return date

def _add_macro_def(df: pd.DataFrame, sid: int, start_date: pd.Timestamp, end_date: pd.Timestamp, ticker: str, asset_name: str) -> None:
    """Add macro asset definition to DataFrame."""
    auto_close_date = end_date + pd.Timedelta(days=1)
    exchange = 'MACRO'
    df.loc[sid] = (ticker, asset_name, start_date, end_date, start_date, auto_close_date, exchange)

def _to_prices_df(df: pd.DataFrame, sid: int) -> pd.DataFrame:
    df.index = df.index.tz_localize(None)
    df['sid'] = sid
    df.set_index('sid', append=True, inplace=True)
    df = _append_ohlc(df)
    return df

def _append_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    df.index.names = ['date', 'sid']
    df.columns = ['open']
    df['high'] = df['low'] = df['close'] = df['open']
    df['volume'] = 100.0
    return df

def utc(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, utc=False)

# --- DataFrame Creation ---
def create_macro_equities_df() -> pd.DataFrame:
    """Create DataFrame with macro asset definitions."""
    end_date = utc(last_available_date())
    df = pd.DataFrame(columns=METADATA_HEADERS)
    _add_macro_def(df, 10003, utc('1990-01-02'), end_date, 'TR3M', 'US Treasury Bill 3 MO')
    _add_macro_def(df, 10006, utc('1990-01-02'), end_date, 'TR6M', 'US Treasury Bill 6 MO')
    _add_macro_def(df, 10012, utc('1990-01-02'), end_date, 'TR1Y', 'US Treasury Bond 1 YR')
    _add_macro_def(df, 10024, utc('1990-01-02'), end_date, 'TR2Y', 'US Treasury Bond 2 YR')
    _add_macro_def(df, 10036, utc('1990-01-02'), end_date, 'TR3Y', 'US Treasury Bond 3 YR')
    _add_macro_def(df, 10060, utc('1990-01-02'), end_date, 'TR5Y', 'US Treasury Bond 5 YR')
    _add_macro_def(df, 10084, utc('1990-01-02'), end_date, 'TR7Y', 'US Treasury Bond 7 YR')
    _add_macro_def(df, 10120, utc('1990-01-02'), end_date, 'TR10Y', 'US Treasury Bond 10 YR')
    _add_macro_def(df, 10240, utc('1990-01-02'), end_date, 'TR20Y', 'US Treasury Bond 20 YR')
    _add_macro_def(df, 10400, utc('1996-12-31'), end_date, 'CBOND', 'US Corporate Bond Yield')
    _add_macro_def(df, 10410, utc('1990-01-02'), end_date, 'INDPRO', 'Industrial Production Index')
    _add_macro_def(df, 10420, utc('1990-01-02'), end_date, 'INDPROPCT', 'Industrial Production Montly % Change')
    _add_macro_def(df, 10440, utc('1990-01-02'), end_date, 'UNRATE', 'Civilian Unemployment Rate')
    _add_macro_def(df, 10450, utc('1990-01-02'), end_date, 'RATEINF', 'US Inflation Rates YoY')
    return df

def create_macro_prices_df(start_str: str, calendar) -> pd.DataFrame:
    """Fetch and format macro price data for all assets."""
    start = pd.to_datetime(start_str)
    start = trading_date(start, calendar)
    end = pd.to_datetime(last_available_date())
    if start is not None and start > end:
        start = end
    m_start = start - DateOffset(months=14)
    try:
        # Interest Rates (T-bills and T-bonds)
        # FIX: Removed duplicate 'DGS1', added 'DGS2', added 'DGS20'
        tickers = ['DTB3', 'DTB6', 'DGS1', 'DGS2', 'DGS3', 'DGS5', 'DGS7', 'DGS10', 'DGS20']
    
        # Matching SIDs: 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y
        sids = [10003, 10006, 10012, 10024, 10036, 10060, 10084, 10120, 10240]
        tres_df = pdr.DataReader(tickers, 'fred', start, end)
        tres_df.columns = sids
        tres_df.index = tres_df.index.tz_localize(None)
        sessions = calendar.sessions_in_range(start, end)
        tres_df = tres_df.reindex(sessions).ffill().dropna()
        prices = tres_df.unstack().to_frame().swaplevel()
        prices = _append_ohlc(prices)
        # Corporate Bond Yield
        corp_bond_df = _to_prices_df(pdr.DataReader(['BAMLC0A0CMEY'], 'fred', start, end).ffill(), 10400)
        prices = pd.concat([prices, corp_bond_df])
        # Industrial Production Index
        indpro_df = pdr.DataReader(['INDPRO'], 'fred', m_start, end)\
            .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
        prices = pd.concat([prices, _to_prices_df(indpro_df, 10410)])
        # Industrial Production % Change
        indpro_p_df = pdr.DataReader(['INDPRO'], 'fred', m_start, end).pct_change()\
            .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
        prices = pd.concat([prices, _to_prices_df(indpro_p_df, 10420)])
        # Unemployment Rate
        unrate_df = pdr.DataReader(['UNRATE'], 'fred', m_start, end)\
            .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
        prices = pd.concat([prices, _to_prices_df(unrate_df, 10440)])
        # Inflation Rate (YoY)
        # ... (previous code)
        inf_df = (pdr.DataReader(['CPIAUCNS'], 'fred', m_start, end).pct_change(periods=12) * 100.00).round(2)\
            .reindex(pd.date_range(start=m_start, end=end), method='ffill').loc[pd.date_range(start, end)]
        prices = pd.concat([prices, _to_prices_df(inf_df, 10450)])
        # FIX: Drop any rows containing NaNs to prevent SQLite errors
        prices.dropna(inplace=True)
        return prices.sort_index()
    except Exception as e:
        logger.error(f"Error fetching macro data: {e}")
        raise

# --- Ingestion Logic ---
def ingest(start: str, calendar) -> int:
    """Ingest macro data into SQLite databases."""
    from sharadar.pipeline.engine import load_sharadar_bundle
    from zipline.assets import ASSET_DB_VERSION
    from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter
    from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter
    macro_equities_df = create_macro_equities_df()
    macro_prices_df = create_macro_prices_df(start, calendar)
    output_dir = get_output_dir()
    asset_dbpath = os.path.join(output_dir, f"assets-{ASSET_DB_VERSION}.sqlite")
    asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
    asset_db_writer.write(equities=macro_equities_df, exchanges=EXCHANGE_DF)
    prices_dbpath = os.path.join(output_dir, "prices.sqlite")
    sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
    sql_daily_bar_writer.write(macro_prices_df)
    return macro_prices_df.shape[0]

# --- Main CLI ---
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ingest macroeconomic data for Zipline.")
    parser.add_argument('--start', type=str, default=DEFAULT_START_DATE, help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()
    nasdaq_api_key = env.get(NASDAQ_API_KEY_ENV)
    if not nasdaq_api_key:
        logger.error(f"Environment variable {NASDAQ_API_KEY_ENV} not set.")
        sys.exit(1)
    nasdaqdatalink.ApiConfig.api_key = nasdaq_api_key
    calendar = get_calendar('XNYS')
    start = trading_date(args.start, calendar)
    logger.info(f"Adding macro data from {start.date()}...")
    try:
        n = ingest(str(start.date()), calendar)
        logger.info(f"Inserted/updated {n} entries.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
