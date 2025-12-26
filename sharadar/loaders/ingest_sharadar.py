from os import environ as env
import os
import pandas as pd
import numpy as np
import nasdaqdatalink
import gc
import sqlite3
import traceback
from contextlib import closing
from pathlib import Path

# Zipline & Sharadar Imports
from zipline.utils.calendar_utils import get_calendar
from sharadar.util.output_dir import get_data_dir
from sharadar.util.output_dir import get_cache_dir
from sharadar.util.nasdaqdatalink_util import fetch_entire_table, fetch_table_by_date, fetch_sf1_table_date
from sharadar.util.nasdaqdatalink_util import last_available_date
from sharadar.util.equity_supplementary_util import lookup_sid
from sharadar.util.equity_supplementary_util import insert_asset_info, insert_fundamentals, insert_daily_metrics
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter, SQLiteDailyBarReader, SQLiteDailyAdjustmentWriter
from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter, SQLiteAssetFinder
from zipline.assets import AssetFinder, AssetDBWriter
from zipline.assets import ASSET_DB_VERSION
from zipline.utils.cli import maybe_show_progress
from sharadar.util.logger import log
from sharadar.loaders.constant import EXCHANGE_DF, OLDEST_DATE_SEP, METADATA_HEADERS

# ==========================================
# OPTIMIZATION: GLOBAL SQLITE PATCH (M3 MAX)
# ==========================================
original_connect = sqlite3.connect

def fast_connect(*args, **kwargs):
    conn = original_connect(*args, **kwargs)
    try:
        # WAL Mode: Writes to a log file first (Huge speedup on Mac)
        conn.execute("PRAGMA journal_mode = WAL;")
        # NORMAL Sync: Trusts the OS buffer (Removes disk latency)
        conn.execute("PRAGMA synchronous = NORMAL;")
        # CACHE UPDATE: 4GB RAM Cache (-4000000 pages)
        # if 4GB is too much, reduce to -2000000 (2GB) or -1000000 (1GB)
        # Allows SQLite to keep huge chunks of the DB in RAM while processing.
        conn.execute("PRAGMA cache_size = -4000000;") 
        # Temp Store: Operations happen in RAM
        conn.execute("PRAGMA temp_store = MEMORY;")
    except Exception:
        pass
    return conn

# Apply patch globally
sqlite3.connect = fast_connect

# Global API Key
nasdaqdatalink.ApiConfig.api_key = env.get("NASDAQ_API_KEY")

def process_data_table(df):
    """ Vectorized cleanup of price data. """
    m = df['closeunadj'] / df['close']
    df['open'] *= m
    df['high'] *= m
    df['low'] *= m
    df['close'] = df['closeunadj']
    df['volume'] /= m
    df.drop(columns=['closeunadj', 'closeadj', 'lastupdated'], inplace=True, errors='ignore')
    df.fillna(0, inplace=True)
    return df

def must_fetch_entire_table(date):
    if pd.isnull(date):
        return True
    return pd.Timestamp(date) <= OLDEST_DATE_SEP

def fetch_data(start, end):
    """ Fetch SEP and SFP. Optimized for memory. """
    api_key = env.get("NASDAQ_API_KEY")
    if must_fetch_entire_table(start):
        log.info("Fetching ENTIRE SEP and SFP tables (Bulk)...")
        df_sep = fetch_entire_table(api_key, "SHARADAR/SEP", parse_dates=['date'])
        df_sfp = fetch_entire_table(api_key, "SHARADAR/SFP", parse_dates=['date'])
    else:
        log.info(f"Fetching incremental data from {start}...")
        df_sep = fetch_table_by_date(api_key, 'SHARADAR/SEP', start, end)
        df_sfp = fetch_table_by_date(api_key, 'SHARADAR/SFP', start, end)
    
    df = pd.concat([df_sep, df_sfp], copy=False)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    del df_sep, df_sfp
    gc.collect()
    return df

def get_data(sharadar_metadata_df, related_tickers, start=None, end=None):
    """ Optimized: Vectorized Ticker -> SID lookup. """
    df = fetch_data(start, end)
    log.info(f"Processing {len(df)} price rows...")
    log.info("Adding SIDs to all stocks (Vectorized)...")
    # Vectorized Merge Lookup
    meta_reset = sharadar_metadata_df.reset_index()
    if 'permaticker' in meta_reset.columns:
        mapping = meta_reset[['ticker', 'permaticker']].copy()
        mapping.rename(columns={'permaticker': 'sid'}, inplace=True)
    else:
        log.warning("Metadata structure unclear, using slow lookup...")
        df['sid'] = df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
        df = df[df['sid'] != -1]
        df.set_index(['date', 'sid'], inplace=True)
        return process_data_table(df).sort_index()
    # Merge
    df = df.merge(mapping, on='ticker', how='left')
    df['sid'] = df['sid'].fillna(-1).astype(int)
    # Drop unknown
    df = df[df['sid'] != -1]
    df.set_index(['date', 'sid'], inplace=True)
    # Final Processing
    df = process_data_table(df)
    # Cleanup
    del mapping, meta_reset
    gc.collect()
    # Return sorted
    return df.sort_index()

def create_equities_df(df, tickers, sessions, sharadar_metadata_df, show_progress):
    """
    Fixed & Optimized: Vectorized metadata creation ensuring correct index alignment.
    """
    log.info("Generating Equities Metadata (Vectorized)...")
    # Get all SIDs present in the price data
    present_sids = df.index.get_level_values('sid').unique()
    # Filter metadata to only include these SIDs
    # Note: sharadar_metadata_df is indexed by 'ticker'
    mask = sharadar_metadata_df['permaticker'].isin(present_sids)
    subset = sharadar_metadata_df[mask].copy()
    # Create the Equities DataFrame indexed by SID (permaticker)
    # We must reset index to access 'ticker' column, then set index to 'permaticker'
    subset_reset = subset.reset_index() # columns: ticker, permaticker, ...
    equities_df = pd.DataFrame(index=subset_reset['permaticker'])
    equities_df.index.name = 'sid'
    # FIX: Map data correctly using the shared index (permaticker)
    equities_df['symbol'] = subset_reset['ticker'].values
    equities_df['ticker'] = subset_reset['ticker'].values
    equities_df['asset_name'] = subset_reset['name'].values
    equities_df['start_date'] = subset_reset['firstpricedate'].values
    equities_df['end_date'] = subset_reset['lastpricedate'].values
    equities_df['first_traded'] = subset_reset['firstpricedate'].values
    # Add one day to end_date for auto_close_date
    equities_df['auto_close_date'] = (subset_reset['lastpricedate'] + pd.Timedelta(days=1)).values
    # Handle Exchanges
    equities_df['exchange'] = subset_reset['exchange'].fillna('OTC').replace('None', 'OTC').values
    # Filter for required Zipline headers
    equities_df = equities_df[METADATA_HEADERS]
    return equities_df

# --- Helpers ---
def create_dividends_df(sharadar_metadata_df, related_tickers, existing_tickers, start):
    dividends_df = nasdaqdatalink.get_table('SHARADAR/ACTIONS', date={'gte': start}, action=['dividend', 'spinoffdividend'], paginate=True)
    tickers_dividends = dividends_df['ticker'].unique()
    tickers_intersect = set(existing_tickers).intersection(tickers_dividends)
    dividends_df = dividends_df.loc[dividends_df['ticker'].isin(tickers_intersect)]
    dividends_df = dividends_df.rename(columns={'value': 'amount'})
    dividends_df['sid'] = dividends_df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    dividends_df.index = dividends_df['date']
    dividends_df['record_date'] = dividends_df['declared_date'] = dividends_df['pay_date'] = dividends_df['ex_date'] = dividends_df.index
    dividends_df.drop(['action', 'date', 'name', 'contraticker', 'contraname', 'ticker'], axis=1, inplace=True)
    return dividends_df.sort_index()

def create_splits_df(sharadar_metadata_df, related_tickers, existing_tickers, start):
    splits_df = nasdaqdatalink.get_table('SHARADAR/ACTIONS', date={'gte': start}, action=['split'], paginate=True)
    tickers_splits = splits_df['ticker'].unique()
    tickers_intersect = set(existing_tickers).intersection(tickers_splits)
    splits_df = splits_df.loc[splits_df['ticker'].isin(tickers_intersect)]
    splits_df['value'] = 1.0 / splits_df['value']
    splits_df.rename(columns={'value': 'ratio', 'date': 'effective_date'}, inplace=True, copy=False)
    splits_df['ratio'] = splits_df['ratio'].astype(float)
    splits_df['sid'] = splits_df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    splits_df.drop(['action', 'name', 'contraticker', 'contraname', 'ticker'], axis=1, inplace=True)
    splits_df.index = splits_df['effective_date']
    return splits_df.sort_index()

def trading_date(date, cal):
    if isinstance(date, str): date = pd.Timestamp(date)
    if date.tz is not None: date = date.tz_localize(None)
    date = date.normalize()
    if not cal.is_session(date):
        date = cal.next_close(date).tz_localize(None).normalize()
    return date

def create_metadata():
    sharadar_metadata_df = nasdaqdatalink.get_table('SHARADAR/TICKERS', table=['SFP', 'SEP'], paginate=True)
    sharadar_metadata_df.set_index('ticker', inplace=True)
    related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
    related_tickers = ' ' + related_tickers.astype(str) + ' '
    return related_tickers, sharadar_metadata_df

def _ingest(start, calendar=get_calendar('XNYS'), output_dir=get_data_dir(), universe=False, sanity_check=True, use_last_available_dt=True):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(get_cache_dir(), exist_ok=True)
    log.info("Start ingesting SEP, SFP and SF1 data into %s ..." % output_dir)
    gc.collect()
    start_session = trading_date(start, calendar)
    end_session = pd.Timestamp(last_available_date())
    sessions = calendar.sessions_in_range(start_session, end_session)
    prices_dbpath = os.path.join(output_dir, "prices.sqlite")
    # Determine Start Fetch Date
    start_fetch_date = sessions[0].strftime('%Y-%m-%d')
    if use_last_available_dt and os.path.exists(prices_dbpath):
        try:
            start_fetch_date = SQLiteDailyBarReader(prices_dbpath).last_available_dt.strftime('%Y-%m-%d')
        except: pass
    log.info("Start fetch date: %s" % start_fetch_date)
    log.info("Start loading sharadar metadata...")
    related_tickers, sharadar_metadata_df = create_metadata()
    # Get Prices (Optimized)
    prices_df = get_data(sharadar_metadata_df, related_tickers, start_fetch_date)
    if not prices_df.empty:
        log.info(f"Price data retrieved ({len(prices_df)} rows). Sorting...")
        prices_df.sort_index(inplace=True)
    else:
        log.info("No price data retrieved.")
    tickers = prices_df['ticker'].unique()
    # Create Equities (Fixed & Optimized)
    equities_df = create_equities_df(prices_df, tickers, sessions, sharadar_metadata_df, show_progress=True)
    log.info("Writing Assets DB...")
    asset_dbpath = os.path.join(output_dir, ("assets-%d.sqlite" % ASSET_DB_VERSION))
    asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
    asset_db_writer.write(equities=equities_df, exchanges=EXCHANGE_DF)
    log.info(f"Writing Pricing Data to {prices_dbpath}...")
    sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
    sql_daily_bar_writer.write(prices_df)
    del prices_df
    gc.collect()
    log.info("Creating Dividends & Splits...")
    dividends_df = create_dividends_df(sharadar_metadata_df, related_tickers, tickers, start_fetch_date)
    splits_df = create_splits_df(sharadar_metadata_df, related_tickers, tickers, start_fetch_date)
    # Write Adjustments
    adjustment_dbpath = os.path.join(output_dir, "adjustments.sqlite")
    sql_daily_bar_reader = SQLiteDailyBarReader(prices_dbpath)
    asset_db_reader = SQLiteAssetFinder(asset_dbpath)
    adjustment_writer = SQLiteDailyAdjustmentWriter(adjustment_dbpath, sql_daily_bar_reader, asset_db_reader, sessions)
    adjustment_writer.write(splits=splits_df, dividends=dividends_df)
    log.info("Processing Fundamentals (SF1)...")
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_asset_info(sharadar_metadata_df, cursor)
    # Fundamentals
    start_date_fundamentals = asset_db_reader.last_available_fundamentals_dt
    if must_fetch_entire_table(start_date_fundamentals):
        sf1_df = fetch_entire_table(env["NASDAQ_API_KEY"], "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])
    else:
        sf1_df = fetch_sf1_table_date(env["NASDAQ_API_KEY"], start_date_fundamentals)
    # Insert Fundamentals
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_fundamentals(sharadar_metadata_df, sf1_df, cursor, show_progress=True)
    del sf1_df; gc.collect()
    # Daily Metrics
    log.info("Processing Daily Metrics...")
    start_date_metrics = asset_db_reader.last_available_daily_metrics_dt
    if must_fetch_entire_table(start_date_metrics):
        daily_df = fetch_entire_table(env["NASDAQ_API_KEY"], "SHARADAR/DAILY", parse_dates=['date'])
    else:
        daily_df = fetch_table_by_date(env["NASDAQ_API_KEY"], 'SHARADAR/DAILY', start_date_metrics)
    # Insert Daily Metrics
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True)
    del daily_df; gc.collect()
    if universe:
        from sharadar.pipeline.universes import update_universe, TRADABLE_STOCKS_US, base_universe, context
        screen = base_universe(context())
        update_universe(TRADABLE_STOCKS_US, screen)
    # Sanity Check
    Path(os.path.join(output_dir, "ok")).touch()
    log.info("Ingest finished Successfully!")

def from_nasdaqdatalink():
    def ingest(environ, asset_db_writer, minute_bar_writer, daily_bar_writer, adjustment_writer, calendar,
               start_date, end_date, cache, show_progress, output_dir):
        try:
            if os.path.exists(output_dir) and not os.listdir(output_dir): os.rmdir(output_dir)
        except: pass
        try:
            _ingest(start_date, calendar)
        except Exception:
            log.error(traceback.format_exc())
    return ingest