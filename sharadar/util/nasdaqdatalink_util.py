from io import BytesIO
from zipfile import ZipFile

import numpy as np
import pandas as pd
import nasdaqdatalink
import requests
from click import progressbar
from sharadar.util.logger import log
from six.moves.urllib.parse import urlencode

ONE_MEGABYTE = 1024 * 1024
NASDAQ_DATALINK_URL = (
    'https://data.nasdaq.com/api/v3/datatables/'
)

DATA_START_DATE = "2000-01-01" # aviod error with zipline-reloaded 

def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers['content-length'])
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data


def format_metadata_url(api_key, table_name):
    """ Build the query URL for Quandl Prices metadata.
    """
    if table_name == "SHARADAR/SEP":
        query_params = [("date.gte", DATA_START_DATE), ('api_key', api_key), ('qopts.export', 'true')]
    elif table_name == "SHARADAR/SFP":
        query_params = [("date.gte", DATA_START_DATE), ('api_key', api_key), ('qopts.export', 'true')]
    elif table_name == "SHARADAR/DAILY":
        query_params = [("date.gte", DATA_START_DATE), ('api_key', api_key), ('qopts.export', 'true')]
    elif table_name == "SHARADAR/SF1":
        query_params = [("calendardate.gte", DATA_START_DATE), ('api_key', api_key), ('qopts.export', 'true')]
    return (
            NASDAQ_DATALINK_URL + table_name + ".csv?" + urlencode(query_params)
    )


def load_data_table(file, index_col=None, parse_dates=False):
    """ Load data table from zip file provided by Quandl.
    """
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        wiki_prices = file_names.pop()
        with zip_file.open(wiki_prices) as table_file:
            data_table = pd.read_csv(table_file, index_col=index_col,
                                     parse_dates=parse_dates, na_values=['NA'])

    return data_table


def fetch_entire_table(api_key, table_name, index_col=None, parse_dates=False, retries=5):
    log.info("Start loading the entire %s dataset..." % table_name)
    for _ in range(retries):
        try:
            source_url = format_metadata_url(api_key, table_name)
            metadata = pd.read_csv(source_url)

            # Extract link from metadata and download zip file.
            table_url = metadata.loc[0, 'file.link']

            raw_file = download_with_progress(
                table_url,
                chunk_size=ONE_MEGABYTE,
                label="Downloading data from nasdaqdatalink table " + table_name
            )

            log.info("Parsing data from nasdaqdatalink table %s." % table_name)
            return load_data_table(raw_file, index_col=index_col, parse_dates=parse_dates)

        except Exception:
            log.exception("Exception raised reading nasdaqdatalink data. Retrying.")

    else:
        raise ValueError("Failed to download data from '%s' after %d attempts." % (source_url, retries))

def fetch_table_by_date(api_key, table_name, start, end=None, index_col=None):
    """
    Load data from nasdaqdatalink and correct them so that they are unadjusted.
    The index must be the date
    """

    log.info(
        "Start loading Sharadar %s price data from %s to %s..." % (table_name, start, "today" if end is None else end))
    nasdaqdatalink.ApiConfig.api_key = api_key
    df = nasdaqdatalink.get_table(table_name,
                                  date={'gte': start, 'lte': end},
                                  paginate=True)
    if index_col is not None:
        # the df['date'] dtype is already datetime64[ns]
        df.set_index(index_col, inplace=True)
    return df


def fetch_sf1_table_date(api_key, start, end=None):
    log.info("Start loading Sharadar SF1 fundamentals data from %s to %s..." % (start, "today" if end is None else end))
    nasdaqdatalink.ApiConfig.api_key = api_key
    return nasdaqdatalink.get_table('SHARADAR/SF1', dimension=['ARQ', 'ART'],
                                    lastupdated={'gte': start, 'lte': end},
                                    paginate=True)


def last_available_date():
    return nasdaqdatalink.get_table('SHARADAR/TICKERS', ticker='SPY')['lastpricedate'][0].strftime('%Y-%m-%d')

