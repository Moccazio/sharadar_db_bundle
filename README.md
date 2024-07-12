Sqlite based zipline bundle for the Sharadar datasets SEP, SFP and SF1.

Unlike the standard zipline bundles, it allows incremental updates, because sql tables are used instead of bcolz.

Make sure you can access Nasdaq-Data-Link, and you have a Nasdaq api key. I have set my Nasdaq api key as an environment variable.

>export NASDAQ_API_KEY="your API key"

>conda env config vars set NASDAQ_API_KEY="your API key"  

Clone or download the code and install it using:
>git clone https://github.com/Moccazio/sharadar_db_bundle

>cd sharadar_db_bundle

>pip install -r requirements.txt

>python setup.py install

For installation directly into the environment, use pip:
>pip install git+https://github.com/Moccazio/sharadar_db_bundle

Create a folder for storing the log files open terminal and run:
>mkdir ~/log

For zipline in order to build the cython files run:
>python setup.py build_ext --inplace

Add this code to your ~/.zipline/extension.py:
```python
from zipline.data import bundles
from zipline.finance import metrics
from sharadar.loaders.ingest_sharadar import from_nasdaqdatalink
from sharadar.util.metric_daily import default_daily

bundles.register("sharadar", from_nasdaqdatalink(), create_writers=False)
metrics.register('default_daily', default_daily)
```

The new entry point is **sharadar-zipline** (it replaces *zipline*).

For example to ingest data use:
> sharadar-zipline ingest

To ingest price and fundamental data every day at 21:30 using cron
> 30 21 * * *	cd $HOME/zipline/lib/python3.6/site-packages/sharadar_db_bundle && $HOME/zipline/bin/python sharadar/__main__.py ingest > $HOME/log/sharadar-zipline-cron.log 2>&1

To run an algorithm
> sharadar-zipline -f algo.py -s 2017-01-01 -e 2020-01-01


To start a notebook 
> conda activate py310

> jupyter notebook


Sharadar Fundamentals could be use as follows:
```python
from zipline.pipeline import Pipeline
import pandas as pd
from sharadar.pipeline.factors import (
    MarketCap,
    EV,
    Fundamentals
)
from sharadar.pipeline.engine import symbol, symbols, make_pipeline_engine
from zipline.pipeline.filters import StaticAssets

pipe = Pipeline(columns={
    'mkt_cap': MarketCap(),
    'ev': EV(),
    'debt': Fundamentals(field='debtusd_arq'),
    'cash': Fundamentals(field='cashnequsd_arq')
},
screen = StaticAssets(symbols(['IBM', 'F', 'AAPL']))
)
spe = make_pipeline_engine()

pipe_date = pd.to_datetime('2020-02-03', utc=False)

stocks = spe.run_pipeline(pipe, pipe_date)
stocks
```
