# trackinsight-data-python

# Installation

```bash
uv init
uv add trackinsight-data-python
uv add dotenv # needed to load environment variables
uv add polars # needed to manipulate dataframes
```

## Setup the environment variables

Some variables are read from the environment.
We recommend managing environment variables in a local .env file and loading it via the `dotenv` library


```bash
# .env
TRACK_API_KEY=YOUR_KEY_HERE # your API key
TRACK_API_DL_WORKERS=10 # number of parallel threads when downloading data
TRACK_API_STORAGE=trackinsight_data # local folder to use when storing data on disk
TRACK_API_VERIFY_CERT=True # Set to False if certificate validation is not possible in you environment
```



## Usage

```python
# main.py
import polars as pl
import trackinsight_data_python as API
from dotenv import load_dotenv
load_dotenv(override=True)

ccy='usd'
metadata= API.getMetadata()
stamp = max(metadata["reportsAsOf"][ccy]) # get latest stamp for reports

reports = API.getReports(ccy=ccy,stamp=stamp) # reports is a Polars DataFrame, you can use .to_pandas() to convert it to a Pandas DataFrame

nuclear_etfs = reports.filter(pl.col('class_theme').list.join(';') =='Nuclear Energy')

ids = nuclear_etfs["share_id"].to_list()

usd_timeseries = API.getTimeseries(ids=ids,start='2024-01-01',end=None,ccy=ccy)

holdings = API.getHoldings(ids=ids)


```

```bash
uv run python main.py
```
