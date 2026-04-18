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



## Example Usage

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


# Public API

The functions below are exported by `trackinsight_data_python` and can be imported from the package root:

```python
import trackinsight_data_python as API
```

## DataFrame Loaders

Use these functions when you want to load API data directly into memory as Polars DataFrames.

```python
metadata = API.getMetadata(asDataFrame=False)
```

Returns available report and holdings partition metadata. When `asDataFrame=True`, returns the metadata as a Polars DataFrame instead of a dictionary.

```python
shares_df = API.getShares()
```

Loads the full shares dataset into a Polars DataFrame.

```python
timeseries_df = API.getTimeseries(start='2019-01-01', end=None, ccy='eur', ids=None)
```

Loads timeseries rows for `ccy` between `start` and `end`. `ccy` must be one of the supported currencies. `start` and `end` use `YYYY-MM-DD` strings; `end=None` leaves the upper bound open. Use `ids` to restrict the result to specific share IDs.

```python
reports_df = API.getReports(stamp=None, ccy='eur', ids=None, periods=None)
```

Loads report rows for `ccy`. `ccy` must be one of the supported currencies. When `stamp=None`, the latest available report stamp for the currency is used. `periods` is a list or tuple of supported period names; when `None`, the default report periods are requested. Use `ids` to restrict the result to specific share IDs.

```python
holdings_df = API.getHoldings(ids=None, proxy=True, level=0, extraLines=False)
```

Loads holdings rows. `proxy` controls whether proxy holdings are included. `level` controls ETF look-through expansion depth. `extraLines` controls whether special portfolio lines are included. Use `ids` to restrict the result to specific share IDs.

```python
liquidity_df = API.getLiquidity(start, end, ccy='eur', ids=None)
```

Loads liquidity rows for `ccy` between `start` and `end`. `ccy` must be one of the supported currencies. Dates use `YYYY-MM-DD` strings. Use `ids` to restrict the result to specific share IDs.

```python
liquidity_summary_df = API.getLiquiditySummary(start, end, ccy='eur', ids=None)
```

Loads liquidity summary rows for `ccy` between `start` and `end`. `ccy` must be one of the supported currencies. Dates use `YYYY-MM-DD` strings. Use `ids` to restrict the result to specific share IDs.

## Downloaders

Use these functions when you want to download API data to local files and receive a glob pattern pointing to the downloaded dataset. Files are written in a Hive-partitioned folder layout, which is well suited for query engines such as DuckDB and PyArrow-based tools.

```python
API.downloadShares(format='parquet')
```

Downloads the shares dataset to disk and returns a glob pattern for the downloaded files. `format` must be one of the supported formats and defaults to `parquet`.

```python
API.downloadReports(stamp=None, ccy='eur', format='parquet', periods=None)
```

Downloads report rows for `ccy` and returns a glob pattern for the downloaded files. `ccy` must be one of the supported currencies. When `stamp=None`, the latest available report stamp for the currency is used. `periods` is a list or tuple of supported period names; when `None`, the default report periods are requested. `format` must be one of the supported formats and defaults to `parquet`.

```python
API.downloadTimeseries(start, end, ccy='eur', format='parquet')
```

Downloads timeseries rows for `ccy` between `start` and `end`, then returns a glob pattern for the downloaded files. `ccy` must be one of the supported currencies. Dates use `YYYY-MM-DD` strings. `format` must be one of the supported formats and defaults to `parquet`.

```python
API.downloadHoldings(format='parquet', proxy=True, level=0, extraLines=False)
```

Downloads holdings rows and returns a glob pattern for the downloaded files. `proxy`, `level`, and `extraLines` have the same behavior as `getHoldings()`. `format` must be one of the supported formats and defaults to `parquet`.

```python
API.downloadLiquidity(start, end, ccy='eur', format='parquet')
```

Downloads liquidity rows for `ccy` between `start` and `end`, then returns a glob pattern for the downloaded files. `ccy` must be one of the supported currencies. Dates use `YYYY-MM-DD` strings. `format` must be one of the supported formats and defaults to `parquet`.

```python
API.downloadLiquiditySummary(start, end, ccy='eur', format='parquet')
```

Downloads liquidity summary rows for `ccy` between `start` and `end`, then returns a glob pattern for the downloaded files. `ccy` must be one of the supported currencies. Dates use `YYYY-MM-DD` strings. `format` must be one of the supported formats and defaults to `parquet`.

## Supported Values

- `ccy`: `eur`, `usd`
- `format`: `parquet`, `json`, `csv`
- `periods`: `one-day`, `one-week`, `week-to-date`, `one-month`, `month-to-date`, `three-month`, `three-month-to-date`, `six-month`, `six-month-to-date`, `one-year`, `year-to-date`, `one-year-to-date`, `three-year`, `three-year-to-date`
