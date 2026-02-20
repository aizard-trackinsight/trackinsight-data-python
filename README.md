# trackinsight-data-python

## Install from GitHub

```bash
uv add "git+https://github.com/aizard-trackinsight/trackinsight-data-python.git"
```

## Usage

```python
import polars as pl
import trackinsight_data_python as API

# You need to add API_KEY and API_HOST to .env

ccy='usd'

metadata= API.getMetadata()
stamp = metadata["reportsAsOf"][ccy][-1] # get latest stamp for reports

reports = API.getReports(ccy=ccy,stamp=stamp) # reports is a Polars DataFrame, you can use .to_pandas() if needed

nuclear_etfs = reports.filter(pl.col('class_theme').list.join(';') =='Nuclear Energy')

ids = nuclear_etfs["share_id"].to_list();

usd_timeseries = API.getTimeseries(ids=ids,start='2024-01-01',end=None,ccy=ccy)

holdings = API.getHoldings(ids=ids)


```
