def getPartitions(*args, **kwargs):
    from .loader import getPartitions as _get_partitions

    return _get_partitions(*args, **kwargs)


def getJSON(*args, **kwargs):
    from .loader import getJSON as _get_json

    return _get_json(*args, **kwargs)


def _get_data_dir():
    from .loader import data_dir as _data_dir

    return _data_dir

# Load into memory

def getMetadata():
    metadata = {"reportsAsOf":{},'holdingsAsOf':{}}
    for ccy in ['usd','eur']:
        [data, headers] = getJSON('partitions/reports',{"ccy":ccy})
        partitions=(data.get('result').get('partitions'))
        metadata["reportsAsOf"][ccy]=list({d["stamp"] for d in partitions})
    return metadata
    
def getShares():
    params = {}
    return getPartitions(endpoint="shares",params=params)

def getTimeseries(start='2019-01-01',end=None,ccy='eur',ids=None):
    params = {"from":start,"to":end,"ccy":ccy}
    if ids is not None:
        params["ids"] = ",".join([str(i) for i in ids])
    return getPartitions(endpoint="timeseries",params=params)

def getReports(stamp='2026-01-30',ccy='eur',ids=None):  
    periods = ",".join([
        "one-day", "one-week",
        "month-to-date", "three-month-to-date",
        "year-to-date", "one-year-to-date", "three-year"
    ])
    params = {"stamp":stamp,"ccy":ccy,"columns":"*","periods":periods}
    if ids is not None:
        params["ids"] = ",".join([str(i) for i in ids])
    return getPartitions(endpoint="reports",params=params)
    

def getHoldings(ids=None):
    params = {}
    if ids is not None:
        params["ids"] = ",".join([str(i) for i in ids])
    return getPartitions(endpoint="holdings",params=params)


def getLiquidity(start,end):
    params = {"from":start,"to":end}
    return getPartitions(endpoint="liquidity",params=params)


# Stream to disk

def downloadShares(format='parquet'):
    endpoint='shares'
    folder = endpoint
    params = {"format":format}
    getPartitions(endpoint=endpoint,folder=folder,params=params);
    pattern = _get_data_dir() / format / folder / "**/*.parquet"
    return str(pattern)

def downloadReports(stamp='2026-01-30',ccy='eur',format='parquet'):
    periods = ",".join([
        "one-day", "one-week",
        "month-to-date", "three-month-to-date",
        "year-to-date", "one-year-to-date", "three-year"
    ])
    endpoint = 'reports'
    folder = ccy+'_reports'
    params = {"stamp":stamp,"ccy":ccy,"columns":"*","periods":periods}
    getPartitions(endpoint='reports',folder=folder,params=params,format=format,partitionOrder=["stamp","mod_20"]);
    pattern = _get_data_dir() / format / folder / ("stamp="+stamp) / "**/*.parquet"
    return str(pattern)

def downloadTimeseries(start,end,ccy='eur',format='parquet'):
    endpoint = 'timeseries'
    folder = ccy+'_timeseries'
    params = {"from":start,"to":end,"ccy":ccy}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = _get_data_dir() / format / folder / "**/*.parquet"
    return str(pattern)

def downloadHoldings(format='parquet'):
    endpoint = 'holdings'
    folder = endpoint
    params = {}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = _get_data_dir() / format / folder / "**/*.parquet"
    return str(pattern)
    
def downloadLiquidity(start,end,format='parquet'):
    endpoint = 'liquidity'
    folder = endpoint
    params = {"from":start,"to":end}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = _get_data_dir() / format / folder / "**/*.parquet"
    return str(pattern)

