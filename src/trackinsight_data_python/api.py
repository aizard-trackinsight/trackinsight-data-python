from .partitions import getJSON,getPartitions


# Load into memory

def getMetadata():
    """Fetch available report partition stamps grouped by currency.

    Args:
        None.

    Returns:
        dict: Metadata dictionary with report partition stamps by currency.
    """
    metadata = {"reportsAsOf":{},'holdingsAsOf':{}}
    for ccy in ['usd','eur']:
        [data, headers] = getJSON('partitions/reports',{"ccy":ccy})
        partitions=(data.get('result').get('partitions'))
        metadata["reportsAsOf"][ccy]=list({d["stamp"] for d in partitions})
    return metadata
    
def getShares():
    """Load the full shares dataset into memory.

    Args:
        None.

    Returns:
        polars.DataFrame: In-memory shares data returned by ``getPartitions``.
    """
    params = {}
    return getPartitions(endpoint="shares",params=params)

def getTimeseries(start='2019-01-01',end=None,ccy='eur',ids=None):
    """Load timeseries rows filtered by date range, currency, and optional IDs.

    Args:
        start (str, optional): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str | None, optional): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        ids (list[int] | tuple[int] | None, optional): Optional instrument IDs to filter.

    Returns:
        polars.DataFrame: In-memory timeseries data returned by ``getPartitions``.
    """
    params = {"from":start,"to":end,"ccy":ccy}
    if ids is not None:
        params["ids"] = ",".join([str(i) for i in ids])
    return getPartitions(endpoint="timeseries",params=params)

def getReports(stamp='2026-01-30',ccy='eur',ids=None):  
    """Load report rows for a given valuation stamp, currency, and optional IDs.

    Args:
        stamp (str, optional): Report valuation date in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        ids (list[int] | tuple[int] | None, optional): Optional instrument IDs to filter.

    Returns:
        polars.DataFrame: In-memory report data returned by ``getPartitions``.
    """
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
    """Load holdings rows, optionally filtered to specific IDs.

    Args:
        ids (list[int] | tuple[int] | None, optional): Optional instrument IDs to filter.

    Returns:
        polars.DataFrame: In-memory holdings data returned by ``getPartitions``.
    """
    params = {}
    if ids is not None:
        params["ids"] = ",".join([str(i) for i in ids])
    return getPartitions(endpoint="holdings",params=params)


def getLiquidity(start,end):
    """Load liquidity rows for the provided date range.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.

    Returns:
        polars.DataFrame: In-memory liquidity data returned by ``getPartitions``.
    """
    params = {"from":start,"to":end}
    return getPartitions(endpoint="liquidity",params=params)
