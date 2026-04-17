from .partitions import getJSON,getPartitions
import polars as pl

# Functions to load data into in-memory data frames


idsLimit = 1000

def getMetadata(asDataFrame=False):
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
        metadata["reportsAsOf"][ccy]=sorted({d["stamp"] for d in partitions})
    [data, headers] = getJSON('partitions/holdings',{})
    partitions=(data.get('result').get('partitions'))
    metadata["holdingsAsOf"]["year"]=list({d["year"] for d in partitions})[0]
    metadata["holdingsAsOf"]["month"]=list({d["month"] for d in partitions})[0]
    if asDataFrame:
        return pl.DataFrame(metadata)
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
        ids (list[int] | tuple[int] | None, optional): Optional share IDs to filter.

    Returns:
        polars.DataFrame: In-memory timeseries data returned by ``getPartitions``.
    """
    
    params = {"from":start,"to":end,"ccy":ccy}

    if ids is not None and len(ids) <= idsLimit:
        params["ids"] = ",".join([str(i) for i in ids])
    data = getPartitions(endpoint="timeseries",params=params)

    if data is not None:
        if ids is not None and len(ids) > idsLimit:
            data = data.filter(pl.col("id").is_in(ids))

    return data

def getReports(stamp=None,ccy='eur',ids=None,periods=None):  
    """Load report rows for a given valuation stamp, currency, and optional IDs.

    Args:
        stamp (str, optional): Report valuation date in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        ids (list[int] | tuple[int] | None, optional): Optional share IDs to filter.
        periods (list[str] | tuple[str, ...] | None, optional): Report periods to request,
            for example ``["one-day", "one-week", "year-to-date"]``.
            When ``None``, the default report periods are requested.
    Returns:
        polars.DataFrame: In-memory report data returned by ``getPartitions``.
    """
    
    if ccy is None:
        ccy = 'eur'
        
    if stamp is None:
        stamp = max(getMetadata()["reportsAsOf"][ccy])

    if periods is None:
        periods = [
            "one-day", "one-week",
            "month-to-date", "three-month-to-date",
            "year-to-date", "one-year-to-date", "three-year"
        ]
    
    params = {"stamp":stamp,"ccy":ccy,"columns":"*","periods":",".join(periods)}

    if (ids is not None) and (len(ids) <= idsLimit):
        params["ids"] = ",".join([str(i) for i in ids])
    data = getPartitions(endpoint="reports",params=params)

    if data is not None:
        if (ids is not None) and (len(ids) > idsLimit):
            data = data.filter(pl.col("share_id").is_in(ids))

    return data

    

def getHoldings(ids=None, proxy=True, level=0, extraLines=False):
    """Load holdings rows, optionally filtered to specific IDs.

    Args:
        ids (list[int] | tuple[int] | None, optional): Optional share IDs to filter.
        proxy (bool, optional): Whether to include proxy holdings.
        level (int, optional): The depth at which ETFs containing other ETFs are expanded in portfolios. 0 = no expansion, 1 = expand ETFs once, 2 = expand ETFs of ETFs recursively
        extraLines (bool, optional): Whether to include special portfolio lines (????????CASH, ??DERIVATIVE, ?????NOTCASH, ?????UNKNOWN)
    Returns:
        polars.DataFrame: In-memory holdings data returned by ``getPartitions``.
    """
    params = { "proxy":"true" if proxy else "false", "level":level, "extraLines":"true" if extraLines else "false" }

    if (ids is not None) and (len(ids) <= idsLimit):
        params["ids"] = ",".join([str(i) for i in ids])
    data = getPartitions(endpoint="holdings",params=params)

    if data is not None:
        if (ids is not None) and (len(ids) > idsLimit):
            data = data.filter(pl.col("share_id").is_in(ids))

    return data


def getLiquidity(start,end,ccy='eur',ids=None):
    """Load liquidity rows for the provided date range.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.

    Returns:
        polars.DataFrame: In-memory liquidity data returned by ``getPartitions``.
    """
    params = {"from":start,"to":end,"ccy":ccy}

    if (ids is not None) and (len(ids) <= idsLimit):
        params["ids"] = ",".join([str(i) for i in ids])
    data = getPartitions(endpoint="liquidity",params=params)

    if data is not None:
        if (ids is not None) and (len(ids) > idsLimit):
            data = data.filter(pl.col("share_id").is_in(ids))
    return data
    

def getLiquiditySummary(start,end,ccy='eur',ids=None):
    """Load liquidity summary rows for the provided date range.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.


    Returns:
        polars.DataFrame: In-memory liquidity summary data returned by ``getPartitions``.
    """
    endpoint = 'liquidity_summary'
    params = {"from":start,"to":end,"ccy":ccy}

    if (ids is not None) and (len(ids) <= idsLimit):
        params["ids"] = ",".join([str(i) for i in ids])
    data = getPartitions(endpoint=endpoint,params=params)

    if data is not None:
        if (ids is not None) and (len(ids) > idsLimit):
            data = data.filter(pl.col("share_id").is_in(ids))
    return data