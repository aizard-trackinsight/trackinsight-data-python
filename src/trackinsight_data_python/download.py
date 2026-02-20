from .partitions import getJSON,getPartitions,data_dir


def downloadShares(format='parquet'):
    """Download shares partitions to disk and return the output file pattern.

    Args:
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint='shares'
    folder = endpoint
    params = {"format":format}
    getPartitions(endpoint=endpoint,folder=folder,params=params);
    pattern = data_dir / format / folder / "**/*.parquet"
    return str(pattern)

def downloadReports(stamp='2026-01-30',ccy='eur',format='parquet'):
    """Download report partitions for the given stamp and return the output pattern.

    Args:
        stamp (str, optional): Report valuation date in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    periods = ",".join([
        "one-day", "one-week",
        "month-to-date", "three-month-to-date",
        "year-to-date", "one-year-to-date", "three-year"
    ])
    endpoint = 'reports'
    folder = ccy+'_reports'
    params = {"stamp":stamp,"ccy":ccy,"columns":"*","periods":periods}
    getPartitions(endpoint='reports',folder=folder,params=params,format=format,partitionOrder=["stamp","mod_20"]);
    pattern = data_dir / format / folder / ("stamp="+stamp) / "**/*.parquet"
    return str(pattern)

def downloadTimeseries(start,end,ccy='eur',format='parquet'):
    """Download timeseries partitions for a date range and return the output pattern.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'timeseries'
    folder = ccy+'_timeseries'
    params = {"from":start,"to":end,"ccy":ccy}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = data_dir / format / folder / "**/*.parquet"
    return str(pattern)

def downloadHoldings(format='parquet'):
    """Download holdings partitions to disk and return the output file pattern.

    Args:
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'holdings'
    folder = endpoint
    params = {}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = data_dir / format / folder / "**/*.parquet"
    return str(pattern)
    
def downloadLiquidity(start,end,format='parquet'):
    """Download liquidity partitions for a date range and return the output pattern.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'liquidity'
    folder = endpoint
    params = {"from":start,"to":end}
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    pattern = data_dir / format / folder / "**/*.parquet"
    return str(pattern)
