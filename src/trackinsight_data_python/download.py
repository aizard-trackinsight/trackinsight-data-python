from ._params import (
    build_holdings_params,
    build_liquidity_params,
    build_reports_params,
    build_shares_params,
    build_timeseries_params,
)
from .api import getMetadata
from .partitions import getPartitions,read_vars


def downloadShares(format='parquet'):
    """Download shares partitions to disk and return the output file pattern.

    Args:
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint='shares'
    folder = endpoint
    params = build_shares_params()
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);

    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("**/*."+format)

    return str(pattern)

def downloadReports(stamp=None,ccy='eur',format='parquet',periods=None):
    """Download report partitions for the given stamp and return the output pattern.

    Args:
        stamp (str | None, optional): Report valuation date in ``YYYY-MM-DD`` format.
            When ``None``, the latest available stamp for ``ccy`` is used.
        ccy (str, optional): Currency code.
        format (str, optional): File format requested from the API.
        periods (list[str] | tuple[str, ...] | None, optional): Report periods to request.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'reports'
    folder = ccy+'_reports'
    params, stamp = build_reports_params(
        stamp=stamp,
        ccy=ccy,
        periods=periods,
        metadata_loader=getMetadata,
    )
    getPartitions(endpoint='reports',folder=folder,params=params,format=format,partitionOrder=["stamp","mod_20"]);
    
    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("stamp="+stamp) / ("**/*."+format)
    
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
    params = build_timeseries_params(start=start, end=end, ccy=ccy)
    
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    
    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("**/*."+format)
    
    return str(pattern)

def downloadHoldings(format='parquet',proxy=True,level=0,extraLines=False):
    """Download holdings partitions to disk and return the output file pattern.

    Args:
        format (str, optional): File format requested from the API.
        proxy (bool, optional): Whether to include proxy holdings.
        level (int, optional): The depth at which ETFs containing other ETFs are expanded in portfolios. 0 = no expansion, 1 = expand ETFs once, 2 = expand ETFs of ETFs recursively
        extraLines (bool, optional): Whether to include special portfolio lines (????????CASH, ??DERIVATIVE, ?????NOTCASH, ?????UNKNOWN)

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'holdings'
    folder = endpoint
    
    params = build_holdings_params(proxy=proxy, level=level, extraLines=extraLines)
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    
    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("**/*."+format)
    return str(pattern)
    
def downloadLiquidity(start,end,ccy='eur',format='parquet'):
    """Download liquidity partitions for a date range and return the output pattern.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'liquidity'
    folder = ccy+"_"+endpoint
    params = build_liquidity_params(start=start, end=end, ccy=ccy)
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    
    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("**/*."+format)
    
    return str(pattern)

def downloadLiquiditySummary(start,end,ccy='eur',format='parquet'):
    """Download liquidity summary partitions for a date range and return the output pattern.

    Args:
        start (str): Start date (inclusive), in ``YYYY-MM-DD`` format.
        end (str): End date (inclusive), in ``YYYY-MM-DD`` format.
        ccy (str, optional): Currency code.
        format (str, optional): File format requested from the API.

    Returns:
        str: Glob pattern pointing to downloaded files on disk.
    """
    endpoint = 'liquidity_summary'
    folder = ccy+"_"+endpoint
    params = build_liquidity_params(start=start, end=end, ccy=ccy)
    getPartitions(endpoint=endpoint,folder=folder,params=params,format=format);
    
    [host, key, data_dir, max_workers, verify_cert] = read_vars()

    pattern = data_dir / format / folder / ("**/*."+format)
    
    return str(pattern)
