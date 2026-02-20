import polars as pl
import os
import shutil
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO
from urllib.parse import urlencode
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

load_dotenv()
host = os.getenv("API_HOST")
key = os.getenv("API_KEY")
data_dir = Path(os.getenv("API_STORAGE"))
data_dir.mkdir(parents=True,exist_ok=True)

def inline_print(msg):
    """Print a message in place on the current terminal line.

    Args:
        msg (str): Message to render in the terminal.

    Returns:
        None: This function only writes to stdout.
    """
    print(f"\r{msg}", end="", flush=True)

def getURL(endpoint,params):
    """Build an API URL from an endpoint and query parameters.

    Args:
        endpoint (str): API endpoint path relative to ``API_HOST``.
        params (dict | None): Query parameters. Keys with ``None`` values are skipped.

    Returns:
        str: Fully qualified request URL.
    """
    qparams =""
    if params is not None:
        qparams = "&"+urlencode({k: v for k, v in params.items() if v is not None})

    url = host+"/"+endpoint+"?"+qparams
    return url

def getJSON(endpoint,params=""):
    """Execute a JSON request and return payload and response headers.

    Args:
        endpoint (str): API endpoint path relative to ``API_HOST``.
        params (dict | str, optional): Query parameters passed to ``getURL``.

    Returns:
        list: Two-item list ``[data, headers]`` from the HTTP response.
    """
    url = getURL(endpoint,params,)
    response = requests.get(url,headers={"X-API-KEY":key})
    response.raise_for_status()   # raises error if the request failed
    data = response.json()
    return [data, response.headers]


def getPartition(endpoint,partition_params,folder=None,partitionPath="",format='parquet'):
    """Fetch one partition either to memory or to disk.

    Args:
        endpoint (str): Dataset endpoint name.
        partition_params (dict): Partition-specific query parameters.
        folder (str | None, optional): Output folder relative to ``API_STORAGE``.
            When ``None``, data is returned in memory.
        partitionPath (str, optional): Nested subpath used for partitioned output.
        format (str, optional): Response format (for example ``parquet`` or ``json``).

    Returns:
        polars.DataFrame | None: In-memory data when ``folder`` is ``None``; otherwise ``None``.
    """
    
    data_dir = Path(os.getenv("API_STORAGE"))
    
    if folder is not None: # When writing to disk
        output_folder = data_dir / folder / partitionPath
        output_folder.mkdir(parents=True,exist_ok=True)
        output_filepath = output_folder / ("data."+format)
        
    url = getURL('data/'+endpoint,partition_params)
    
    with requests.get(url,headers={"X-API-KEY":key},stream=(folder is not None), timeout=60) as r:
        if r.status_code == 500:
            print(r.text)
        r.raise_for_status()
        
        if format=='json':
            response = r.json()
            if response.get("error") is not None:
                raise ValueError(str(response["error"]))
            else:
                if folder is not None:
                    with open(output_filepath, "w") as f:
                        json.dump(response.get("result"), f, indent=2)
                else:
                    return pl.DataFrame(response.get("result"))  
        else:
            if folder is not None: # When writing to disk
                with open(output_filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filters out keep-alive chunks
                            f.write(chunk)
            else: # When keeping in memory
                return pl.read_parquet(BytesIO(r.content))


def getPartitions(endpoint,folder=None,params={},format="parquet",partitionOrder=None):
    """Fetch all partitions for a dataset in parallel.

    Args:
        endpoint (str): Dataset endpoint name.
        folder (str | None, optional): Output folder name. When ``None``, data is kept in memory.
        params (dict, optional): Base query parameters shared across partitions.
        format (str, optional): Response format requested from the API.
        partitionOrder (list[str] | None, optional): Explicit key order used to build partition paths.

    Returns:
        polars.DataFrame | list: Concatenated DataFrame when ``folder`` is ``None``;
            otherwise a list of per-partition results.
    """
    [data, headers] = getJSON('partitions/'+endpoint, params)
    transactionId = data["result"]["transactionId"]
    partitions = data["result"]["partitions"]
    results = [None] * len(partitions)

    args = []
    for partition in partitions:
        partition_params = params | {"transactionId":transactionId} | partition
        partition_params["transactionId"] = data["result"]["transactionId"]
        partition_params["format"]=format
        partitionPaths=[]
        partition_items = list(partition.items())
        
        if partitionOrder is not None:
            for p in partitionOrder:
                value=partition[p]
                partitionPaths.append(p+"="+str(value))    
        else:
            for key, value in partition.items():
                partitionPaths.append(key+"="+str(value))

        partitionPath="/".join(partitionPaths)
        args.append({
            "endpoint":endpoint,
            "partition_params":partition_params,
            "folder":None if folder is None else "/".join([format,folder]),
            "partitionPath":partitionPath,
            "format":format})
        
    progress = 0
    total = len(args)
    with ThreadPoolExecutor(max_workers=10) as pool:
        future_to_i = { pool.submit(
            getPartition,
            args["endpoint"],
            args["partition_params"],
            args["folder"],
            args["partitionPath"],
            args["format"]): i for i, args in enumerate(args)}
        
        for fut in as_completed(future_to_i):
            i = future_to_i[fut]
            results[i] = fut.result()
            progress = progress+1
            inline_print(f'loading progress: {round(100 * progress / total,0)}%')
        print('\n')
    
    if folder is None and len(results) > 0:
        return pl.concat(results, how="vertical_relaxed")
    else:
        return results
