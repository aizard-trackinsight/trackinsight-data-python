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
data_dir = Path(os.getenv("API_STORAGE"))
data_dir.mkdir(parents=True,exist_ok=True)
host = os.getenv("API_HOST")
key = os.getenv("API_KEY")

def inline_print(msg):
    print(f"\r{msg}", end="", flush=True)

def getURL(endpoint,params):
    qparams =""
    if params is not None:
        qparams = "&"+urlencode({k: v for k, v in params.items() if v is not None})

    url = host+"/"+endpoint+"?"+qparams
    return url

def getJSON(endpoint,params=""):
    url = getURL(endpoint,params,)
    response = requests.get(url,headers={"X-API-KEY":key})
    response.raise_for_status()   # raises error if the request failed
    data = response.json()
    return [data, response.headers]


def getPartition(endpoint,partition_params,folder=None,partitionPath="",format='parquet'):
    
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
    
    if folder is None:
        return pl.concat(results, how="vertical_relaxed")
    else:
        return results