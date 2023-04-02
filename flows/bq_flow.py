import json
import subprocess
from datetime import datetime

import pandas as pd
import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_insert_stream

VERSION = (
    subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        stdout=subprocess.PIPE,
    )
    .stdout.decode("utf-8")
    .strip()
)


@task
def get_data(url: str):
    # import the source
    res = requests.get(url)
    if res.ok:
        coins = res.json()

        return coins


@task
def transform_data(data):
    missing = {
        "gatetoken": "https://gatechain.io/",
        "dydx": "https://dydx.foundation/",
    }
    coins = data["data"]

    # Convert timestamp string to datetime object
    ts = pd.to_datetime(data["timestamp"], unit="ms")
    dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S.%f")

    # Convert datetime object to Unix timestamp
    unix_timestamp = dt.timestamp()

    transformed = []
    for coin in coins:
        try:
            if coin["id"] in missing.keys():
                coin["explorer"] = missing[coin["id"]]
            transformed.append(
                {
                    "timestamp": unix_timestamp,
                    "id": coin["id"],
                    "rank": int(coin["rank"]),
                    "symbol": coin["symbol"],
                    "name": coin["name"],
                    "supply": float(coin["supply"]),
                    # "max_supply": float(coin["maxSupply"]) if coin["maxSupply"] else None,
                    "market_cap_usd": float(coin["marketCapUsd"]),
                    "volume_usd_24hr": float(coin["volumeUsd24Hr"]),
                    "price_usd": float(coin["priceUsd"]),
                    "change_percent_24hr": float(coin["changePercent24Hr"]),
                    # "vwap_24hr": float(coin["vwap24Hr"]) if coin["vwap24Hr"] else None,
                    "url": coin["explorer"],
                }
            )
        except TypeError as e:
            print(f"Error processing: \n{coin}\n{e}")
            print("###")

    return pd.DataFrame(transformed)


@flow
def insert_into_bigquery(data):
    # your code to insert data into BigQuery goes here
    gcp_credentials = GcpCredentials(project="dtc-de-course-374214")

    data_json = json.loads(data.to_json(orient="records"))

    result = bigquery_insert_stream(
        dataset="crypto_decap",
        table="coins",
        records=data_json,
        gcp_credentials=gcp_credentials,
        location="us-central1",
    )
    return result


@flow(
    name="Crypto Coins ETL Flow",
    description="Simple flow to build my capstone project upon.",
    version=VERSION,
    task_runner=ConcurrentTaskRunner(),
)
def etl_api_to_bq(url: str) -> None:
    data = get_data(url)
    trans_data = transform_data(data)
    _ = insert_into_bigquery(trans_data)


if __name__ == "__main__":
    url = "https://api.coincap.io/v2/assets"
    etl_api_to_bq(url)
