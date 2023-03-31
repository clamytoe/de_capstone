import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from prefect import flow, task

VERSION = (
    subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        stdout=subprocess.PIPE,
    )
    .stdout.decode("utf-8")
    .strip()
)
LOCAL_STORE = Path("data")
RAW_DIR = LOCAL_STORE / "raw"


@task(retries=10, retry_delay_seconds=10)
def extract(source: str, filename: str, save: bool = True) -> Any:
    # import the source
    res = requests.get(source)
    if res.ok:
        coins = res.json()

        if save:
            # create directories if they do not exist
            RAW_DIR.mkdir(parents=True, exist_ok=True)

            # save raw the file
            ts = coins["timestamp"]
            source_file = RAW_DIR / f"{ts}_{filename}"
            raw_df = pd.DataFrame(coins["data"])
            raw_df["timestamp"] = coins["timestamp"]
            raw_df.to_csv(source_file, index=False)

        return coins


@task
def transform(data: Any) -> tuple[str, pd.DataFrame]:
    missing = {
        "gatetoken": "https://gatechain.io/",
        "dydx": "https://dydx.foundation/",
    }
    coins = data["data"]
    str_datetime = data["timestamp"]
    ts = pd.to_datetime(data["timestamp"], unit="ms")
    transformed = []
    for coin in coins:
        try:
            if coin["id"] in missing.keys():
                coin["explorer"] = missing[coin["id"]]
            transformed.append(
                {
                    "timestamp": ts,
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

    return str_datetime, pd.DataFrame(transformed)


@task
def load(
    data: pd.DataFrame,
    prefix: str,
    date: str,
) -> None:
    # create file name with current date
    filename = f"{date}_{prefix}.parquet"
    local_file = LOCAL_STORE / filename

    # save dataframe
    data.to_parquet(local_file)


@flow(
    name="Crypto Coins ETL Flow",
    description="Simple flow to build my capstone project upon.",
    version=VERSION,
)
def prefect_flow():
    # url for the source data
    url = "https://api.coincap.io/v2/assets"

    # local name for source file
    filename = "cryptocoin.csv"

    # extract the data
    coins_json = extract(url, filename)

    # transform the data
    timestamp, df = transform(coins_json)

    # load the data
    load(df, "coins", timestamp)


if __name__ == "__main__":
    prefect_flow()
