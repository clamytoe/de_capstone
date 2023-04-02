import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

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


@task(
    retries=3,
    retry_delay_seconds=20,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=1),
)
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

    return ts.strftime("%Y-%m-%d_%H-%M"), pd.DataFrame(transformed)


@task
def write_load(
    data: pd.DataFrame,
    prefix: str,
    timestamp: str,
) -> Path:
    # create file name with current date
    dt, _ = timestamp.split("_")
    year, month, day = dt.split("-")
    local_dir = LOCAL_STORE / year / month / day
    local_dir.mkdir(parents=True, exist_ok=True)

    ts = timestamp.replace("-", "")
    ts = ts.replace("_", "")
    filename = f"{ts}_{prefix}.parquet"
    local_file = local_dir / filename

    # save dataframe
    data.to_parquet(local_file)

    return local_file


@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("decap-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)  # type: ignore


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

    # write the data
    path = write_load(df, "coins", timestamp)

    # write to gcs
    write_gcs(path)


@flow()
def etl_parent_flow():
    prefect_flow()


if __name__ == "__main__":
    # prefect_flow()
    etl_parent_flow()
