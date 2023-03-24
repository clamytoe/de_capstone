import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
from prefect import flow, task

from data_utils import import_csv, save_csv

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


@task
def extract(source: str, filename: str, save: bool = True) -> pd.DataFrame:
    # import the source
    csv_df = import_csv(source)

    if save:
        # create directories if they do not exist
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        # save the file
        source_file = RAW_DIR / filename
        csv_df.to_csv(source_file, index=False)

    return csv_df


@task
def transform(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # select the specified columns
    df = data[columns]

    # remove null values
    df_nona = df.fillna("None")

    return df_nona


@task
def load(data: pd.DataFrame, prefix: str) -> None:
    # create file name with current date
    today = f"{prefix}_{int(datetime.now().timestamp())}.csv"
    local_file = LOCAL_STORE / today

    # save dataframe
    save_csv(data, local_file)


@flow(
    name="Hardware ETL Flow",
    description="Simple flow to build my capstone project upon.",
    version=VERSION,
)
def prefect_flow():
    # url for the source data
    url = "http://localhost:8080/hardware.csv"

    # local name for source file
    filename = "hardware.csv"

    # specify the columns to keep
    columns = "host host_sn display display_sn".split()

    # extract the data
    df_hosts = extract(url, filename)

    # transform the data
    df = transform(df_hosts, columns)

    # load the data
    load(df, "hosts")


if __name__ == "__main__":
    prefect_flow()
