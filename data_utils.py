from pathlib import Path
from typing import Union

import pandas as pd


def import_csv(source: Union[Path, str]) -> pd.DataFrame:
    """Imports csv file

    It will accept either a URL or a pathlib.Path for a csv file and return it
    as a Pandas DataFrame.

    Args:
        source (Union[Path, str]): Source URL or Path to the file.

    Returns:
        pd.DataFrame: The DataFrame generated from the csv source file.
    """
    return pd.DataFrame(
        pd.read_csv(
            source,
            sep=",",
            header=0,
            index_col=False,
        )
    )


def save_csv(df: pd.DataFrame, dest: Path) -> None:
    """Save DataFrame to CSV

    The DataFrame will be saved as a CSV file to the specified destination Path.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        dest (Path): The local Path to save the CSV to.
    """
    df.to_csv(path_or_buf=dest, index=False)


def save_json(df: pd.DataFrame, dest: Path) -> None:
    """Save DataFrame to JSON

    The DataFrame will be saved as a JSON file to the specified destination Path.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        dest (Path): The local Path to save the JSON to.
    """
    df.to_json(
        dest,
        orient="records",
        indent=2,
        date_format="epoch",
        double_precision=10,
        force_ascii=True,
        date_unit="ms",
        default_handler=None,
    )


if __name__ == "__main__":
    csv_path = Path("hardware.csv")
    json_path = Path("hardware.json")

    df = import_csv(csv_path)
    save_json(df, json_path)
