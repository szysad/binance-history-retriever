import pandas as pd
from collections import OrderedDict
from typing import Tuple, List
from .commons import Format


def to_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(level=['open_time'], drop=False)
    df = set_dtypes(df, Format.KLINESRAW)
    return df

def clear_raw(df: pd.DataFrame) -> pd.DataFrame:
    # drop rows where open_time is not unique
    dupes = df['open_time'].duplicated().sum()
    if dupes > 0:
        df = df[df['open_time'].duplicated() == False]

    df.sort_values(by=['open_time'], ascending=False)
    return df


def set_dtypes(df: pd.DataFrame, format: OrderedDict) -> pd.DataFrame:
    datetime_keys: List[Tuple[str, str]] = []

    for label, val in format.items():
        if 'datetime' not in label:
            continue

        unit = val[-3:-1]
        datetime_keys.append((label, unit))

    for label, unit in datetime_keys:
        df[label] = pd.to_datetime(df[label], unit=unit)

    df = df.astype(dtype=format)
    return df
