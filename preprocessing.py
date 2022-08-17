import os
from datetime import date, datetime, timedelta
from typing import NamedTuple, List

import time
import pandas as pd

ALL_LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'ignore',
    'tradable'
]

FINAL_LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    'tradable'
]

class AllLabelsRow(NamedTuple):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    ignore: float
    tradable: bool


def set_dtypes(df):
    """
    convert all columns in pd.df to their proper dtype
    assumes csv is read raw without modifications; pd.read_csv(csv_filename)"""

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df = df.astype(dtype={
        'open_time': 'datetime64[ms]',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64',
        'close_time': 'datetime64[ms]',
        'quote_asset_volume': 'float64',
        'number_of_trades': 'int64',
        'taker_buy_base_asset_volume': 'float64',
        'taker_buy_quote_asset_volume': 'float64',
        'ignore': 'float64'
    })

    return df


def set_dtypes_compressed(df):
    """Create a `DatetimeIndex` and convert all critical columns in pd.df to a dtype with low
    memory profile. Assumes csv is read raw without modifications; `pd.read_csv(csv_filename)`."""

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df = df.astype(dtype={
        'open_time': 'datetime64[ms]',
        'open': 'float32',
        'high': 'float32',
        'low': 'float32',
        'close': 'float32',
        'volume': 'float32',
        'number_of_trades': 'uint16',
        'quote_asset_volume': 'float32',
        'taker_buy_base_asset_volume': 'float32',
        'taker_buy_quote_asset_volume': 'float32',
        'tradable': 'bool'
    })

    return df


def assert_integrity(df):
    """make sure no rows have empty cells or duplicate timestamps exist"""

    assert df.isna().all(axis=1).any() == False
    assert df['open_time'].duplicated().any() == False


def create_merge_batch(b1, b2):
    first_ts = b1['open_time'].max()
    last_ts = b2['open_time'].min()
    assert (last_ts - first_ts) % 60000 == 0, "Cant merge batches without interpolation"
    rows: List[AllLabelsRow] = []
    last_row = list(b1.itertuples(index=False))[-1]  
    for open_time in range(first_ts, last_ts, 60*1000):
        row = last_row._replace(open_time=open_time, tradable=False)
        rows.append(row)
    
    return pd.DataFrame.from_records(rows, columns=FINAL_LABELS)
    


def build_clean(df):
    """build new dataframe with new column 'tradable' when each new row has timestamp greater by 1m
       if that row can not be created there 'tradable' is set to 0 otherwise 1.
    """

    df.drop(labels=['tradable'], axis=1, errors='ignore', inplace=True)

    df.sort_values(by=['open_time'], ascending=False)
    expected_td = 60000
    last_ts = df['open_time'].loc[0] - expected_td
    rows: List[AllLabelsRow] = []
    for row in df.itertuples(index=False):
        ok = True
        expected_open_time = last_ts + expected_td
        if row.open_time != expected_open_time:
            ok = False
        if row.close_time - row.open_time != 59999:
            ok = False
        
        # correct open time
        row = row._replace(open_time=expected_open_time)
        
        if ok is True:
            # add row as normal
            new_row = AllLabelsRow(*row, tradable=True)
        else:
            # add row with tradable false flag
            new_row = AllLabelsRow(*row, tradable=False)
        rows.append(new_row)
        last_ts += expected_td

    return pd.DataFrame.from_records(rows, columns=ALL_LABELS)


def write_raw_to_parquet(df, full_path):
    """takes raw df and writes a parquet to disk"""
    df = set_dtypes_compressed(df)

    # give all pairs the same nice cut-off
    df = df[df['open_time'] < str(date.today())]

    df.to_parquet(full_path)


def is_outdated(df) -> bool:
    """checks if data in dataframe doesnt end to early"""
    ts_threshold = int(time.time()) - 10 * 24 * 60 * 60
    max_time = df['open_time'].max()
    return max_time < ts_threshold * 1000

if __name__ == "__main__":
    # TODO test build_clean

    import random
    n_rows = 5
    t0 = datetime(2018, 1, 1, 0, 0)
    dt = timedelta(minutes=1)
    open_times = [t0, t0 + dt, t0 + 2.5*dt, t0 + 3*dt, t0 + 4*dt]
    data = {label: [random.randint(0, 100) for _ in range(n_rows)] for label in ALL_LABELS[:-1]}  # without tradable label
    data['open_time'] = open_times
    data['close_time'] = [open_t + timedelta(milliseconds=59999) for open_t in open_times]

    df = build_clean(pd.DataFrame(data))
    print(df)