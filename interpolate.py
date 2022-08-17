import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
from typing import NamedTuple
import pandas as pd
import preprocessing as pp


LABELS = [
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
    'interpolated'
]

INTERP_LABELS = [
    #'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume',
    #'interpolated'
]


class AllLabelsRow(NamedTuple):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    interpolated: bool


def validate(df):
    raw_ts = df['open_time'].values.astype(int) // 10 ** 6

    wrong_ts = 0

    td = 60*1000
    last_ts = raw_ts[0] - td
    for row in raw_ts:
        expected_ts = int(last_ts + td)
        if int(row) != int(expected_ts):
            wrong_ts += 1
        last_ts = expected_ts

    print(f"quality: {round((raw_ts.size - wrong_ts) / raw_ts.size, 3)}")

def interpolate(f):
    df = pq.read_table(f).to_pandas()
    if 'interpolated' in df.columns:
        return

    df.sort_values(by=['open_time'], ascending=False)

    # create df with nans to interpolate
    expected_td = timedelta(minutes=1)
    last_ts = df['open_time'].loc[0] - expected_td
    start_ts = last_ts + expected_td
    rows = []

    for row in df.itertuples(index=False):
        expected_open_time = last_ts + expected_td
        new_row = None

        while expected_open_time < row.open_time:
            vals = (expected_open_time,) + (np.nan,) * 9 + (True,)
            new_row = AllLabelsRow(*vals)

            rows.append(new_row)
            expected_open_time += expected_td
            last_ts = expected_open_time
            #print(rows[-1].open_time, end='\r', flush=True)

            if len(rows) > 1:
                assert rows[-1].open_time > rows[-2].open_time, f"ts1: {rows[-1].open_time}, ts2: {rows[-2].open_time}"

        
        if expected_open_time >= row.open_time:
            new_row = AllLabelsRow(*row, interpolated=False)
            rows.append(new_row)
            last_ts = row.open_time
        #print(rows[-1].open_time, end='\r', flush=True)
        if len(rows) > 1:
            assert rows[-1].open_time > rows[-2].open_time, f"ts1: {rows[-1].open_time}, ts2: {rows[-2].open_time}"
    
    df = pd.DataFrame.from_records(rows, columns=LABELS)

    # interpolate
    df[INTERP_LABELS] = df[INTERP_LABELS].interpolate(method='linear', axis=0, inplace=False, limit_area='inside')
    raw_ts = df['open_time'].values.astype(int) // 10 ** 6
    df.drop(df[raw_ts % 60000 != 0].index, inplace=True)

    pp.assert_integrity(df)

    validate(df)
    df.to_parquet(f)



if __name__ == "__main__":
    ds_dir = Path("./data")
    files = list(ds_dir.glob("*.parquet"))
    total_files = len(files)
    for i, f in enumerate(files):
        if f.name != "ADA-BTC.parquet":
            continue
        interpolate(f)
        print(f"{i} / {total_files} interpolating {f.name}")
