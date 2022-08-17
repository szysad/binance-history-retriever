from tempfile import NamedTemporaryFile
from unicodedata import name
import pandas as pd
from datetime import datetime, timedelta
import random
from validate import validate
from src.interpolation import interpolate


INTERPOLATED_LABELS = [
    'open',
    'high',
    'low',
    'close',
    'volume',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume'
]

ALL_LABELS = [
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
]

def test_interpolation():
    dt = timedelta(minutes=1)
    t0 = datetime(2020, 1, 1, 0, 0)
    N = 9
    data = {label: [random.random() for _ in range(N)] for label in INTERPOLATED_LABELS}
    data['open_time'] = [t0, t0 + dt, t0 + 2*dt, t0 + 3*dt, t0 + 3.5*dt, t0 + 3.9*dt, t0 + 4*dt, t0 + 6.3*dt, t0 + 9.111*dt]
    df = pd.DataFrame(data)
    df = df[ALL_LABELS]

    with NamedTemporaryFile() as tmpfile:
        interpolate(df).to_parquet(tmpfile.name)
        ok, all = validate(tmpfile.name)
        assert all == ok
        print("TEST PASSED")
    

if __name__ == "__main__":
    test_interpolation()