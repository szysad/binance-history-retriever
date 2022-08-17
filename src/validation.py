from datetime import timedelta, datetime
import pandas as pd
import numpy as np


MAX_TIME_GAP = timedelta(days=1)
MIN_LAST_DATETIME = datetime(year=2022, month=8, day=16) - timedelta(days=35*3)


def is_outdated(df: pd.DataFrame) -> bool:
    return df['open_time'].max() < MIN_LAST_DATETIME

def are_time_gaps_ok(df: pd.DataFrame) -> bool:
    time_diffs = (df['open_time'].shift(periods=-1) - df['open_time']).unique()
    time_diffs = time_diffs[~np.isnan(time_diffs)]
    
    max_timegap = timedelta(seconds=time_diffs.max() / np.timedelta64(1, 's'))
    return max_timegap < MAX_TIME_GAP

def validate(df: pd.DataFrame) -> bool:
    return (
        not is_outdated(df) and \
        are_time_gaps_ok(df)
    )


