import pandas as pd
from src.commons import PostProcessedRow, Format, LabelsSubsets
from datetime import timedelta
from typing import List


INTERP_CHUNK_SIZE = 2**14


def post_interpolation_clean(df: pd.DataFrame) -> pd.DataFrame:
    raw_ts = df['open_time'].values.astype(int) // 10 ** 6
    return df.drop(df[raw_ts % 60000 != 0].index, inplace=False)

def interpolate(df: pd.DataFrame) -> pd.DataFrame:
    ''' Interpolates columns with 1m open_time gap
        expects already cleaned df in KLINESPROCESSING format returns in
        POSTPROCESSED format
    '''

    df_chunks: List[pd.DataFrame] = []
    new_chunk_rows: List[PostProcessedRow] = []
    expected_td = timedelta(minutes=1)
    last_ts = df['open_time'].loc[0] - expected_td

    for i, row in enumerate(df.itertuples(index=False)):
        expected_open_time = last_ts + expected_td

        while expected_open_time < row.open_time:
            new_chunk_rows.append(
                PostProcessedRow.interpolation_blank(expected_open_time)
            )
            expected_open_time += expected_td
            last_ts = expected_open_time
        
        # now expected_open_time >= row.open_time
        new_chunk_rows.append(
            PostProcessedRow.from_pd_namedtuple(row, interpolated=False)
        )
        last_ts = row.open_time

        if i % INTERP_CHUNK_SIZE == 0:
            df_chunks.append(
                pd.DataFrame.from_records(new_chunk_rows, columns=Format.POSTPROCESSED.keys())
            )
            new_chunk_rows.clear()
    
    # create chunk from remaining rows
    if i % INTERP_CHUNK_SIZE != 0:
        df_chunks.append(
                pd.DataFrame.from_records(new_chunk_rows, columns=Format.POSTPROCESSED.keys())
            )
        new_chunk_rows.clear()
    
    df = pd.concat(df_chunks)    
    df[LabelsSubsets.INTERPOLATED] = df[LabelsSubsets.INTERPOLATED].interpolate(method='linear', axis=0, inplace=False, limit_area='inside')
    df = post_interpolation_clean(df)
    df.reset_index(drop=True, inplace=True)
    return df
