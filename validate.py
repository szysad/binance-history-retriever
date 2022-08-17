import pyarrow.parquet as pq
from pathlib import Path


def validate(f):
    table = pq.read_table(f)
    df = table.to_pandas()
    raw_ts = df['open_time'].values.astype(int) // 10 ** 6

    wrong_ts = 0

    td = 60*1000
    last_ts = raw_ts[0] - td
    for i, row in enumerate(raw_ts):
        expected_ts = int(last_ts + td)
        if int(row) != int(expected_ts):
            wrong_ts += 1
        last_ts = expected_ts

    return raw_ts.size - wrong_ts, raw_ts.size


if __name__ == "__main__":
    min_quality = 1.0
    ds_dir = Path("./processed_data")
    files = list(ds_dir.glob("*.parquet"))
    total_files = len(files)
    for i, f in enumerate(files):
        ok, all = validate(f)
        min_quality = min(min_quality, ok / all)
        print(f"{i} / {total_files} checking {f.name}, quality: {round(ok / all, 3)}")
    
    print(f"min file quality: {min_quality}")
