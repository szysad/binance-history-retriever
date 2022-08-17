import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path


def transform_file(f):
    table = pq.read_table(f)
    df = table.to_pandas()
    df.reset_index(inplace=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f)


if __name__ == "__main__":
    ds_dir = Path("./data")
    files = list(ds_dir.glob("*.parquet"))
    total_files = len(files)
    for i, f in enumerate(files):
        print(f"transforming {f.name}, {i} / {total_files}")
        transform_file(f)
