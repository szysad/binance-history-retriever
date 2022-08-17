from typing import List
import pyarrow.parquet as pq
import threading
from pathlib import Path
from queue import Queue, Empty
from src.preprocessing import clear_raw, set_dtypes, to_raw
from src.commons import Format
from src.interpolation import interpolate
from src.validation import validate
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

READ_DIR = 'data'
WRITE_DIR = 'processed_data'
N_WORKERS = 1


def worker(task_queue: Queue, completion_queue: Queue):
    self_id = threading.get_ident()
    try:
        while True:
            pqt: Path = task_queue.get_nowait()
            logger.debug(f"worker {self_id} starts processing {pqt.name}.")
            df = pq.read_table(pqt).to_pandas()
            df = to_raw(df)
            df = clear_raw(df)
            df = set_dtypes(df, Format.KLINESPROCESSING)
            if validate(df) is True:
                df = interpolate(df)
                df.to_parquet(pqt.parent.parent / WRITE_DIR / pqt.name)
            else:
                logger.warning(f"worker {self_id} failed to validate {pqt.name}")
            logger.debug(f"worker {self_id} finished processing {pqt.name}.")
            task_queue.task_done()
            completion_queue.put(item=(self_id, pqt))
    except Empty:
        # finish work
        completion_queue.put(item=None)
        return


def main():
    Path(WRITE_DIR).mkdir(exist_ok=True)

    total = 0
    threads: List[threading.Thread] = []
    task_queue = Queue()
    completion_queue = Queue()
    for pqt in Path(READ_DIR).glob("*.parquet"):
        task_queue.put_nowait(pqt)
        total += 1

    for _ in range(N_WORKERS):
        threads.append(
            threading.Thread(target=worker, args=(task_queue, completion_queue))
        )
    
    for t in threads:
        t.start()
    
    processed = 0
    finished = 0
    try:
        while True:
            msg = completion_queue.get()
            if msg is None:
                finished += 1
                if finished == N_WORKERS:
                    break
            else:
                worker_id, pqt = msg
                processed += 1
                logger.info(f"Worker {worker_id} finished processing {pqt.name}. {processed} / {total}")
                completion_queue.task_done()
    except Empty:
        pass
    
    for t in threads:
        t.join()
    
    logger.info(f"finished processing all files")
    

if __name__ == '__main__':
    main()
