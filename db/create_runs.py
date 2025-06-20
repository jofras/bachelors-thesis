# create_runs.py

import psycopg
from psycopg import Connection
from typing import List, Tuple
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

USER = ""
PASSWORD = ""

def create_repetition_runs_table(conn: Connection):
    """
    Creates the repetition_runs table and necessary indexes.
    """
    logger.info("Creating repetition_runs table")
    
    with conn.cursor() as cur:
        # adding hash to the primary key would disallow the same hallucinated sentence in a new run
        cur.execute("""
            CREATE TABLE repetition_runs (
                id SERIAL PRIMARY KEY,
                hash TEXT NOT NULL,
                file_num INTEGER NOT NULL,
                line_num INTEGER NOT NULL,
                sent_num INTEGER NOT NULL,
                run_length INTEGER NOT NULL,
                UNIQUE (file_num, line_num, sent_num)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_repetition_runs_hash
            ON repetition_runs (hash);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_repetition_runs_file_loc
            ON repetition_runs (file_num, line_num, sent_num);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_repetition_runs_run_length 
            ON repetition_runs (run_length);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_repetition_runs_file_line_sent 
            ON repetition_runs (file_num, line_num, sent_num);
        """)
    conn.commit()
    logger.info("Done creating repetition_runs table")

def insert_run_occurrence_batch(
    batch: List[Tuple[str, int, int, int, int]],
    hash_val: str,
    run: List[Tuple[int, int, int]]
):
    """
    Prepares a single run for bulk insertion by appending it to a batch list.
    """
    file_num, line_num, sent_num = run[0]
    run_length = len(run)
    batch.append((hash_val, file_num, line_num, sent_num, run_length))

def flush_batch(conn: Connection, batch: List[Tuple[str, int, int, int, int]]):
    """
    Executes a bulk insert for all items in the batch using `executemany`.
    """
    if not batch:
        return
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO repetition_runs (hash, file_num, line_num, sent_num, run_length)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, batch)
    conn.commit()
    batch.clear()

def main_loop_streamed(conn: Connection, threshold: int, batch_size: int = 1000):
    """
    Main analysis loop with streaming and batched insertion.
    """
    create_repetition_runs_table(conn)

    logger.info("Querying frequent sentence hashes...")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT hash
            FROM sentences
            GROUP BY hash
            HAVING COUNT(*) >= %s;
        """, (threshold,))
        hashes = [row[0] for row in cur.fetchall()]

    logger.info(f"Found {len(hashes)} hashes with count ≥ {threshold}")

    batch: List[Tuple[str, int, int, int, int]] = []
    hash_count = 0
    loop_time = 0
    avg_loop_time = 0
    
    for h in hashes:
        loop_time = time.time()
        with conn.cursor(name='stream_cursor') as cur:
            cur.execute("""
                SELECT file_num, line_num, sent_num
                FROM sentences
                WHERE hash = %s
                ORDER BY file_num, line_num, sent_num;
            """, (h,))
            
            prev = None
            run = []

            for row in cur:
                if prev is not None and (
                    row[0] == prev[0] and   # file 
                    row[1] == prev[1] and   # line
                    row[2] == prev[2] + 1   # sentence
                ):
                    run.append(row)
                else:
                    if len(run) >= threshold:
                        insert_run_occurrence_batch(batch, h, run)
                    run = [row]
                prev = row

            if len(run) >= threshold:
                insert_run_occurrence_batch(batch, h, run)

        hash_count += 1
        loop_time = time.time() - loop_time
        avg_loop_time = (avg_loop_time * (hash_count - 1) + loop_time) / hash_count
        if hash_count % 10000 == 0:
            logger.info(f"Processed {hash_count}/{len(hashes)} hashes ({hash_count/len(hashes) * 100:.2f}%). Estimated time remaining: {(len(hashes) - hash_count) * avg_loop_time / 60:.2f} minutes")
        if len(batch) >= batch_size:
            flush_batch(conn, batch)

    flush_batch(conn, batch)
    logger.info("All hashes processed and final batch flushed.")

if __name__ == "__main__":
    conn = psycopg.connect(
        dbname="sentence_db",
        user=USER,
        password=PASSWORD,
        host="localhost",
        port=5432
    )
    main_loop_streamed(conn, threshold=2, batch_size=1000)
    conn.close()
