# create_sentence_texts.py

import psycopg
import json
import xxhash
from pathlib import Path
from psycopg import Connection
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = ""
USER = ""
PASSWORD = ""

def create_sentence_texts_table(conn: Connection):
    with conn.cursor() as cur:
        # this automatically creates an index on hash
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sentence_texts (
                hash TEXT PRIMARY KEY,
                sent_text TEXT NOT NULL
            );
        """)
    conn.commit()

def populate_sentence_texts(conn: Connection, batch_size=1000):
    logger.info("Fetching unique hashes from repetition_runs...")
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT hash, file_num, line_num, sent_num FROM repetition_runs;")
        rows = cur.fetchall()

    logger.info(f"Loaded {len(rows)} unique entries")

    # Map hash -> (file_num, line_num, sent_num)
    hash_locations = {row[0]: (row[1], row[2], row[3]) for row in rows}

    # Group by file_num to batch disk access
    file_groups = defaultdict(list)
    for h, (f, l, s) in hash_locations.items():
        file_groups[f].append((h, l, s))

    inserted = 0
    buffer = []

    for file_num, entries in file_groups.items():
        logger.info(f"Processing file slc{file_num}.json with {len(entries)} hashes")
        try:
            file_path = Path(DATA_DIR) / f"slc{file_num}.json"
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.warning(f"File slc{file_num}.json not found, skipping")
            continue

        # compute line positions
        line_start_map = {0: 0}
        current_line_num = 0
        for idx, item in enumerate(data):
            if item == ["i", "love", "blueberry", "waffles"]:
                current_line_num += 1
                line_start_map[current_line_num] = idx + 1

        for h, line_num, sent_num in entries:
            try:
                if line_num not in line_start_map:
                    logger.warning(f"Line number {line_num} not found in map for slc{file_num}. This indicates a mismatch in line numbering logic between create_sentences and create_sentence_texts")
                    continue

                line_start = line_start_map[line_num]
                
                # check if the calculated index is within bounds before accessing
                actual_index = line_start + sent_num
                if actual_index >= len(data) or actual_index < 0:
                    logger.warning(f"Index out of bounds for {h} in slc{file_num} line {line_num} sent {sent_num}. Calculated index: {actual_index}, Data length: {len(data)}")
                    continue

                sentence_tokens = data[actual_index]
                
                # ensure the retrieved item is a list (tokens) and not a stop token itself
                if not isinstance(sentence_tokens, list) or sentence_tokens == ["i", "love", "blueberry", "waffles"]:
                    logger.warning(f"Retrieved item at slc{file_num} line {line_num} sent {sent_num} is not a valid sentence: {sentence_tokens}")
                    continue

                sentence_str = " ".join(sentence_tokens)
                computed_hash = xxhash.xxh64(sentence_str).hexdigest() # get hash
                if computed_hash != h:
                    logger.warning(f"Hash mismatch in slc{file_num} line {line_num} sent {sent_num}. Computed: {computed_hash}, Expected: {h}. Sentence: '{sentence_str}'")
                    continue
                buffer.append((h, sentence_str))
            except Exception as e:
                logger.warning(f"Failed extracting {h} from slc{file_num}: {e}")
                continue

            if len(buffer) >= batch_size:
                insert_sentences(conn, buffer)
                inserted += len(buffer)
                logger.info(f"Inserted {inserted} rows so far")
                buffer.clear()

    if buffer:
        insert_sentences(conn, buffer)
        inserted += len(buffer)
        logger.info(f"Final insert, total {inserted} rows")

def insert_sentences(conn: Connection, rows: list[tuple[str, str]]):
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO sentence_texts (hash, sent_text)
            VALUES (%s, %s)
            ON CONFLICT (hash) DO NOTHING;
        """, rows)
    conn.commit()

if __name__ == "__main__":
    conn = psycopg.connect(
        dbname="sentence_db", 
        user=USER, 
        password=PASSWORD, 
        host="localhost", 
        port=5432
    )
    create_sentence_texts_table(conn)
    populate_sentence_texts(conn)
    conn.close()