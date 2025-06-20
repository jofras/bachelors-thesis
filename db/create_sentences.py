# create_sentences.py

from utils.filefinder import FileFinder
from pathlib import Path
import logging
import re
import json
import xxhash
import psycopg
from psycopg import Connection
from psycopg.errors import DatabaseError, OperationalError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000  # tune based on memory and perf

DATA_DIR = ""
USER = ""
PASSWORD = ""

def extract_file_idx(file_path: Path, prefix: str, suffix: str, extension: str) -> int:
    filename = file_path.name
    pattern = re.escape(prefix) + r'(\d+)' + re.escape((suffix or "") + extension)
    match = re.fullmatch(pattern, filename)
    if not match:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern")
    return int(match.group(1))

def hash_sentence(sentence: list[str]) -> str:
    return xxhash.xxh64(" ".join(sentence)).hexdigest()

def populate_db(
    input_file_paths: list[Path],
    database_conn: Connection,
    stop_token=["i", "love", "blueberry", "waffles"],
    file_prefix="slc",
    file_suffix=None,
    file_extension=".json"
) -> None:
    insert_stmt = """
        INSERT INTO sentences (hash, file_num, line_num, sent_num, sent_offset)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        with database_conn.cursor() as cursor:
            buffer = []

            for file_path in input_file_paths:
                file_path = Path(file_path)
                try:
                    file_idx = extract_file_idx(file_path, file_prefix, file_suffix, file_extension)
                    logger.info(f"Processing file {file_path} with extracted index {file_idx}")
                except ValueError as e:
                    logger.warning(f"Skipping file {file_path}: {e}")
                    continue

                line_idx = 0 # setting this to one will confuse downstream processing
                sent_idx = 0
                offset = 0

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if not isinstance(data, list) or not all(isinstance(sentence, list) for sentence in data):
                        raise ValueError("File must contain a list of lists (tokenized sentences)")

                    for sentence in data:
                        if sentence == stop_token:
                            line_idx += 1
                            sent_idx = 0
                            offset = 0
                            continue

                        if not sentence: # kinda redundant but better to double check
                            continue

                        sentence_hash = hash_sentence(sentence)
                        glove_output_length = len(" ".join(sentence)) + 1

                        buffer.append((
                            sentence_hash,
                            file_idx,
                            line_idx,
                            sent_idx,
                            offset
                        ))

                        sent_idx += 1
                        offset += glove_output_length

                        if len(buffer) >= BATCH_SIZE:
                            try:
                                cursor.executemany(insert_stmt, buffer)
                                database_conn.commit()
                                logger.debug(f"Inserted batch of {len(buffer)} sentences from {file_path}")
                                buffer.clear()
                            except (DatabaseError, OperationalError) as db_err:
                                logger.error(f"DB error during batch insert from file {file_path}: {db_err}")
                                database_conn.rollback()
                                buffer.clear()

                except (json.JSONDecodeError, IOError, ValueError) as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    continue

            # Insert remaining buffered data
            if buffer:
                try:
                    cursor.executemany(insert_stmt, buffer)
                    database_conn.commit()
                    logger.debug(f"Inserted final batch of {len(buffer)} sentences")
                    buffer.clear()
                except (DatabaseError, OperationalError) as db_err:
                    logger.error(f"Final DB insert error: {db_err}")
                    database_conn.rollback()

    except Exception as exc:
        logger.critical(f"Unexpected error during DB population: {exc}")
        database_conn.rollback()
        raise

def initialize_schema(conn: Connection) -> None:
    try:
        with conn.cursor() as cur:
            # apparently independent serial keys are better performance-wise for big joins
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sentences (
                    id SERIAL PRIMARY KEY,
                    hash TEXT NOT NULL,
                    file_num INT NOT NULL,
                    line_num INT NOT NULL,
                    sent_num INT NOT NULL,
                    sent_offset INT NOT NULL,
                    UNIQUE (file_num, line_num, sent_num)
                );
            """)
        conn.commit()
        logger.info("Schema initialized successfully.")
    except (DatabaseError, OperationalError) as e:
        logger.error(f"Error initializing schema: {e}")
        conn.rollback()
        raise

def create_indices(conn: Connection) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_file_line_sent ON sentences(file_num, line_num, sent_num);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON sentences(hash);")

        conn.commit()
        logger.info("Indices created successfully.")
    except (DatabaseError, OperationalError) as e:
        logger.error(f"Error creating indices: {e}")
        conn.rollback()
        raise

def create_and_initialize_db(
    input_file_paths: list[Path],
    stop_token: list[str] = ["i", "love", "blueberry", "waffles"],
    file_prefix: str = "slc",
    file_suffix: str = None,
    db_name: str = "sentence_db"
) -> None:

    db_user = USER
    db_password = PASSWORD
    db_host = "localhost"
    db_port = 5432

    try:
        with psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        ) as conn:
            initialize_schema(conn)
            populate_db(
                input_file_paths,
                database_conn=conn,
                stop_token=stop_token,
                file_prefix=file_prefix,
                file_suffix=file_suffix,
            )
            create_indices(conn)

    except (DatabaseError, OperationalError) as db_err:
        logger.error(f"Database connection or operation failed: {db_err}")
        raise
    except Exception as exc:
        logger.critical(f"Unexpected error during DB initialization: {exc}")
        raise

if __name__ == "__main__":
    finder = FileFinder(
        directory=DATA_DIR,
        file_extension='.json',
        prefix='slc'
    )

    files = finder.find_files()

    create_and_initialize_db(files)