# remove_seq.py

from utils.filefinder import FileFinder
from pathlib import Path
import re
import psycopg
import json
import xxhash
import logging
import time

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = ""
USER = ""
PASSWORD = ""

STOP_SENTINEL = ["i", "love", "blueberry", "waffles"]

def extract_file_idx(file_path: Path, prefix: str, suffix: str, extension: str) -> int:
    filename = file_path.name
    pattern = re.escape(prefix) + r'(\d+)' + re.escape((suffix or "") + extension)
    match = re.fullmatch(pattern, filename)
    if not match:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern")
    return int(match.group(1))

def hash_sentence(sentence: list[str]) -> str:
    return xxhash.xxh64(" ".join(sentence)).hexdigest()

if __name__ == "__main__":
    
    # get relevant files
    slc_files = FileFinder(
        directory=DATA_DIR,
        file_extension='.json',
        prefix='slc'
    ).find_files()

    # connect to database
    conn = psycopg.connect(
        dbname="podcast_sentence_db", 
        user=USER, 
        password=PASSWORD, 
        host="localhost", 
        port=5432
    )

    # to be justified (see plots in analysis/hallucinations.ipynb)
    threshold = 4

    total_time = 0

    for j, slc_file in enumerate(slc_files):
        logger.info(f"Processing {Path(slc_file).name}")

        start_time = time.time()

        with open(slc_file, 'r') as infile:
            sentence_list = json.load(infile)

        file_idx = extract_file_idx(Path(slc_file), prefix="slc", suffix="", extension=".json")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT hash, line_num, sent_num, run_length
                FROM repetition_runs
                WHERE file_num = %s AND run_length >= %s
                ORDER BY line_num, sent_num
            """, (file_idx, threshold))
            runs = cur.fetchall()

        logger.info(f"Found {len(runs)} repeated runs in {Path(slc_file).name}")
        # preprocess runs into a map: (line_num, sent_num) -> (hash, run_length)
        run_map = {
            (line_num, sent_num): (hash_str, run_length)
            for hash_str, line_num, sent_num, run_length in runs
        }

        output_list = []
        line_num = 0
        sent_num = 0
        i = 0
        n = len(sentence_list)

        while i < n:
            sentence = sentence_list[i]

            if sentence == STOP_SENTINEL:
                line_num += 1
                sent_num = 0
                i += 1
                output_list.append(sentence) # for glove (will be filtered in create_ijson_gen anyway)
                continue  # do not include in output

            key = (line_num, sent_num)
            if key in run_map:
                expected_hash, run_length = run_map[key]

                # extract run slice and verify hash
                if i + run_length > n:
                    logger.error(f"Run at {key} exceeds sentence list bounds")
                    break

                run_slice = sentence_list[i : i + run_length]
                all_match = all(hash_sentence(s) == expected_hash for s in run_slice)

                if not all_match:
                    logger.error(f"Hash mismatch in run at {key} with length {run_length}")
                else:
                    logger.debug(f"Removed repeated run at {key} (length {run_length})")

                # skip entire run
                i += run_length
                sent_num += run_length
            else:
                output_list.append(sentence)
                i += 1
                sent_num += 1

        # save cleaned file
        match = re.search(r'(\d+)$', Path(slc_file).stem)
        suffix = match.group(1) if match else Path(slc_file).stem

        output_path = Path(f"") / f"w2v{suffix}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as outfile:
            json.dump(output_list, outfile, ensure_ascii=False)

        loop_time = time.time() - start_time
        total_time += loop_time

        # estimate time remaining
        files_done = j + 1
        files_left = len(slc_files) - files_done
        avg_loop_time = total_time / files_done
        estimated_remaining = files_left * avg_loop_time / 60  # in minutes

        logger.info(f"Saved cleaned file to {output_path.name}. Estimated time remaining: {estimated_remaining:.2f} minutes")
    
    logger.info(f"Processing completed in {total_time/60:.2f} minutes")