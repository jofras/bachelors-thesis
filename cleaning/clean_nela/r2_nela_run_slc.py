# run_slc.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import SentenceListCreator
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

INDIR = ""
INPREFIX = ""
INEXTENSION = ".txt"
OUTDIR = ""
OUTPREFIX = ""

if __name__ == "__main__":

    finder = FileFinder(
        directory=INDIR,
        file_extension=INEXTENSION,
        prefix=INPREFIX
    )
    
    slc_files = finder.find_files()
    task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    slc_file = slc_files[task_id]

    slc_func = SentenceListCreator(
        ner=True,
        chunk_size=250000,
        batch_size=1,
        n_process=1
    )

    proc = FileProcessor(
        input_file_path_list=[slc_file],
        function=slc_func,
        destination=OUTDIR,
        output_prefix=OUTPREFIX
    )

    proc.process_files()