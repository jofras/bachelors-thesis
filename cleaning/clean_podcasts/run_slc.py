# run_slc.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import SentenceListCreator
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/cluster/scratch/jquinn/input_tc/',
        file_extension='.txt',
        prefix='tc'
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
        destination='/cluster/scratch/jquinn/output_slc/',
        output_prefix='slc'
    )

    proc.process_files()