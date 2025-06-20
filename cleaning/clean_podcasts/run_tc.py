# run_tc.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import TextCleaner
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/cluster/scratch/jquinn/input_raw/',
        file_extension='.txt',
        prefix='raw'
    )
    
    tc_files = finder.find_files()
    task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    tc_file = tc_files[task_id]

    tc_func = TextCleaner(
        remove_non_speaker_content=True,
        contraction_level=1
    )

    proc = FileProcessor(
        input_file_path_list=[tc_file],
        function=tc_func,
        destination='/cluster/scratch/jquinn/output_tc/',
        output_prefix='tc'
    )

    proc.process_files()