# run_es.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import EntrySimplifier
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/Users/jonny/Documents/eth/bachelor_thesis/bachelors-thesis/datasets/nela/nela-gt-2020/newsdata',
        file_extension='.json'
    )
    
    es_files = finder.find_files()
    # task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    # tc_file = tc_files[task_id]

    es_func = EntrySimplifier(
        keep_fields=["content"],
        output_extension=".txt",
        keep_labels=False,
        nela=True
    )

    proc = FileProcessor(
        #input_file_path_list=[tc_file],
        input_file_path_list=es_files,
        function=es_func,
        destination='/Users/jonny/Documents/eth/bachelor_thesis/bachelors-thesis/datasets/nela/content_only',
        output_prefix='es_'
    )
    
    proc.process_files()