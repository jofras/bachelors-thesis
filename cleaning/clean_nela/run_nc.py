# run_nc.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import NewsCleaner
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

DIRECTORY = ""
EXTENSION = ".txt"
OLD_PREFIX = ""
NEW_PREFIX = ""
DESTINATION = ""

if __name__ == "__main__":

    finder = FileFinder(
        directory=DIRECTORY,
        file_extension=EXTENSION,
        prefix=OLD_PREFIX
    )
    
    # nc_files = finder.find_files()
    # task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    # nc_file = nc_files[task_id]

    nc_files = finder.find_files()

    nc_func = NewsCleaner(contraction_level=1)

    proc = FileProcessor(
        input_file_path_list=nc_files,
        function=nc_func,
        destination=DESTINATION,
        output_prefix=NEW_PREFIX
    )
    
    proc.process_files()