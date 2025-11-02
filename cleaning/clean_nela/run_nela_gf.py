# run_nela_gf.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import GloVeFormatter
import os

"""
This script assumes NELA sentence lists (what was input into w2v). 
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/cluster/scratch/jquinn/r2_nela_new_output_slc',
        file_extension='.json',
        prefix='r2_nela_new_slc'
    )
    
    slc_files = finder.find_files()
    task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    slc_file = slc_files[task_id]

    gf_func = GloVeFormatter(
        stop_token=["i", "love", "blueberry", "waffles"]
    )

    proc = FileProcessor(
        input_file_path_list=[slc_file],
        function=gf_func,
        destination='/cluster/scratch/jquinn/r2_nela_new_output_gf',
        output_prefix='r2_nela_new_gf'
    )

    proc.process_files()