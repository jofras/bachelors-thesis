# run_podcast_gf.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import GloVeFormatter
import os

"""
This script runs glove formatting on hallucination-removed podcasts separated by a stop token. 
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/cluster/scratch/jquinn/output_hlc/',
        file_extension='.json',
        prefix='w2v'
    )
    
    w2v_files = finder.find_files()
    task_id = int(os.getenv("SLURM_ARRAY_TASK_ID", 0))
    w2v_file = w2v_files[task_id]

    gf_func = GloVeFormatter(
        stop_token=["i", "love", "blueberry", "waffles"]
    )

    proc = FileProcessor(
        input_file_path_list=[w2v_file],
        function=gf_func,
        destination='',
        output_prefix='podcast_gf'
    )

    proc.process_files()