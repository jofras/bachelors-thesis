# run_slc.py

from utils.filefinder import FileFinder
from utils.fileproc import FileProcessor
from utils.filefunc import GloVeFormatter
import os

"""
This script assumes the podcast dataset has been merged on urls and only contains the "turnText" part of the original jsonl files.
"""

if __name__ == "__main__":

    finder = FileFinder(
        directory='/Users/jonny/Documents/eth/bachelor_thesis/bachelors-thesis/datasets/podcasts/post_hlc/3',
        file_extension='.json',
        prefix='w2v'
    )
    
    w2v_files = finder.find_files()

    gf_func = GloVeFormatter(
        stop_token=["i", "love", "blueberry", "waffles"]
    )

    proc = FileProcessor(
        input_file_path_list=w2v_files,
        function=gf_func,
        destination='/Users/jonny/Documents/eth/bachelor_thesis/bachelors-thesis/datasets/podcasts/post_gf',
        output_prefix='glv'
    )

    proc.process_files()