# run_w2v.py

import gensim 
from gensim.models import Word2Vec
from gensim.models.callbacks import CallbackAny2Vec
import logging
import os
import ijson
from pathlib import Path
from utils.filefinder import FileFinder
import csv

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO) 
logger = logging.getLogger("run_w2v.py")

"""
This script assumes that the sentence lists it gets as input are devoid of any repeated sentence fragments (in the case of the podcast dataset). 
It creates a sentence list generator, that is then fed into gensim's word2vec model. 
It saves the model to the same directory. 
"""

PODCASTS = 1
FILEPATH = 'cluster/scratch/jquinn/input'

def create_ijson_gen(input_file_paths, stop_token):
    """
    Takes a list of input file paths (JSON) and creates an ijson generator.
    """
    successes = failures = 0
    
    for file_path in input_file_paths:
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                parser = ijson.items(f, 'item')  # adjust key based on actual JSON structure
                
                for sentence in parser:
                    if isinstance(sentence, list) and sentence != [stop_token] and sentence != []:
                        yield sentence
            successes += 1
        
        except Exception as e:
            failures += 1
            logger.error(f"Error processing file {file_path}: {e}")
            continue

    logger.info(f"Inserted sentences from {successes} files successfully. Encountered {failures} errors")

class CSVLossLogger(CallbackAny2Vec):
    def __init__(self, filepath="cluster/scratch/jquinn/loss_log.csv"):
        self.epoch = 0
        self.loss_previous = 0.0
        self.filepath = filepath

        # ensure the CSV file has a header
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['epoch', 'loss', 'delta'])

    def on_epoch_end(self, model):
        current_loss = model.get_latest_training_loss()
        delta = current_loss - self.loss_previous
        self.loss_previous = current_loss

        with open(self.filepath, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([self.epoch, current_loss, delta])

        print(f"Epoch {self.epoch} | Loss: {current_loss:.2f} | Delta: {delta:.2f}")
        self.epoch += 1

if __name__ == "__main__":

    logger.info("If using cython, should be 1: %d", gensim.models.word2vec.FAST_VERSION)

    finder = FileFinder(
        directory=FILEPATH,
        file_extension='.json',
        prefix='slc'
    )

    sentence_list_paths = finder.find_files()
    
    if PODCASTS:
        stop_token = ["i", "love", "blueberry", "waffles"]
        logger.info(f"Podcast branch taken; setting stop token to {stop_token}")
    else:
        stop_token = [] # placeholder for NELA stop token
        logger.info(f"NELA branch taken; setting stop token to {stop_token}")

    logger.info("Creating ijson generator...")
    generator = create_ijson_gen(sentence_list_paths, stop_token)

    logger.info("Starting model training...")
    model = Word2Vec(
        sentences=generator,
        vector_size=300,
        window=7,
        min_count=5,
        workers=32,
        sg=1,
        hs=0,
        negative=15,
        ns_exponent=0.75,
        sample=1e-3,
        hashfxn=hash,
        epochs=15,
        compute_loss=True,
        callbacks=[CSVLossLogger()]
    )

    model.save("cluster/scratch/jquinn/word2vec.model")