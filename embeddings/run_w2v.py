import gensim
from gensim.models import Word2Vec
from gensim.models.callbacks import CallbackAny2Vec
import logging
import os
import ijson
from pathlib import Path
from utils.filefinder import FileFinder
import csv
import cython

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger("run_w2v.py")

PODCASTS = 1
FILEPATH = '/cluster/scratch/jquinn/input_w2v'

class CorpusIterable:
    def __init__(self, file_paths, stop_token):
        self.file_paths = file_paths
        self.stop_token = stop_token

    def __iter__(self):
        for file_path in self.file_paths:
            file_path = Path(file_path) if isinstance(file_path, str) else file_path
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    parser = ijson.items(f, 'item')
                    for sentence in parser:
                        # Compare with stop_token list, not nested list
                        if isinstance(sentence, list) and sentence != self.stop_token and sentence != []:
                            yield sentence
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue

class CSVLossLogger(CallbackAny2Vec):
    def __init__(self, filepath="/cluster/scratch/jquinn/loss_log.csv"):
        self.epoch = 0
        self.loss_previous = 0.0
        self.filepath = filepath

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
        prefix='w2v'
    )

    sentence_list_paths = finder.find_files()

    if PODCASTS:
        stop_token = ["i", "love", "blueberry", "waffles"]
        logger.info(f"Podcast branch taken; setting stop token to {stop_token}")
    else:
        stop_token = []
        logger.info(f"NELA branch taken; setting stop token to {stop_token}")

    logger.info("Creating iterable corpus...")
    corpus = CorpusIterable(sentence_list_paths, stop_token)

    logger.info("Starting model training...")
    model = Word2Vec(
        sentences=corpus,
        vector_size=300,
        window=5, # try 7 or 10 later for more semantic capabilities
        min_count=5,
        workers=64, # cluster
        sg=1,
        hs=0,
        negative=5, # as in paper; for smaller corpora, 5-20 was deemed good
        ns_exponent=0.75,
        sample=1e-5, # aggressively subsamples frequent (i.e. stop)words
        epochs=5, # check 10 or 15 later 
        compute_loss=True,
        callbacks=[CSVLossLogger()]
    )

    model.save("/cluster/scratch/jquinn/word2vec.model")