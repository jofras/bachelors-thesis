# uncovering and mitigating sociocultural bias in public discourse datasets. 

supervised by [dr. alexander hoyle](https://alexanderhoyle.com) and [prof. ryan cotterell](https://rycolab.io/authors/ryan/).

the project is now bigger:
- the original goal of training word embeddings on a [relatively new corpus](https://arxiv.org/pdf/2411.07892) and evaluating their performance pre- and post-debiasing (using various algorithms) remains.
- because of a recent finding, i will also be investigating whether the model used for podcast transcription hallucinates context-dependently.

i am currently working on cleaning the tokenized sentences using the database. 

some bookkeeping on file types (not shared yet): 
- post_tc: files that have been "cleaned" from things like music annotation, gibberish, etc., with stop tokens appended
- post_slc: json lists in word2vec, with empty lists and potential hallucinations still therein
- post_hlc: hallucination-removed w2v files
- post_gf: glove-formatted, hallucination removed files

i've backed up all files that aren't currently in use onto my external ssd. (they're each around 11-18GB, depending on file format.)