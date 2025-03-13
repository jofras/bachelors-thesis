# bachelors-thesis

all the code i'm using for my bachelor's thesis, split into five folders: 

## wrangling

this folder contains all code and files related to preprocessing, i.e. the original podcast data + code to break it down into podcast transcripts. 

## embedding_gen

all code related to training embeddings, plus the generated embeddings (and models). models are described as follows: 

{acronym}_{dataset}{percentage}{preprocessing style}_{dim}_{epochs}_{vocab size}_{window size}

where 

acronym = w2v for word2vec, glv for glove + j if self-implemented
dataset = p for podcast (define more if necessary)
percentage = f (full) if all data, else percentage of dataset processed
preprocessing style = v for vanilla (no bigrams/trigrams), b for bigrams, t for trigrams
dim = vector dimension
epochs = num_epochs in training
vocab size = f (full) if all data, else specify
window size = self explanatory

## eval

everything i used to evaluate the embeddings, performance- and bias-wise. will contain plots too. 

## debiasing

all code related to debiasing the embeddings. contains algorithms as well as the debiased embeddings. 

## extras 

code that wasn't explicitly a part of the proposal. 

