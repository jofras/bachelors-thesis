# Welcome to my Bachelor's Thesis Repository! 

My thesis is titled "Uncovering and Mitigating Sociocultural Bias in Public Discourse Datasets" and is supervised by Alexander Hoyle and Ryan Cotterell. The goal is twofold: First, I investigate a fairly new podcast dataset, training both word2vec and gloVe models on it and evaluating the resulting embeddings' performance and bias. Second, I'll implement debiasing algorithms like Hard-Debias and INLP, run them on the embeddings, and compare results. 

## Project Hierarchy

This repository is split into five folders, each representing a different stage of the project. 

### wrangling

This folder contains all code used in data preprocessing, i.e. turning the raw podcast dataset into formats accepted by the word2vec and gloVe models. I've organised most of the code into classes.

```filefinder.py``` contains a **FileFinder** class. As the name suggests, instances of this class find all files in a given directory that match chosen criteria. 

```fileproc.py``` contains the **FileProcessor** class. This class takes a list of input files, a destination directory, and a ***FileFunction** (see next paragraph), and transforms all input files using that function, saving the output in the destination directory. 

```filefunc.py``` contains a parent class, **FileFunction**, and many subclasses that achieve different functionalities. A **FileFunction**, most generally, takes an input file path and an output file path and transforms input to output using its ```map()``` function. As of yet, I've implemented five subclasses: 

- ```EntrySimplifier``` takes a JSONL as input and gets rid of some of its fields, returning either an updated JSONL or a text file. One can choose to omit labels in the output.
- ```TextCleaner``` takes a text file and can remove bracketed content, as well as expand contractions with different degrees of intelligence. Please note that I haven't been able to get ```pycontractions``` running on my computer, so only ```contraction_level = 1``` works. 
- ```StopTokenAppender``` appends a custom stop token to the end of each line in a text file (default: ```epoc``` for *end of podcast*). See [design choices](#design-choices).
- ```SentenceListCreator``` takes a text file and creates a list of tokens for each sentence therein, combining these lists to one big list. This is required for word2vec training. 
- ```GloVeFormatter``` takes the list output by ```SentenceListCreator```, and returns a text file in GloVe input format. See [design choices](#design-choices). 

Other methods in this folder include ```merge_on_url``` and ```create_ijson_gen```. See the diagram for explanations. 

To help give an overview of the wrangling pipeline, here's a visual diagram: 

![image](https://github.com/user-attachments/assets/43e5f0e9-de5d-4749-a9e3-1f24b9f586d1)

Subclasses of ```FileFunction``` are in yellow, while other methods are colored blue. 

#### design choices

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

