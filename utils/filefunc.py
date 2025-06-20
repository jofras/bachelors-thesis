from pathlib import Path
import shutil
import logging
from typing import List, Optional, Iterator

# for SentenceListCreator
import re
import json
import spacy
from spacy.tokens import Doc

# for TextCleaner
import contractions

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FileFunction")

class FileFunction:

    # base class for file functions mapping input to output files
    # only supports 1-to-1 mapping – any aggregations aren't supported (build FileAggregator?)

    def __init__(self,
                 input_extension: Optional[str] = None, # these extensions include the dot, e.g. '.txt'
                 output_extension: Optional[str] = None
                 ) -> None:
        
        self.input_file_path = None # both paths to be set by processor
        self.output_file_path = None
        self.input_extension = input_extension
        self.output_extension = output_extension


    def apply(self) -> Path:

        # assumes input and output file paths are valid (should be given FileProcessor)
        # validates arguments and applies the file operation
        # returns the output file path
        
        # check file extensions
        if not self.check_extensions():
            logger.error(f"Extension mismatch: Expected {self.input_extension} for input, {self.output_extension} for output")
            raise TypeError(f"Extension mismatch: Expected {self.input_extension} for input, {self.output_extension} for output")

        try:
            # these logging wrappers may be duplicate, check next time you run sth
            logger.info(f"Applying operation from {self.input_file_path} to {self.output_file_path}")
            self.map()
            logger.info("Operation completed successfully")
            return self.output_file_path
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise Exception(f"An unexpected error occurred: {e}")
        

    def map(self) -> None:
        
        # transforms input to output file (default: copy – to be overridden in derived classes)
        # assumes input and output files are valid

        logger.info(f"Copying file from {self.input_file_path} to {self.output_file_path}")
        shutil.copy2(self.input_file_path, self.output_file_path) # default implementation


    def check_extensions(self):

        # check if input file paths match the extensions the class is expecting

        input_match = self.input_extension is None or self.input_file_path.suffix == self.input_extension
        output_match = self.output_extension is None or self.output_file_path.suffix == self.output_extension
        return input_match and output_match


class SentenceListCreator(FileFunction):

    # transforms a text file into a json containing a list of lists of sentence tokens (as specified by gensim's w2v)
    # e.g. "I like hot chocolate. Do you?" -> [['i', 'like', 'hot', 'chocolate'], ['do', 'you']]

    # json output for later ijson generation
    # you can use named entity recognition if you want (includes replacing e.g. ordinals like "first" with <ordinal>)

    def __init__(self,
                 ner: bool = False,
                 nod: bool = False,
                 chunk_size: Optional[int] = 500000,
                 batch_size: Optional[int] = 1,
                 n_process: Optional[int] = 4
                 ) -> None:

        self.input_extension = ".txt"
        self.output_extension = ".json"
        super().__init__(self.input_extension, self.output_extension)

        self.ner = ner
        self.nod = nod
        self.MERGE_LABELS = {"PERSON", "ORG", "GPE", "LOC", "FAC", "PRODUCT", "EVENT", "LAW", "WORK_OF_ART", "LANGUAGE"}
        self.REPLACE_LABELS = {
            "CARDINAL": "<cardinal>",
            "ORDINAL": "<ordinal>",
            "MONEY": "<money>",
            "PERCENT": "<percent>", 
            "QUANTITY": "<quantity>",
            "DATE": "<date>",
            "TIME": "<time>"
        }

        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info(f"SpaCy model ({self.nlp}) loaded successfully")
        except Exception as e:
                logger.error(f"Error loading SpaCy model: {e}")
                raise

        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.n_process = n_process
        
        self.sentence_list = []      


    def map(self) -> None:

        # see class description  
        
        try: 
            logger.info(f"Processing text file: {self.input_file_path}")
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file:
                text = input_file.read()
            
                chunks = self.chunk(text, self.chunk_size)
                
                if self.ner and not self.nod: # ner, with disablements
                    docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer'], 
                                         batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize_ner(docs, len(chunks))
                elif self.ner and self.nod: # ner, no disablements
                    docs = self.nlp.pipe(chunks, batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize_ner(docs, len(chunks))
                elif not self.ner and not self.nod: # no ner, with disablements
                    docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner'],
                             batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize(docs, len(chunks))
                else: # no ner, no disablements
                    docs = self.nlp.pipe(chunks, batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize(docs, len(chunks))

            logger.info(f"Writing {len(self.sentence_list)} sentences to json file")
            try:
                with open(self.output_file_path, 'w') as output_file:
                    json.dump(self.sentence_list, output_file, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to write output file: {e}")
                raise
        
        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            raise


    def chunk(self, text: str, chunk_size: int = 500000):

        text_chunks = []
        start = 0
        
        while start < len(text):
            end = min(start + chunk_size, len(text))
            
            if end < len(text):
                for boundary in ['\n\n', '\n', '. ', '? ', '! ']:
                    pos = text.rfind(boundary, start, end)
                    if pos > start:
                        end = pos + len(boundary)
                        break
            
            text_chunks.append(text[start:end])
            start = end

        return text_chunks


    def tokenize_ner(self, docs: Iterator[Doc], num_chunks: int) -> List[List[str]]:
        
        # returns sentence list, but with named entities as single tokens
        # Lebron James -> lebron_james

        sentences = []

        for j, doc in enumerate(docs):
            logger.info(f"Processing chunk {j} of {num_chunks}.") # num_chunks is just for visualizing speed
            try:
                for sentence in doc.sents:

                    sentence_words = []
                    i = 0

                    while i < len(sentence):
                        token = sentence[i]

                        if token.ent_iob_ == 'B':
                            ent_type = token.ent_type_
                            ent_tokens = [token.norm_.lower()]
                            i += 1
                            while i < len(sentence) and sentence[i].ent_iob_ == 'I':
                                ent_tokens.append(sentence[i].norm_.lower())
                                i += 1

                            if ent_type in self.MERGE_LABELS:
                                sentence_words.append("_".join(ent_tokens))
                            elif ent_type in self.REPLACE_LABELS:
                                sentence_words.append(self.REPLACE_LABELS[ent_type])
                            else:
                                sentence_words.extend(ent_tokens)
                            
                        elif token.ent_iob_ == 'O':
                            if token.is_alpha:
                                norm = token.norm_.lower()
                                sentence_words.extend(norm.split())
                            i += 1
                        else:
                            logger.error(f"Unexpected IOB tag '{token.ent_iob_}' at token '{token.text}'")
                            raise ValueError(f"Unexpected IOB tag '{token.ent_iob_}'")
                    
                    if sentence_words: # fix
                        sentences.append(sentence_words)
                
                logger.info(f"Successfully processed chunk {j}")
                    
            except Exception as e:
                logger.error(f"Error processing chunk {j}: {e}")
                raise

        return sentences
    

    def tokenize(self, docs: Iterator[Doc], num_chunks: int) -> List[List[str]]:
        
        # returns said sentence list

        sentences = []

        for i, doc in enumerate(docs):
            logger.info(f"Processing chunk {i} of {num_chunks}.") # num_chunks is just for visualizing speed
            try:
                for sentence in doc.sents:
                    
                    # tokenize sentence
                    sentence_words = [token.text.lower() for token in sentence if token.is_alpha]
                    if sentence_words:
                        sentences.append(sentence_words)
                
                logger.info(f"Successfully processed chunk {i}")
                    
            except Exception as e:
                logger.error(f"Error tokenizing chunk: {e}")
                raise

        
        return sentences
    
class SentenceListCreator2(FileFunction):

    # transforms a text file into a json containing a list of lists of sentence tokens (as specified by gensim's w2v)
    # e.g. "I like hot chocolate. Do you?" -> [['i', 'like', 'hot', 'chocolate'], ['do', 'you']]

    # json output for later ijson generation
    # you can use named entity recognition if you want (includes replacing e.g. ordinals like "first" with <ordinal>)

    def __init__(self,
                 ner: bool = False,
                 nod: bool = False,
                 chunk_size: Optional[int] = 500000,
                 batch_size: Optional[int] = 1,
                 n_process: Optional[int] = 4
                 ) -> None:

        self.input_extension = ".txt"
        self.output_extension = ".json"
        super().__init__(self.input_extension, self.output_extension)

        self.ner = ner
        self.nod = nod
        self.MERGE_LABELS = {"PERSON", "ORG", "GPE", "LOC", "FAC", "PRODUCT", "EVENT", "LAW", "WORK_OF_ART", "LANGUAGE"}
        self.REPLACE_LABELS = {
            "CARDINAL": "<cardinal>",
            "ORDINAL": "<ordinal>",
            "MONEY": "<money>",
            "PERCENT": "<percent>", 
            "QUANTITY": "<quantity>",
            "DATE": "<date>",
            "TIME": "<time>"
        }

        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info(f"SpaCy model ({self.nlp}) loaded successfully")
        except Exception as e:
                logger.error(f"Error loading SpaCy model: {e}")
                raise

        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.n_process = n_process
        
        self.sentence_list = []      


    def map(self) -> None:

        # see class description  
        
        try: 
            logger.info(f"Processing text file: {self.input_file_path}")
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file:
                text = input_file.read()
            
                chunks = self.chunk(text, self.chunk_size)
                
                if self.ner and not self.nod: # ner, with disablements
                    docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer'], 
                                         batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize_ner(docs, len(chunks))
                elif self.ner and self.nod: # ner, no disablements
                    docs = self.nlp.pipe(chunks, batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize_ner(docs, len(chunks))
                elif not self.ner and not self.nod: # no ner, with disablements
                    docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner'],
                             batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize(docs, len(chunks))
                else: # no ner, no disablements
                    docs = self.nlp.pipe(chunks, batch_size=self.batch_size, n_process=self.n_process)
                    self.sentence_list = self.tokenize(docs, len(chunks))

            logger.info(f"Writing {len(self.sentence_list)} sentences to json file")
            try:
                with open(self.output_file_path, 'w') as output_file:
                    json.dump(self.sentence_list, output_file, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to write output file: {e}")
                raise
        
        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            raise


    def chunk(self, text: str, chunk_size: int = 500000):

        text_chunks = []
        start = 0
        
        while start < len(text):
            end = min(start + chunk_size, len(text))
            
            if end < len(text):
                for boundary in ['\n\n', '\n', '. ', '? ', '! ']:
                    pos = text.rfind(boundary, start, end)
                    if pos > start:
                        end = pos + len(boundary)
                        break
            
            text_chunks.append(text[start:end])
            start = end

        return text_chunks


    def tokenize_ner(self, docs: Iterator[Doc], num_chunks: int) -> List[List[str]]:
        
        # returns sentence list, but with named entities as single tokens
        # Lebron James -> lebron_james

        sentences = []

        for j, doc in enumerate(docs):
            logger.info(f"Processing chunk {j} of {num_chunks}.") # num_chunks is just for visualizing speed
            try:
                for sentence in doc.sents:

                    sentence_words = []
                    i = 0

                    while i < len(sentence):
                        token = sentence[i]

                        if token.ent_iob_ == 'B':
                            ent_type = token.ent_type_
                            ent_tokens = [token.norm_.lower()]
                            i += 1
                            while i < len(sentence) and sentence[i].ent_iob_ == 'I':
                                ent_tokens.append(sentence[i].norm_.lower())
                                i += 1

                            if ent_type in self.MERGE_LABELS:
                                sentence_words.append("_".join(ent_tokens))
                            elif ent_type in self.REPLACE_LABELS:
                                sentence_words.append(self.REPLACE_LABELS[ent_type])
                            else:
                                sentence_words.extend(ent_tokens)
                            
                        elif token.ent_iob_ == 'O':
                            if token.is_alpha:
                                norm = token.norm_.lower()
                                sentence_words.extend(norm.split())
                            i += 1
                        else:
                            logger.error(f"Unexpected IOB tag '{token.ent_iob_}' at token '{token.text}'")
                            raise ValueError(f"Unexpected IOB tag '{token.ent_iob_}'")
                    
                    if sentence_words: # fix
                        sentences.append(sentence_words)
                
                logger.info(f"Successfully processed chunk {j}")
                    
            except Exception as e:
                logger.error(f"Error processing chunk {j}: {e}")
                raise

        return sentences
    

    def tokenize(self, docs: Iterator[Doc], num_chunks: int) -> List[List[str]]:
        
        # returns said sentence list

        sentences = []

        for i, doc in enumerate(docs):
            logger.info(f"Processing chunk {i} of {num_chunks}.") # num_chunks is just for visualizing speed
            try:
                for sentence in doc.sents:
                    
                    # tokenize sentence
                    sentence_words = [token.text.lower() for token in sentence if token.is_alpha]
                    if sentence_words:
                        sentences.append(sentence_words)
                
                logger.info(f"Successfully processed chunk {i}")
                    
            except Exception as e:
                logger.error(f"Error tokenizing chunk: {e}")
                raise

        
        return sentences
    

class SentenceListCreator(FileFunction):

    # transforms a text file into a json containing a list of lists of sentence tokens (as specified by gensim's w2v)
    # e.g. "I like hot chocolate. Do you?" -> [['i', 'like', 'hot', 'chocolate'], ['do', 'you']]

    # json output for later ijson generation
    # you can use named entity recognition if you want (includes replacing e.g. ordinals like "first" with <ordinal>)

    def __init__(self,
                 ner: bool = False,
                 chunk_size: Optional[int] = 500000,
                 batch_size: Optional[int] = 1,
                 n_process: Optional[int] = 4
                 ) -> None:

        self.input_extension = ".txt"
        self.output_extension = ".json"
        super().__init__(self.input_extension, self.output_extension)

        self.ner = ner # whether to use spacy's named entity recognition
        self.MERGE_LABELS = {"PERSON", "ORG", "GPE", "LOC", "FAC", "PRODUCT", "EVENT", "LAW", "WORK_OF_ART", "LANGUAGE"}
        self.REPLACE_LABELS = {
            "CARDINAL": "<cardinal>",
            "ORDINAL": "<ordinal>",
            "MONEY": "<money>",
            "PERCENT": "<percent>", 
            "QUANTITY": "<quantity>",
            "DATE": "<date>",
            "TIME": "<time>"
        }

        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info(f"SpaCy model ({self.nlp}) loaded successfully")
        except Exception as e:
                logger.error(f"Error loading SpaCy model: {e}")
                raise

        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.n_process = n_process
        
        self.sentence_list = []      


    def map(self) -> None:

        # see class description  
        
        try: 
            logger.info(f"Processing text file: {self.input_file_path}")

            with open(self.input_file_path, 'r', encoding='utf-8') as f:
                total_lines = sum(1 for _ in f)
            logger.info(f"Total lines in file: {total_lines}")

            with open(self.input_file_path, 'r', encoding='utf-8') as input_file:
                
                for i, line in enumerate(input_file):
            
                    chunks = self.chunk(line, self.chunk_size)
                    
                    if self.ner: # ner
                        docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer'], 
                                            batch_size=self.batch_size, n_process=self.n_process)
                        self.sentence_list.extend(self.tokenize_ner(docs, i, total_lines, len(chunks)))
                    else: # no ner
                        docs = self.nlp.pipe(chunks, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner'],
                                batch_size=self.batch_size, n_process=self.n_process)
                        self.sentence_list.extend(self.tokenize(docs, i, total_lines, len(chunks)))
                    
                    self.sentence_list.append(["i", "love", "blueberry", "waffles"])

            logger.info(f"Writing {len(self.sentence_list)} sentences to json file")
            try:
                with open(self.output_file_path, 'w') as output_file:
                    json.dump(self.sentence_list, output_file, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to write output file: {e}")
                raise
        
        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            raise


    def chunk(self, text: str, chunk_size: int = 500000):

        text_chunks = []
        start = 0
        
        while start < len(text):
            end = min(start + chunk_size, len(text))
            
            if end < len(text):
                for boundary in ['\n\n', '\n', '. ', '? ', '! ']:
                    pos = text.rfind(boundary, start, end)
                    if pos > start:
                        end = pos + len(boundary)
                        break
            
            text_chunks.append(text[start:end])
            start = end

        return text_chunks


    def tokenize_ner(self, docs: Iterator[Doc], line_num: int, num_lines: int, num_chunks: int) -> List[List[str]]:
        
        # returns sentence list, but with named entities as single tokens
        # Lebron James -> lebron_james

        sentences = []

        for j, doc in enumerate(docs):
            logger.info(f"Processing chunk {j} of {num_chunks} in line {line_num} of {num_lines}.") # num_chunks is just for visualizing speed
            try:
                for sentence in doc.sents:

                    sentence_words = []
                    i = 0

                    while i < len(sentence):
                        token = sentence[i]

                        if token.ent_iob_ == 'B':
                            ent_type = token.ent_type_
                            ent_tokens = [token.norm_.lower()]
                            i += 1
                            while i < len(sentence) and sentence[i].ent_iob_ == 'I':
                                ent_tokens.append(sentence[i].norm_.lower())
                                i += 1

                            if ent_type in self.MERGE_LABELS:
                                sentence_words.append("_".join(ent_tokens))
                            elif ent_type in self.REPLACE_LABELS:
                                sentence_words.append(self.REPLACE_LABELS[ent_type])
                            else:
                                sentence_words.extend(ent_tokens)
                            
                        elif token.ent_iob_ == 'O':
                            if token.is_alpha:
                                norm = token.norm_.lower()
                                sentence_words.extend(norm.split())
                            i += 1
                        else:
                            logger.error(f"Unexpected IOB tag '{token.ent_iob_}' at token '{token.text}'")
                            raise ValueError(f"Unexpected IOB tag '{token.ent_iob_}'")
                    
                    if sentence_words: # fix
                        sentences.append(sentence_words)
                
                logger.info(f"Successfully processed chunk {j}")
                    
            except Exception as e:
                logger.error(f"Error processing chunk {j}: {e}")
                raise

        return sentences
    

    def tokenize(self, docs: Iterator[Doc], line_num: int, num_lines: int, num_chunks: int) -> List[List[str]]:
        sentences = []

        for i, doc in enumerate(docs):
            logger.info(f"Processing chunk {i} of {num_chunks} in line {line_num} of {num_lines}.")
            try:
                for sentence in doc.sents:
                    sentence_words = []
                    for token in sentence:
                        if token.is_alpha:
                            sentence_words.extend(token.norm_.lower().split())  # flattening
                    if sentence_words:
                        sentences.append(sentence_words)
                logger.info(f"Successfully processed chunk {i}")
            except Exception as e:
                logger.error(f"Error tokenizing chunk: {e}")
                raise

        return sentences



class EntrySimplifier(FileFunction):

    # simplifies jsonl entries by keeping only specified fields
    # assumes jsonl input and outputs to either jsonl or text

    def __init__(self,
                 keep_fields: List[str], # which fields to keep
                 output_extension: str = ".jsonl", # could be .txt too
                 keep_labels: bool = True # whether to keep field labels
                 ) -> None:

        # initialize EntrySimplifier

        self.input_extension = ".jsonl"

        if output_extension not in [".txt", ".jsonl"]:
            logger.error(f"Invalid output extension: {output_extension}")
            raise TypeError("Output must be .txt or .jsonl file.")
        else: 
            self.output_extension = output_extension
        
        super().__init__(self.input_extension, self.output_extension)

        if keep_fields == [] or keep_fields is None:
            logger.error("No fields to keep specified.")
            raise AttributeError("Please specify at least one field to keep.")
        self.keep_labels = keep_labels


    def map(self) -> None:

        # processes each line of the jsonl file, keeping only specified fields 
        # for jsonl output -> simplified json objects
        # for txt output -> formatted strings with field values
        
        try:
            with open(self.input_file_path, 'r') as infile, open(self.output_file_path, 'w') as outfile:
                
                line_count = 0
                for line in infile: 
                    
                    try:
                        data = json.loads(line)
                        line_count += 1

                        valid_fields = []
                        for field in self.keep_fields:
                            if field in data:
                                valid_fields.append(field)
                            else: 
                                logger.warning(f"Field '{field}' not found in line {line_count}")
                        
                        # case distinction
                        if self.output_extension == ".jsonl":
                            if self.labels:
                                simplified_entry = {field: data[field] for field in valid_fields}
                            else:
                                simplified_entry = [data[field] for field in valid_fields] # list instead of set
                            outfile.write(json.dumps(simplified_entry, ensure_ascii=False) + '\n')
                        
                        else: # .txt
                            if self.labels:
                                simplified_entry = ", ".join([f"{field}: {data[field]}" for field in valid_fields])
                            else: 
                                simplified_entry = ", ".join([str(data[field]) for field in valid_fields])
                            outfile.write(simplified_entry + '\n')
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid json line in line {line_count}: {e}")
                    except Exception as e:
                        logger.error(f"Error processing line {line_count}: {e}")

            logger.info(f"Processed {line_count} entries")

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise


class TextCleaner(FileFunction):

    # can remove all sorts of non-speaker content (marked all sorts of ways) and extend contractions
    # haven't implemented the highest contraction level yet, as i'm having problems downloading required packages
    
    # assumes both .txt input and output

    def __init__(self,
                 remove_non_speaker_content: bool = True,
                 contraction_level: int = 1
                 ) -> None:
        
        self.input_extension = ".txt"
        self.output_extension = ".txt"

        super().__init__(self.input_extension, self.output_extension)

        self.remove_non_speaker_content = remove_non_speaker_content
        
        if contraction_level >= 0 and contraction_level <= 2:
            self.contraction_level = contraction_level
        else:
            logger.error(f"Contraction level must be 0 (no contraction expansion), 1 (using contractions) or 2 (using pycontractions), got {contraction_level}")
            raise ValueError(f"Contraction level must be between 0 and 2")


    def map(self) -> None:
        
        # see class definition above 

        # try: 
        #     logger.info(f"Processing text file: {self.input_file_path}")
        #     with open(self.input_file_path, 'r', encoding='utf-8') as input_file, open(self.output_file_path, 'w', encoding='utf-8') as output_file:
        #         text = input_file.read()
                
        #         if self.remove_non_speaker_content:
        #             logger.info("Removing non-speaker content")
        #             text = re.sub(r'\[.*?\]', '', text) # remove [text]
        #             text = re.sub(r'\(.*?\)', '', text) # remove (text)
        #             text = re.sub(r'\{.*?\}', '', text) # remove {text}
        #             text = re.sub(r'<.*?>', '', text) # remove <text>
        #             text = re.sub(r'\s\*[a-zA-Z\s]+\*\s', ' ', text) # remove *text* -> spaces to avoid removing profanities
        #             text = re.sub(r'\s/+\w+/+\s', '', text) # remove //text/ (variable number of slashes on either side)
        #             text = re.sub(r'~.*?~', '', text) # remove tilde tags (uncommon)
        #             text = re.sub(r'\\', '', text) # remove backslashes (escape characters)
        #             # removing backslashes is needed to ensure contraction expansion works properly,
        #             # as apostrophes are often escaped in this dataset

        #         if self.contraction_level == 1:
        #             logger.info("Expanding contractions naively")
        #             text = contractions.fix(text)
        #         elif self.contraction_level == 2:
        #             logger.error("'contraction_level == 2' hasn't been implemented yet.")
        #             raise

        #         logger.info(f"Writing {len(text)} characters to text file")
        #         output_file.write(text)
        
        # except Exception as e:
        #     logger.error(f"Error processing text file: {e}")
        #     raise

        try:
            logger.info(f"Processing text file: {self.input_file_path}")
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file, \
                 open(self.output_file_path, 'w', encoding='utf-8') as output_file:

                for line_num, line in enumerate(input_file, 1): # Process line by line
                    # 1. Remove the trailing newline character for processing
                    # This ensures regexes don't accidentally match across lines and
                    # that we control the final newline.
                    processed_line_content = line.rstrip('\n')

                    if self.remove_non_speaker_content:
                        # logger.debug(f"Before removal: '{processed_line_content}'") # For detailed debugging
                        
                        # Apply regexes to the current line content
                        # Replace with a single space to prevent words from concatenating
                        processed_line_content = re.sub(r'\[.*?\]', ' ', processed_line_content) # [text]
                        processed_line_content = re.sub(r'\(.*?\)', ' ', processed_line_content) # (text)
                        processed_line_content = re.sub(r'\{.*?\}', ' ', processed_line_content) # {text}
                        processed_line_content = re.sub(r'<.*?>', ' ', processed_line_content) # <text>
                        
                        # Note: \s will match spaces, tabs, newlines.
                        # Since we've rstrip('\n')ed the line, \s will primarily match spaces/tabs within the line.
                        # The original regex \s\*...*\s is specifically designed for content with *leading and trailing spaces*.
                        processed_line_content = re.sub(r'\s\*[a-zA-Z\s]+\*\s', ' ', processed_line_content) # *text*
                        
                        # These now replace with a space to prevent concatenation
                        processed_line_content = re.sub(r'\s/+\w+/+\s', ' ', processed_line_content) # //text/
                        processed_line_content = re.sub(r'~.*?~', ' ', processed_line_content) # ~text~
                        
                        processed_line_content = re.sub(r'\\', '', processed_line_content) # remove backslashes (escape characters)
                        
                        # Remove greater and less than symbols
                        processed_line_content = re.sub(r'>+', ' ', processed_line_content) # remove sequences of >
                        processed_line_content = re.sub(r'<+', ' ', processed_line_content) # remove sequences of <

                        # Clean up multiple spaces that might result from replacements, and trim leading/trailing spaces
                        processed_line_content = re.sub(r'\s+', ' ', processed_line_content).strip()
                        # logger.debug(f"After removal: '{processed_line_content}'") # For detailed debugging

                    if self.contraction_level == 1:
                        # logger.debug(f"Before contractions: '{processed_line_content}'") # For detailed debugging
                        processed_line_content = contractions.fix(processed_line_content)
                        # logger.debug(f"After contractions: '{processed_line_content}'") # For detailed debugging
                    elif self.contraction_level == 2:
                        logger.error("'contraction_level == 2' hasn't been implemented yet.")
                        raise NotImplementedError("'contraction_level == 2' hasn't been implemented yet.")

                    # Decide what to write for this line
                    # To preserve line count even for lines that become empty:
                    # output_file.write(processed_line_content + '\n')
                    
                    # If you want to completely remove lines that become empty/whitespace-only:
                    if processed_line_content: # Only write if there's actual content
                        output_file.write(processed_line_content + '\n')
                    else:
                        # Optionally log if a line was removed
                        logger.info(f"Skipping empty line {line_num} in {self.input_file_path}")


            logger.info(f"Finished processing file: {self.input_file_path}")

        except Exception as e:
            logger.error(f"Error processing text file {self.input_file_path}: {e}")
            raise


class StopTokenAppender(FileFunction):

    # appends a custom stop token to each line of a text file (default: "I love blueberry waffles")
    # assumes .txt input and output

    # why? -> tokenizing removes newline tokens, but glove (and hallucination analysis) needs someway of recognizing podcast boundaries
        
    def __init__(self,
                 stop_token: str = "I love blueberry waffles." # this is making me hungry
                 ) -> None:
        
        self.input_extension = ".txt"
        self.output_extension = ".txt"

        super().__init__(self.input_extension, self.output_extension)

        self.stop_token = stop_token # please let this work


    def map(self): 
        
        try: 
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file, open(self.output_file_path, 'w', encoding='utf-8') as output_file:

                for line in input_file:
                    
                    cleaned_line_content = line.strip()
                    if not (cleaned_line_content.endswith('.') or 
                            cleaned_line_content.endswith('!') or 
                            cleaned_line_content.endswith('?')):
                        cleaned_line_content += '.'

                    modified_line = f"{cleaned_line_content} {self.stop_token}\n"
                    
                    output_file.write(modified_line)

        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            raise


class GloVeFormatter(FileFunction):

    # expects a json file containing word2vec-formatted sentence lists and outputs a text file

    def __init__(self,
                 stop_token: str = "eopc" # "end of podcast"
                ) -> None:
        
        self.input_extension = ".json"
        self.output_extension = ".txt"

        super().__init__(
            self.input_extension, 
            self.output_extension
        )

        self.stop_token = stop_token


    def map(self) -> None: 

        try: 
            logger.info(f"Processing json file: {self.input_file_path}")

            with open(self.input_file_path, 'r', encoding='utf-8') as input_file, open(self.output_file_path, 'w', encoding='utf-8') as output_file:
            
                data = json.load(input_file)  # load json (should be a list of lists)

                if not isinstance(data, list) or not all(isinstance(sentence, list) for sentence in data):
                    raise ValueError("json file must contain a list of lists (tokenized sentences).")

                for sentence in data:
                    if sentence == [self.stop_token]:  # skip lines that are just ["eopc"]
                        output_file.write("\n")  # add newline
                    elif sentence != []:
                        output_file.write(" ".join(sentence) + " ")  # write words as a space-separated line
        
                logger.info(f"Finished processing. Output saved to: {self.output_file_path}")
                
        
        except Exception as e:
            logger.error(f"Error processing json file: {e}")
            raise
