from pathlib import Path
import shutil
import logging
from typing import List, Optional, Dict, Any, Union

# for SentenceListCreator
import re
import json
import spacy
from spacy.tokens import Token, Doc
from spacy.language import Language
import itertools

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FileFunction")

class FileFunction:

    """
    Base class for file operations that transform input files to output files. 

    Attributes: 
        input_file_path (Path): Path to the input file.
        output_file_path (Path): Path where the output file will be saved. 
        input_extension (str): Expected extension of the input file.
        output_extension (str): Expected extension of the output file.
    """

    def __init__(self,
                 input_file_path: Optional[Union[str, Path]] = None,
                 output_file_path: Optional[Union[str, Path]] = None,
                 input_extension: Optional[str] = None,
                 output_extension: Optional[str] = None
                 ) -> None:
        
        """
        Initialize the FileFunction.

        Args: 
            input_file_path: Path to the input file.
            output_file_path: Path where the output will be saved.
            input_extension: Expected extension of the input file. 
            output_extension: Expected extension of the output file. 
        """

        self.input_file_path = Path(input_file_path) if input_file_path and isinstance(input_file_path, str) else input_file_path
        self.output_file_path = Path(output_file_path) if output_file_path and isinstance(output_file_path, str) else output_file_path
        self.input_extension = input_extension
        self.output_extension = output_extension


    def apply(self) -> Path:

        """
        Apply the file operation, transforming input into output.

        Returns: 
            Path: Path to the output file.

        Raises: 
            AttributeError: If input or output path is not initialized. 
                (Might be worth changing -> what if you only want to process a single file? Should you really need to call the FileProcessor?) 
            TypeError: If paths are not Path objects or extensions don't match. 
            FileNotFoundError: If input file doesn't exist. 
            PermissionError: If there are permission issues. 
            Exception: For other unexpected errors.
        """

        # validate input and output paths
        if self.input_file_path is None:
            logger.error("The input file path is uninitialized")
            raise AttributeError("The input file path is uninitialized")
        
        if self.output_file_path is None:
            logger.error("The output file path is uninitialized")
            raise AttributeError("The output file path is uninitialized")
        
        if not isinstance(self.input_file_path, Path):
            logger.error("Input file path must be a pathlib.Path object")
            raise TypeError("Input file path must be a pathlib.Path object")
        
        if not isinstance(self.output_file_path, Path):
            logger.error("Output file path must be a pathlib.Path object")
            raise TypeError("Output file path must be a pathlib.Path object")
        
        # check if input file exists
        if not self.input_file_path.exists():
            logger.error(f"Input file does not exist: {self.input_file_path}")
            raise FileNotFoundError(f"Input file not found: {self.input_file_path}")
        
        # check file extensions
        if not self.check_extensions():
            logger.error(f"Extension mismatch: Expected {self.input_extension} for input, {self.output_extension} for output")
            raise TypeError(f"Extension mismatch: Expected {self.input_extension} for input, {self.output_extension} for output")
        
        # create output directory if it doesn't exist -> do this in FileProcessor wrapper
        try: 
            if not self.output_file_path.parent.exists():
                self.output_file_path.parent.mkdir(parents=True, exist_ok=True) 
                logger.info(f"Created output directory: {self.output_file_path.parent}")
        except PermissionError:
            logger.error(f"Permission error when creating directory: {self.output_file_path.parent}")
            raise

        try:
            logger.info(f"Applying operation from {self.input_file_path} to {self.output_file_path}")
            self.map()
            logger.info("Operation completed successfully")
            return self.output_file_path

        except FileNotFoundError:
            logger.error(f"Input file not found: {self.input_file_path}")
            raise FileNotFoundError(f"Input file not found: {self.input_file_path}")
        except PermissionError:
            logger.error(f"Permission error when processing file: {self.input_file_path}")
            raise PermissionError(f"Permission error when processing file: {self.input_file_path}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise Exception(f"An unexpected error occurred: {e}")
        

    def map(self) -> None:
        
        """
        Transform input file into output file. Default implementation copies the file. 

        Override this method in derived classes for specific transformations. 

        Assumes self.input_file_path and self.output_file_path exist and are valid. 
        """

        logger.info(f"Copying file from {self.input_file_path} to {self.output_file_path}")
        shutil.copy2(self.input_file_path, self.output_file_path) # default implementation


    def check_extensions(self):

        """
        Check if file extensions match the expected extensions. 

        Returns:
            bool: True if extensions match, False otherwise. 
        """

        input_match = self.input_extension is None or self.input_file_path.suffix == self.input_extension
        output_match = self.output_extension is None or self.output_file_path.suffix == self.output_extension
        return input_match and output_match



class SentenceListCreator(FileFunction):

    """
    Transforms a text file into a json file containing a list of tokenized sentences. 

    Assumes a .txt input file and a .json output file. 
    Can optionally remove bracketed content and recognize named entities. 

    Attributes: 
        remove_brackets (bool): Whether to remove content in brackets. 
        recognize_named_entities (bool): Whether to recognize named entities. 
        sentence_list (List): List to store tokenized sentences. 
        nlp (spacy.Language): SpaCy language model for text processing. 
    """

    def __init__(self,
                 input_file_path: Union[str, Path],
                 output_file_path: Union[str, Path],
                 remove_bracketed_content: bool = True,
                 recognize_named_entities: bool = False,
                 sentence_list: Optional[List[List[str]]] = None,
                 nlp: Optional[Language] = None
                 ) -> None:
        
        """
        Initialize the SentenceListCreator.

        Args:
            input_file_path: Path to the input text file.
            output_file_path: Path where the output json will be saved.
            remove_bracketed_content: Whether to remove text in brackets. 
            recognize_named_entities: Whether to identify named entities.
            sentence_list: Optional pre-existing list to store results.
            nlp: SpaCy language model (loads en_core_web_sm by default). 
        """

        self.input_extension = ".txt"
        self.output_extension = ".json"
        super().__init__(
            Path(input_file_path) if isinstance(input_file_path, str) else input_file_path, 
            Path(output_file_path) if isinstance(output_file_path, str) else output_file_path, 
            self.input_extension, 
            self.output_extension
        )
        self.remove_brackets = remove_bracketed_content
        self.recognize_named_entities = recognize_named_entities
        self.sentence_list = sentence_list if sentence_list is not None else []
        
        try:
            self.nlp = nlp if nlp is not None else spacy.load("en_core_web_sm")
            logger.info("SpaCy model loaded successfully")
        except Exception as e:
            logger.error(f"Error loading SpaCy model: {e}")
            raise


    def map(self) -> None:

        """
        Process text file into tokenized sentences and save as json.

        Removes bracketed content and processes named entities if specified.
        """
        try: 
            logger.info(f"Processing text file: {self.input_file_path}")
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file:
                text = input_file.read()
                
                if self.remove_brackets:
                    logger.info("Removing bracketed content")
                    text = re.sub(r'\[.*?\]', '', text) # remove [text]
                    text = re.sub(r'\(.*?\)', '', text) # remove (text)
                    text = re.sub(r'\{.*?\}', '', text) # remove {text}

                if self.recognize_named_entities:
                    logger.info("Started creating doc ('ner' enabled)")
                    doc_gen = self.nlp.pipe(text, disable=['tok2vec', 'tagger', 'attribute_ruler', 'lemmatizer'], n_process=6) # parser + ner to recognize named entities
                    logger.info("Turning doc into sentence list")
                    self.sentence_list = list(itertools.chain.from_iterable(self.tokenize_with_named_entities(doc) for doc in doc_gen))
                else:
                    logger.info("Start creating doc")
                    doc_gen = self.nlp.pipe(text, disable=['tok2vec', 'tagger', 'attribute_ruler', 'lemmatizer', 'ner'], n_process=6) # parser to recognize sentence boundaries
                    logger.info("Turning doc into sentence list")
                    self.sentence_list = list(itertools.chain.from_iterable(self.tokenize(doc) for doc in doc_gen))

            logger.info(f"Writing {len(self.sentence_list)} sentences to json file")
            with open(self.output_file_path, 'w') as output_file:
                json.dump(self.sentence_list, output_file, ensure_ascii=False)
        
        except Exception as e:
            logger.error(f"Error processing text file: {e}")
            raise


    def tokenize_with_named_entities(self, doc: Doc) -> List[List[str]]:
        
        """
        Tokenize text into sentences while preserving named entities. 

        Args: 
            text: Input Doc to turn into sentence lists. 

        Returns: 
            List[List[str]]: List of sentences, each containing a list of tokens.

        Raises:
            ValueError: If an entity with no beginning token is found.
        """

        try:
            sentences = []

            for sentence in doc.sents:
                sentence_words = []
                i = 0

                while i < len(sentence):
                    token = sentence[i]

                    if token.ent_iob_ == 'B':
                        entity_text = token.text.lower()
                        if i + 1 < len(sentence) and sentence[i + 1].ent_iob_ == 'I':
                            i += 1
                            while i < len(sentence) and sentence[i].ent_iob_ == 'I':
                                entity_text = " ".join(entity_text, sentence[i].text.lower())
                                i += 1
                        sentence_words.append(entity_text)
                    elif token.ent_iob_ == 'O':
                        if token.is_alpha:
                            sentence_words.append(token.text.lower())
                        i += 1
                    else:
                        logger.error(f"Token inside an entity with no beginning found: {token.text}")
                        raise ValueError(f"Unexpected entity IOB tag: {token.ent_iob_}")
                
                sentences.append(sentence_words)
                        

            return sentences
        
        except Exception as e:
            logger.error(f"Error in tokenizing with named entities: {e}")
            raise

    
    def tokenize(self, doc: Doc) -> List[List[str]]:

        """
        Simple tokenization of text into sentences

        Args:
            text: Input Doc to turn into sentence lists.
        
        Returns:
            List[List[str]]: List of sentences, each containing a list of tokens.
        """
        try:
            sentences = []

            for sentence in doc.sents:
                print(f"Sentence: {sentence}")
                sentence_words = [token.text.lower() for token in sentence if token.is_alpha]
                print(sentence_words)
                sentences.append(sentence_words)

            return sentences
        except Exception as e:
            logger.error(f"Error in tokenizing: {e}")
            raise


class EntrySimplifier(FileFunction):

    """
    Simplifies entries in a jsonl file by keeping only specified fields.

    Assumes a .jsonl input file and outputs to either .jsonl or .txt.

    Attributes: 
        keep_fields (List[str]): Fields to keep from each json entry. 
        labels (bool): Whether to include field names in output.
    """

    def __init__(self,
                 input_file_path: Union[str, Path],
                 output_file_path: Union[str, Path],
                 output_extension: str = ".jsonl",
                 keep_fields: Optional[List[str]] = None,
                 labels: bool = True
                 ) -> None:
        
        """
        Initialize the EntrySimplifier. 

        Args: 
            input_file_path: Path to the input jsonl file.
            output_file_path: Path where the output will be saved.
            output_extension: Output file extension (.txt or .jsonl).
            keep_fields: List of field names to keep in each entry.
            labels: Whether to include field names in an output.
        """

        self.input_extension = ".jsonl"

        if output_extension not in [".txt", ".jsonl"]:
            logger.error(f"Invalid output extension: {output_extension}")
            raise TypeError("Output must be .txt or .jsonl file.")
        
        self.output_extension = output_extension
        super().__init__(
            Path(input_file_path) if isinstance(input_file_path, str) else input_file_path, 
            Path(output_file_path) if isinstance(output_file_path, str) else output_file_path, 
            self.input_extension, 
            self.output_extension
        )
        self.keep_fields = keep_fields if keep_fields is not None else []
        self.labels = labels

    def map(self) -> None:

        """
        Process each line of the jsonl file, keeping only specified fields.

        For .jsonl output: Creates simplified json objects.
        For .txt output: Creates formatted strings with field values.
        """
        
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


        