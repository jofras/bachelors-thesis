from pathlib import Path
import shutil
import logging
from typing import List, Optional, Iterator, Any

# for SentenceListCreator
import re
import json
import spacy
from spacy.tokens import Doc

# for EntrySimplifier
from datetime import datetime

# for TextCleaner
import contractions

# for NewsCleaner
import xxhash

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
                        print(token)
                        print(token.ent_iob_)

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
                            elif re.fullmatch(r'[a-zA-Z]+(?:_[a-zA-Z]+)+', token.text): # see TextCleaner; removes 93_FM and stuff, is that what you want? maybe use r'\w+(?:_\w+)+' instead -> no, don't risk, not worth it
                                sentence_words.extend(token.norm_.lower().split()) # like a named entity
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
    
    def __init__(self,
                 keep_fields: List[str],
                 keep_labels: bool = True,
                 output_extension: str = ".txt",
                 nela: bool = False) -> None:
        
        if output_extension not in [".txt", ".jsonl"]:
            logger.error(f"Invalid output extension: {output_extension}")
            raise TypeError("Output must be .txt or .jsonl file.")

        if not keep_fields:
            logger.error("No fields to keep specified.")
            raise AttributeError("Please specify at least one field to keep.")

        self.keep_fields = keep_fields
        self.keep_labels = keep_labels
        self.output_extension = output_extension
        self.nela = nela
        self.date_filter = nela

        input_ext = ".json" if nela else ".jsonl"
        super().__init__(input_ext, output_extension)

    def _filter_fields(self, entry: dict, line_id: int) -> List[str]:
        valid_fields = []
        for field in self.keep_fields:
            if field in entry:
                valid_fields.append(field)
            else:
                logger.warning(f"Field '{field}' not found in entry {line_id}")
        return valid_fields

    def sanitize_field(self, field_value) -> str:
        """
        Convert field to string and remove all newline characters,
        carriage returns, and excessive whitespace to keep output
        on a single line.
        """
        if not isinstance(field_value, str):
            field_value = str(field_value)
        # Replace \r, \n, and other unicode line separators with space
        sanitized = field_value.replace('\r', ' ').replace('\n', ' ').replace('\u2028', ' ').replace('\u2029', ' ')
        # Collapse multiple spaces to one
        sanitized = ' '.join(sanitized.split())
        return sanitized

    def _write_entry(self, outfile, entry: dict, valid_fields: List[str]) -> None:
        if self.output_extension == ".jsonl":
            if self.keep_labels:
                simplified = {field: entry[field] for field in valid_fields}
            else:
                simplified = [entry[field] for field in valid_fields]
            outfile.write(json.dumps(simplified, ensure_ascii=False) + '\n')
        elif self.output_extension == ".txt":
            if self.keep_labels:
                simplified = ", ".join(f"{field}: {self.sanitize_field(entry[field])}" for field in valid_fields)
            else:
                simplified = ", ".join(self.sanitize_field(entry[field]) for field in valid_fields)
            outfile.write(simplified + '\n')
            self._txt_lines_written += 1  # Track lines written for .txt
        else:
            raise ValueError(f"Unsupported output extension: {self.output_extension}")

    def map(self) -> None:
        self._txt_lines_written = 0  # Reset counter for .txt output
        written_entries = 0  # Count entries actually written

        try:
            with open(self.input_file_path, 'r', encoding='utf-8') as infile, \
                 open(self.output_file_path, 'w', encoding='utf-8') as outfile:

                if self.date_filter:
                    try:
                        all_data = json.load(infile)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON: {e}")
                        raise

                    if not isinstance(all_data, list):
                        raise ValueError("Expected a list of JSON entries.")

                    lower_bound = datetime.strptime("2020-05-01", "%Y-%m-%d")
                    upper_bound = datetime.strptime("2020-06-30", "%Y-%m-%d")

                    total = 0
                    kept = 0
                    for i, entry in enumerate(all_data, 1):
                        total += 1
                        date_str = entry.get("date", "")[:10]
                        try:
                            date_val = datetime.strptime(date_str, "%Y-%m-%d")
                        except ValueError:
                            logger.warning(f"Invalid date in entry {i}; skipping.")
                            continue
                        if not (lower_bound <= date_val <= upper_bound):
                            continue

                        kept += 1
                        fields = self._filter_fields(entry, i)
                        self._write_entry(outfile, entry, fields)
                        written_entries += 1

                    logger.info(f"Processed {total} entries, kept {kept} after date filtering.")
                
                else:
                    line_count = 0
                    for line in infile:
                        line_count += 1
                        try:
                            entry = json.loads(line)
                            fields = self._filter_fields(entry, line_count)
                            self._write_entry(outfile, entry, fields)
                            written_entries += 1
                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON at line {line_count}: {e}")
                        except Exception as e:
                            logger.error(f"Error at line {line_count}: {e}")

                    logger.info(f"Processed {line_count} entries.")

            # Assert output line count matches processed entries for .txt files
            if self.output_extension == ".txt":
                assert self._txt_lines_written == written_entries, (
                    f"Output line count mismatch: wrote {self._txt_lines_written} lines but "
                    f"processed {written_entries} entries."
                )

        except Exception as e:
            logger.error(f"Fatal error during map: {e}")
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
                    
                    # Remove greater and less than symbols
                    processed_line_content = re.sub(r'>+', ' ', processed_line_content) # remove sequences of >
                    processed_line_content = re.sub(r'<+', ' ', processed_line_content) # remove sequences of <
                    
                    # Remove all underscores (swap for whitespace)
                    processed_line_content = processed_line_content.replace('_', ' ')
                    
                    # Remove all hyphen types (swap for spaces)
                    processed_line_content = processed_line_content.replace('-', ' ')
                    processed_line_content = re.sub(r'[–—]', ' ', processed_line_content)  # en-dash (–), em-dash (—)

                    # Remove possessive suffixes
                    # Remove singular possessive
                    processed_line_content = re.sub(r"\b(\w+)[’']s\b", r"\1", processed_line_content)
                    # Remove plural possessive, apostrophe followed by space, punctuation, or end of string
                    processed_line_content = re.sub(r"\b(\w+s)[’'](?=\s|$|[.,;:!?—…‘’“”\"\'\-\)])", r"\1", processed_line_content)
                    # Remove random apostrophes surrounded by space (might help mitigate NER problems)
                    processed_line_content = re.sub(r'(?<=\s)[\'’](?=\s)', ' ', processed_line_content)

                    # Final apostrophe rules
                    # Remove apostrophes prefixing or suffixing a word
                    processed_line_content = re.sub(r"\b[’']\s+", '', processed_line_content)  # Remove apostrophe at start of a word boundary
                    processed_line_content = re.sub(r"\s+[’']\b", '', processed_line_content)  # Remove apostrophe at end of a word boundary
                    # Replace apostrophes inside words with underscore (consistent with NER strategy)
                    processed_line_content = re.sub(r"(?<=\w)[’'](?=\w)", '_', processed_line_content)

                    # Double check underscores
                    processed_line_content = re.sub(r'_+', '_', processed_line_content)
                    # Remove stray underscores surrounded by spaces (but not inside words)
                    processed_line_content = re.sub(r'(?<=\s)_+(?=\s)', ' ', processed_line_content)

                    # Double check apostrophes
                    processed_line_content = re.sub(r"[’']", " ", processed_line_content)

                    # Remove stray brackets and braces
                    processed_line_content = re.sub(r'[\[\]\(\)\{\}<>]', ' ', processed_line_content)

                    # Remove everything but normal punctualization
                    processed_line_content = re.sub(r'[^\w\s\.\,\?\!\:\;_]', ' ', processed_line_content)

                    # Reduce sequences of punctuation marks to the first character (e.g., ".?;;.:::" -> ".") -> too aggressive, spacy doesn't care anyway
                    # def reduce_punct_seq(match):
                    #     return match.group(0)[0]

                    # processed_line_content = re.sub(r'[.,?!:;_]{2,}', reduce_punct_seq, processed_line_content)

                    # Repeated punctuation handling
                    # Replace any sequence of colons and semicolons surrounded by non-whitespace with a single space
                    processed_line_content = re.sub(r'(?<=\S)[;:]+(?=\S)', ' ', processed_line_content)

                    # Final whitespace normalization
                    processed_line_content = re.sub(r'\s+', ' ', processed_line_content).strip()

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

class NewsCleaner(FileFunction):
    # news version of the text cleaner, consistent with what was done to the podcasts
    # assumes both .txt input and output

    def __init__(self,
                 contraction_level: int = 1
                 ) -> None:
        
        self.input_extension = ".txt"
        self.output_extension = ".txt"

        super().__init__(self.input_extension, self.output_extension)
        
        if contraction_level >= 0 and contraction_level <= 2:
            self.contraction_level = contraction_level
        else:
            logger.error(f"Contraction level must be 0 (no contraction expansion), 1 (using contractions) or 2 (using pycontractions), got {contraction_level}")
            raise ValueError(f"Contraction level must be between 0 and 2")
        
        self.seen_line_hashes = set()
        self.FROM_RULE_CHAR_LIMIT = 200
        
        # Comprehensive emoji pattern (combining relevant ranges)
        self.EMOJI_PATTERN = re.compile(
            "["

            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F700-\U0001F77F"  # alchemical symbols
            "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA00-\U0001FA6F"  # Chess Symbols etc.
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "\U00002702-\U000027B0"  # Dingbats
            "\U000024C2-\U0001F251"
            "\U0001F1E6-\U0001F1FF"  # flags (iOS)
            "\u2600-\u26FF"          # Misc symbols
            "\u2700-\u27BF"          # Dingbats

            "]+",
            flags=re.UNICODE,
        )

        # ---- NEW: WHOLE LINE REMOVAL PATTERNS ----
        self.WHOLE_LINE_PATTERNS = [
            re.compile(r'^Last modified on *?2020$'),
            re.compile(r'^Log in to update your newsletter preferences.*?Email already exists\.$'),
            re.compile(r'^Sorry we cannot find that page.*?page could have gone missing\. Please click here for the homepage.*?top right hand corner of the page\.$'),
            re.compile(r'^You already have an account\. Please log in.*?by email$'),
            re.compile(r'^You will be connected to www\..*? in just a moment\.\.\.$'),
            re.compile(r'^I would like to receive updates on cool openings and celebrity lifestyle news every week, by email$'),
            re.compile(r'^Get YouTube without the ads$'),
            re.compile(r'^We have detected that JavaScript is disabled in your browser\. Would you like to proceed to legacy Twitter$'),
            re.compile(r'^Bloomberg Follow Bloomberg on LINE messenger.*?you need\.$'),
            re.compile(r'^Your notification has been saved\. There was a problem saving your notification\.$'),
            re.compile(r'^Email notifications are only sent once a day.*?new matching items\.$'),
            re.compile(r'^Those born on this date are under the sign of \[\w+]\.$'),
            re.compile(r'^We rely on advertising to help fund our award winning journalism\. We urge you to turn off your ad blocker.*?access our quality content in the future\.$'),
            re.compile(r'^Thank you for your support\.$'),
            re.compile(r'^The original source of this article is \[source, e\.g\. Global Research\] Comment on \[source\] Articles on our Facebook page$'),
            re.compile(r'^The original source of this article is \[source]$'),
            re.compile(r'^If you manage this site and have a question.*?contact NetFirms directly\.$'),
            re.compile(r'^Thank you for visiting TruNews! The web browser you are using does not support modern websites\.$'),
            re.compile(r'^G O Media may get a commission$'),
            re.compile(r'^Live updates, tweets, photos, analysis and more from UFC.*?tap here\.$'),
            re.compile(r'^Some clues have been edited for clarity\.$'),
            re.compile(r'^The contents of this site are \d{4} Capitol Hill Publishing Corp\., a subsidiary of News Communications, Inc\.$'),
            re.compile(r'^If you are not redirected in a few seconds, click here$'),
            re.compile(r'^You can buy your own print of this cartoon$'),
            re.compile(r'^Get a complete information picture of the day by subscribing to UNIAN news feeds\.$'),
            re.compile(r'^For more information, please call:$'),
            re.compile(r'^Join the conversation\. It gets feisty! Already have an account\?$'),
            re.compile(r'^Log In$'),
            re.compile(r'^Get involved with the news in your community$'),
            re.compile(r'^Out of the ashes of censorship.*?VDARE\.com rises with the acquision.*?West Virginia\.$'),
            re.compile(r'^Having a space where we can meet.*?it is hard to overstate\.$'),
            re.compile(r'^And now, 100 mo donors will receive.*?Thank You Event at the Berkeley Castle\.$'),
            re.compile(r'^VDARE\.com is celebrating it is 20th year in fighting to keep America American\. We are readying the castle\. And we want to see you there!$'),
            re.compile(r'^Please enable JavaScript\. Without JavaScript some features of the site will not be accessible\.$'),
            re.compile(r'^is legally registered in the UK as a company incorporated for charitable purposes\. Head Office: .*? International dialling: .*?$'),
            re.compile(r'^This content is password protected\. To view it please enter your password below:$'),
            re.compile(r'^Please show your Support\. Help keep us Online$'),
            re.compile(r'^You can unlock this content, and much more, by becoming a subscriber\. Please follow the link below to get started\.$'),
            re.compile(r'^SUBSCRIBE to ABC NEWS:.*?(?:http|https):\S+'),
            re.compile(r'^Click to share on Facebook Opens in new window$'),
            re.compile(r'^Success! Now check your email to confirm your subscription\.$'),
            re.compile(r'^Share this on Facebook Opens in a new window$'),
            re.compile(r'^OH YEAH, since we are not corporate or government owned help us out here\.$'),
            re.compile(r'^YOU CAN ALSO SUPPORT US ON$'),
            re.compile(r'^Click to email this to a friend Opens in new window$'),
            re.compile(r'^Click to share on Twitter Opens in new window$'),
            re.compile(r'^Here is a list of organizations where you can donate\.$'),
            re.compile(r'^Welcome to WordPress\. This is your first post\. Edit or delete it, then start writing!$'),
            re.compile(r'^By Dick Morris on May 11, 2020 Click Here to give me your thoughts and continue the discussion. Please forward this email to any friends or family who may be interested in viewing my video commentary!*? Dick Morris TV: Lunch Alert!$'),
            re.compile(r'^!!!! View on YouTube$'),
            re.compile(r'^That address is already in use Thanks for signing up$'),
            re.compile(r'^\s*$'), # Blank lines
            re.compile(r'^Sorry, your browser does not support iframes\.$'),
        ]

        self.PREFIX_REMOVAL_PATTERNS = [
            # ---- NEW: High-priority, specific prefixes ----
            # Match the highly specific Politico 'Editor Note' patterns
            re.compile(r'^Editor Note: Morning (Defense|Education|Money) is a free version of POLITICO Pro (Defense|Education|Financial Services) morning newsletter.*?Act on the news with POLITICO Pro\.'),
            re.compile(r'^Editor Note: As the world commemorates.*?The below piece is an answer to that question\. Please click here to see even more perspectives on this important topic\.'),
            re.compile(r'^This post was originally published on this site'),
            re.compile(r'^Programming announcement:*? Already a Pro subscriber\? Learn more here.'),
            # Match the new President Trump patterns
            re.compile(r'^President Trump Donald John TrumpREAD: The Hill interview.*?MORE'),
            re.compile(r'^President Trump Donald John TrumpTrump administration calls.*?MORE'),
            # Match new "Home [word]" and other specific prefixes
            re.compile(r'^Home\s+(?:Breaking News|Criticism|Activism|Corruption|Culture)\s*'),
            re.compile(r'^This is a video post\. See the videos below\.'),
            re.compile(r'^A brief overview of the recent developments in \[.*?\]:'),
            re.compile(r'^This is a rush transcript and may contain errors\. It will be updated\.'),
            re.compile(r'^(?:UPDATE|BACKGROUND|PURPOSE|OBJECTIVE|INTRODUCTION|SCOPE|ANALYSIS|OPEN THREAD|BREAKING|OPINION|Reuters|Fox News|FILE PHOTO|By Dick Morris).*?(?: on .*?)?[:.]\s*'),
            re.compile(r'^(?:The Virus|The Little Known Beginning|The Deadly Crisis|The Real Stakes).*?Dick Morris TV:(?: Lunch Alert| History Video)!'),
            re.compile(r'^(?:WASHINGTON|NEW YORK|HONG KONG|VIENNA|STAUNTON|[A-Z\s,]+)\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?\s*(?:\(or other locations, days and maybe June instead of may; but the location’s always all caps\))?'),
            re.compile(r'^(?:ETH|NYP|CNBC|TheBlaze|CP|CBN|BIN|TMU|YOU\.S|U\.S)\s*'),
            re.compile(r'^(?:By .*? on .*? Click Here to give me your thoughts and continue the discussion\.|Get all the latest news on coronavirus.*?Sign up here\.|Get breaking news alerts and special reports\..*?weekday mornings\.|The following is a transcript of an interview.*?Face the Nation.”|Slate is making its coronavirus coverage free for all readers\..*?Start your free trial\.|This article was contributed by.*?|This article was originally published by.*?|Stay tuned to Breitbart News for live updates\..*?All times eastern\.|My new book LOSERTHINK.*?|This is the Babylon Bee Interview Show\.|\[an interview title.*?\] appeared first on The Babylon Bee\.|: Urge your governor to reject.*?|: No to mandatory vaccination.*?|: Demand Planned Parenthood.*?|: Yes to reform\. No to riots revolution!|Tell Trump Christians cannot accept.*?|Click here if you are having trouble viewing the slideshow.*?|AMY GOODMAN: This is Democracy Now!,.*?|HOW TO MAKE MONEY ON AMAZON FREE eCOURSE:.*?|Preserve Your Investments W A Gold IRA.*?|Shield Yourself From Identity Theft! Click Here!.*?|Buy, Sell Exchange GOLD w a Mobile App! Click Here!.*?|Do not Get Caught In The Chaos Without Food! Click Here!.*?|Cristina Laila from The Gateway Pundit reports,|You can see Q posts aggregated live.*?http: www\.qanon\.pub|Click here to sign up|Borowitz Report|Here are the best shots from this week Advertiser and Shuttle Camera Club)'),

            # ---- Group 1: Datelines and Timestamps (These are already specific) ----
            re.compile(r'^[A-Z\s,]+,\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}\s*(?:\.\s*[A-Z]+\s*\.)?'),
            re.compile(r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:,)?\s*(?:\d{4})?\s*[A-Z][a-zA-Z]+'),
            re.compile(r'^Last\s+Updated\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}[a-z]{2},\s+\d{4}\s+at\s+[\d\s:]+[ap]m'),
            re.compile(r'^[A-Z\s]{5,}\s+[A-Z][a-zA-Z]+'),

            # ---- Group 2 & 3: Refined Boilerplate, Calls to Action & Disclaimers ----
            re.compile(r'^We will use your email address.*?Privacy Notice.*?data protection rights\.'),
            re.compile(r'^BuzzFeed News has reporters.*?become a member.*?newsletter,\s+Outbreak Today\s*\.'),
            re.compile(r'^Dear Reader:.*?Pandemic Pantry series.*?Enjoy!\s*'),
            re.compile(r'^Here are some news stories.*?click the title above.*?products of the Fake News.*?(?:qanon\.pub|No Q\.)'),
            re.compile(r'^This is a rush transcript\..*?(?:in its final form|It will be updated)\.'),
            re.compile(r'^Sign up for our special edition newsletter to get a daily update on the coronavirus pandemic\.'),
            re.compile(r'^Share on Facebook Share on Twitter Share on Pinterest'),
            re.compile(r'^Please make sure these dispatches.*?Share with kin.*?do likewise\.'),
            re.compile(r'^Click for more article by .*?\.\.'),
            re.compile(r'^Editor note: This story originally was published by .*?\.$'),
            re.compile(r'^The following video is brought to you courtesy of the .*?YouTube Channel\..*?watch it now\.'),
            re.compile(r'^Written by .*? on \. Posted in Latest news'),

            # ---- Group 4: Dynamic & Miscellaneous Boilerplate (Already specific) ----
            re.compile(r'^The global death toll from the coronavirus is more than [\d,.]+,?\s*with more than [\d,.]+\s*million infections confirmed.*?respiratory illness\.'),
            re.compile(r'^This Daily FRN News Brief is a summary of\s*'),
            re.compile(r'^Here is the latest on coronavirus social distancing rules'),
            re.compile(r'^Click to see the full size image'),
            re.compile(r'^This is a video post\. See the video below\.'),
            re.compile(r'^90 OF POLLS PROJECTED TO FALL IN THIS RANGE'),
            re.compile(r'^(?:VT|CNN|COMMENT|PETITION|WATCH|NaturalHealth365)\s*'),
        ]

        # ---- NEW: SUFFIX REMOVAL PATTERNS ----
        self.SUFFIX_REMOVAL_PATTERNS = [
            re.compile(r'Picture$'),
            re.compile(r'The original source of this article is \[.*?\]$'),
            re.compile(r'You can subscribe to \[name\] YouTube channel here\.$'),
            re.compile(r'\[name\] for The New York Times$'),
            re.compile(r'pic\.twitter\.com\s+\S+$'),
            re.compile(r'To see more and join the club visit facebook\.com groups decameraclub$'),
            re.compile(r'Get the latest updates here\.$'),
            re.compile(r'Get the latest updates on COVID 19 here\.$'),
            re.compile(r'Watch live below\.$'),
            re.compile(r'Read more$'),
            re.compile(r'See the report here: SUPPORT THE NETWORK WITH THE LINKS BELOW!.*?$'),
            re.compile(r'Get exclusive content and watch full episodes now by downloading the Portable.TV app:.*?$'),
            re.compile(r'SUBSCRIBE to our YouTube channel for more videos:.*?$'),
            re.compile(r'ALSO AVAILABLE ON HULU: https: hulu\.tv \S+$'),
            re.compile(r'To see more or to join click here$'),
            re.compile(r'To find out more click here$'),
            re.compile(r'and you can watch it below:$'),
            re.compile(r'Please follow and like us:$'),
            re.compile(r'Come and join us now at parler\.com davidicke$'),
            re.compile(r'The video is on the link below:$'),
            re.compile(r'The video can be seen below:$'),
            re.compile(r'Thanks to \[name\] for this (link|video|image|photo)$'),
        ]


    def remove_emojis(self, text: str) -> str:
        text_no_emoji = self.EMOJI_PATTERN.sub(' ', text)
        # Normalize whitespace to single spaces and strip leading/trailing spaces
        return re.sub(r'\s+', ' ', text_no_emoji).strip()

    def map(self) -> None:

        try:
            logger.info(f"Processing text file: {self.input_file_path}")

            self.seen_line_hashes.clear() # to avoid deduplicating across files
            with open(self.input_file_path, 'r', encoding='utf-8') as input_file, \
                 open(self.output_file_path, 'w', encoding='utf-8') as output_file:

                for line_num, line in enumerate(input_file, 1): # Process line by line
                    
                    line_hash = xxhash.xxh64(line.strip().encode('utf-8')).hexdigest()

                    if line_hash in self.seen_line_hashes:
                        logger.info(f"Duplicate line {line_num} found, skipping.")
                        continue # skip to the next line
                    
                    self.seen_line_hashes.add(line_hash)
                    
                    # 1. Remove the trailing newline character for processing
                    processed_line_content = line.rstrip('\n')

                    # ---- NEW: Whole Line Removal Logic ----
                    is_whole_line_match = False
                    for pattern in self.WHOLE_LINE_PATTERNS:
                        if pattern.fullmatch(processed_line_content):
                            logger.info(f"Removed whole line {line_num} due to boilerplate match.")
                            is_whole_line_match = True
                            break
                    if is_whole_line_match:
                        continue # Discard the line and move to the next

                    # ---- Prefix Removal Logic ----
                    prefix_removed = False
                    for pattern in self.PREFIX_REMOVAL_PATTERNS:
                        if pattern.match(processed_line_content):
                            processed_line_content = pattern.sub('', processed_line_content, count=1).lstrip()
                            logger.info(f"Removed prefix from line {line_num}.")
                            prefix_removed = True
                            break # Stop checking other patterns for this line
                    
                    # Handle the special 'from' rule only if no other prefix was removed
                    if not prefix_removed and processed_line_content.startswith('from ') and len(processed_line_content) < self.FROM_RULE_CHAR_LIMIT:
                        logger.info(f"Line {line_num} removed by 'from' rule.")
                        processed_line_content = '' # Discard the line completely

                    # If the line is now empty after all prefix removals, skip it
                    if not processed_line_content:
                        continue
                    
                    # ---- NEW: Suffix Removal Logic ----
                    for pattern in self.SUFFIX_REMOVAL_PATTERNS:
                        if pattern.search(processed_line_content):
                            processed_line_content = pattern.sub('', processed_line_content, count=1).rstrip()
                            logger.info(f"Removed suffix from line {line_num}.")
                            break # Stop checking other patterns for this line

                    processed_line_content = re.sub('•', '. ', processed_line_content)
                    processed_line_content = self.remove_emojis(processed_line_content)

                    if self.contraction_level == 1:
                        processed_line_content = contractions.fix(processed_line_content)
                    elif self.contraction_level == 2:
                        logger.error("'contraction_level == 2' hasn't been implemented yet.")
                        raise NotImplementedError("'contraction_level == 2' hasn't been implemented yet.")
                    
                    # Remove greater and less than symbols
                    processed_line_content = re.sub(r'>+', ' ', processed_line_content) # remove sequences of >
                    processed_line_content = re.sub(r'<+', ' ', processed_line_content) # remove sequences of <
                    
                    # Remove all underscores (swap for whitespace)
                    processed_line_content = processed_line_content.replace('_', ' ')
                    
                    # Remove all hyphen types (swap for spaces)
                    processed_line_content = processed_line_content.replace('-', ' ')
                    processed_line_content = re.sub(r'[–—]', ' ', processed_line_content)  # en-dash (–), em-dash (—)

                    # Remove possessive suffixes
                    # Remove singular possessive
                    processed_line_content = re.sub(r"\b(\w+)[’']s\b", r"\1", processed_line_content)
                    # Remove plural possessive, apostrophe followed by space, punctuation, or end of string
                    processed_line_content = re.sub(r"\b(\w+s)[’'](?=\s|$|[.,;:!?—…‘’“”\"\'\-\)])", r"\1", processed_line_content)
                    # Remove random apostrophes surrounded by space (might help mitigate NER problems)
                    processed_line_content = re.sub(r'(?<=\s)[\'’](?=\s)', ' ', processed_line_content)

                    # Final apostrophe rules
                    # Remove apostrophes prefixing or suffixing a word
                    processed_line_content = re.sub(r"\b[’']\s+", '', processed_line_content)  # Remove apostrophe at start of a word boundary
                    processed_line_content = re.sub(r"\s+[’']\b", '', processed_line_content)  # Remove apostrophe at end of a word boundary
                    # Replace apostrophes inside words with underscore (consistent with NER strategy)
                    processed_line_content = re.sub(r"(?<=\w)[’'](?=\w)", '_', processed_line_content)

                    # Double check underscores
                    processed_line_content = re.sub(r'_+', '_', processed_line_content)
                    # Remove stray underscores surrounded by spaces (but not inside words)
                    processed_line_content = re.sub(r'(?<=\s)_+(?=\s)', ' ', processed_line_content)

                    # Double check apostrophes (sanity)
                    processed_line_content = re.sub(r"[’']", " ", processed_line_content)

                    # Remove everything but normal punctualization (added: quotation marks)
                    processed_line_content = re.sub(r'[^\w\s\.\,\?\!\:\;\"\'_]', ' ', processed_line_content)

                    # Repeated punctuation handling
                    # Replace any sequence of colons and semicolons surrounded by non-whitespace with a single space
                    processed_line_content = re.sub(r'(?<=\S)[;:]+(?=\S)', ' ', processed_line_content)

                    # Final whitespace normalization
                    processed_line_content = re.sub(r'\s+', ' ', processed_line_content).strip()

                    if processed_line_content: # Only write if there's actual content
                        output_file.write(processed_line_content + '\n')
                    else:
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
                 stop_token: List[str] = ["i", "love", "blueberry", "waffles"]
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
                    if sentence == self.stop_token:  # skip sentinels
                        output_file.write("\n")  # add newline
                    elif sentence: # safety, not really needed
                        output_file.write(" ".join(sentence) + " ")  # write words as a space-separated line
        
                logger.info(f"Finished processing. Output saved to: {self.output_file_path}")
        
        except Exception as e:
            logger.error(f"Error processing json file: {e}")
            raise
