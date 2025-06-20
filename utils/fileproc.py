from pathlib import Path
import shutil
import logging
from typing import List, Optional, Union, Tuple, TypeVar
import re

from utils.filefunc import FileFunction

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FileProcessor")

# create new type for FileFunction
T = TypeVar('T', bound=FileFunction)

class FileProcessor:

    # wrapper class for FileFunctions
    # performs as many checks as possible before passing args to FileFunction

    def __init__(self, 
                 input_file_path_list: List[Union[str, Path]], 
                 function: FileFunction, 
                 destination: Union[str, Path] = Path(__file__).resolve().parent,
                 output_prefix: str = None
                 # maybe add some renaming options
                 ) -> None:
        
        self.input_file_path_list = []
        for input_file_path in input_file_path_list:
            path = Path(input_file_path) if isinstance(input_file_path, str) else input_file_path
            self.input_file_path_list.append(path)

        self.function = function
        self.destination = Path(destination) if isinstance(destination, str) else destination
        self.output_prefix = output_prefix
        self.output_file_path_list = []

    def generate_output_file_paths(self) -> None:

        # creates, populates, and returns file paths of processed output files
        # assumes destination is initialized
        
        # clear output file path list if it has already been in use
        if self.output_file_path_list != []:
            self.output_file_path_list.clear()

        logger.info(f"Generating output paths for {len(self.input_file_path_list)} files to {self.destination}")

        for input_path in self.input_file_path_list:
            
            # generate new filename

            match = re.search(r'(\d+)$', input_path.stem)
            suffix = match.group(1) if match else input_path.stem  # fallback in case no match

            new_filename = f"{self.output_prefix}{suffix}{self.function.output_extension}"
            output_path = self.destination / new_filename

            logger.debug(f"Generated output path: {output_path} for input: {input_path}")
            self.output_file_path_list.append(output_path)

        logger.info(f"Successfully generated {len(self.output_file_path_list)} output file paths")


    def process_files(self) -> List[Path]:

        # processes all input files using the specified FileFunction
        # returns a tuple of ints indicating successes and failures
        
        # ensure destination directory exists
        try:
            self.destination.mkdir(parents=True, exist_ok=True)
            logger.info(f"Destination directory ensured: {self.destination}")
        except Exception as e:
            logger.error(f"Error creating destination directory: {e}")
            raise

        # generate output file paths if they don't exist or count doesn't match inputs
        if (self.output_file_path_list == []):
            logger.info("Output file paths need to be generated")
            try:
                self.generate_output_file_paths()
            except Exception as e:
                logger.error(f"Error generating output file paths: {e}")
                raise

        # process each file
        processed_count: int = 0
        error_count: int = 0

        logger.info(f"Starting to process {len(self.input_file_path_list)} files")

        for i, input_path in enumerate(self.input_file_path_list):
            try: 
                if not input_path.exists(): # maybe move this to the constructor
                    logger.error(f"File does not exist: {input_path}")
                    raise FileNotFoundError(f"File not found: {input_path}")
                
                # set input and output paths for function
                self.function.input_file_path = input_path
                self.function.output_file_path = self.output_file_path_list[i]
                
                # apply function
                logger.info(f"Processing file {input_path} with function: {self.function.__class__.__name__}")
                log_path = self.function.apply() # returns self.function.output_file_path
                logger.info(f"File processed at destination: {log_path}")
                
                processed_count += 1

            except Exception as e:
                logger.error(f"Error processing {input_path}: {e}")
                error_count += 1

        logger.info(f"Processing complete. Successfully processed {processed_count} files. Encountered {error_count} errors.")
        return self.output_file_path_list
    
    def set_function(self, function: FileFunction) -> None:
        self.function = function
        self.output_file_path_list = [] # to reset the output extensions
