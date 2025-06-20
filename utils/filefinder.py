from pathlib import Path
import logging
from typing import List, Optional, Union

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FileFinder")

class FileFinder:

    # finds all files in a directory matching given criteria
    # args are pretty self-explanatory

    def __init__(self, 
                 directory: Union[str, Path] = Path(__file__).resolve().parent, # directory to search in
                 # file_name = {prefix}{suffix}{file_extension}
                 file_extension: Optional[str] = None, # include the dot, e.g. '.txt'
                 prefix: Optional[str] = "", # prefix and suffix can't overlap
                 suffix: Optional[str] = "",
                 file_list: Optional[List[Path]] = [], # optional pre-existing list to store results
                 recursive: bool = False # whether to search in subdirectories
                 ) -> None:
        
        # initializes FileFinder with search criteria

        self.directory = Path(directory) if isinstance(directory, str) else directory
        self.file_extension = file_extension
        self.prefix = prefix
        self.suffix = suffix
        self.file_list = file_list 
        self.recursive = recursive

        # validate directory 
        if not self.directory.exists():
            logger.error(f"Directory does not exist: {self.directory}")
            raise FileNotFoundError(f"Directory does not exist: {self.directory}")
        if not self.directory.is_dir():
            logger.error(f"Path is not a directory: {self.directory}")
            raise NotADirectoryError(f"Path is not a directory: {self.directory}")

    def find_files(self) -> List[Path]:

        # finds files matching the specified criteria
        # returns a list of path objects for files that match the criteria
        
        root_path = self.directory
        matching_files = []

        pattern = f"{self.prefix}*{self.suffix}"
        if self.file_extension:
            pattern += f"{self.file_extension}"

        logger.info(f"Searching for files with pattern '{pattern}' in {root_path}")

        try: 
            if self.recursive:
                matching_files = [file_path for file_path in root_path.rglob(pattern) if file_path.is_file()]
            else:
                matching_files = [str(file_path) for file_path in root_path.glob(pattern) if file_path.is_file()]
        
            self.file_list = matching_files 
            logger.info(f"Found {len(matching_files)} files matching the criteria")

            return matching_files
        
        except Exception as e:
            logger.error(f"Error finding files: {e}")
            raise
    
    def set_file_extension(self, file_extension) -> None:
        # add some testing to ensure it's a valid file extension
        self.file_extension = file_extension

    def set_prefix(self, prefix) -> None:
        self.prefix = prefix
    
    def set_suffix(self, suffix) -> None:
        self.suffix = suffix

    def set_directory(self, directory: Union[str, Path]) -> None:
        self.directory = Path(directory) if isinstance(directory, str) else directory
        
        # validate directory 
        if not self.directory.exists():
            logger.error(f"Directory does not exist: {self.directory}")
            raise FileNotFoundError(f"Directory does not exist: {self.directory}")
        if not self.directory.is_dir():
            logger.error(f"Path is not a directory: {self.directory}")
            raise NotADirectoryError(f"Path is not a directory: {self.directory}")
