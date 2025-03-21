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

    """
    This class finds all files in a directory matching specified criteria.

    Attributes:
        directory (Path): Directory to search for files.
        file_extension (str, optional): File extension to filter by (e.g. '.txt').
        prefix (str, optional): File name prefix to filter by.
        suffix (str, optional): File name suffix to filter by (before extension). 
        file_list (List[Path], optional): List to store found file paths.
        recursive (bool): Whether to search recursively in subdirectories. 
    """

    def __init__(self, 
                 directory: Union[str, Path] = Path(__file__).resolve().parent, 
                 file_extension: Optional[str] = None,
                 prefix: Optional[str] = None,
                 suffix: Optional[str] = None,
                 file_list: Optional[List[Path]] = None,
                 recursive: bool = False
                 ) -> None:
        
        """
        Initialize the FileFinder with search criteria.

        Args: 
            directory: Directory to search in.
            file_extension: File extension to filter by (include the dot, e.g. '.txt'). 
            prefix: File name prefix to filter by.
            suffix: File name suffix to filter by (before extension). 
            file_list: Optional pre-existing list to store results.
            recursive: Whether to search in subdirectories. 
        """

        self.directory = Path(directory) if isinstance(directory, str) else directory
        self.file_extension = file_extension
        self.prefix = prefix if prefix else ""
        self.suffix = suffix if suffix else ""
        self.file_list = file_list if file_list is not None else []
        self.recursive = recursive

        # validate directory 
        if not self.directory.exists():
            logger.error(f"Directory does not exist: {self.directory}")
            raise FileNotFoundError(f"Directory does not exist: {self.directory}")
        if not self.directory.is_dir():
            logger.error(f"Path is not a directory: {self.directory}")
            raise NotADirectoryError(f"Path is not a directory: {self.directory}")

    def findFiles(self) -> List[Path]:

        """
        Find files that match the specified criteria. 

        Returns: 
            List[Path]: List of Path objects for files that match the criteria.
        """
        
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
        