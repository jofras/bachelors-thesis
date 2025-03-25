from pathlib import Path
import shutil
import logging
from typing import List, Optional, Union, Tuple, Type, TypeVar

from filefunc import FileFunction

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FileProcessor")

T = TypeVar('T', bound=FileFunction)

class FileProcessor:

    """
    This class, given a list of files, an operation, and a destination, transforms all files and places the results 
    in the destination directory. Assumes that the operation returns a file path (to then be altered).
    """

    def __init__(self, 
                 input_file_path_list: Optional[List[Union[str, Path]]] = None, 
                 output_file_path_list: Optional[List[Union[str, Path]]] = None,
                 operation: Optional[FileFunction] = None, 
                 destination: Union[str, Path] = Path(__file__).resolve().parent
                 # maybe add some renaming options
                 ) -> None:
        
        """
        Initialize the FileProcessor.

        Args: 
            input_file_path_list: List of file paths to process.
            output_file_path_list: Destination file paths.
            operation: FileFunction object to apply to each file.
            destination: Directory where processed files will be saved. 
        """
        
        self.input_file_path_list = input_file_path_list
        self.output_file_path_list = output_file_path_list
        self.operation = operation
        self.destination = Path(destination) if isinstance(destination, str) else destination

    def generate_output_file_paths(self) -> None:
        
        """
        Generates output file paths based on input file and operation.

        Ensures output_file_path_list is initialized and populated.

        Assumes destination is initialized.

        Raises: 
            AttributeError: If input_file_path_list or operation is uninitialized.
            TypeError: If any input path is not a string or Path object.
        """

        # check if input file list exists
        if self.input_file_path_list is None:
            logger.error("Cannot generate output paths: input_file_path_list is None")
            raise AttributeError("Cannot generate output paths: input_file_path_list is None")

        # check if operation exists and has output extension
        if self.operation is None:
            logger.error("Cannot generate output paths: operation is None")
            raise AttributeError("Cannot generate output paths: operation is None")
        
        # initialize output path list if it doesn't exist
        if self.output_file_path_list is None:
            self.output_file_path_list = []
        else:
            self.output_file_path_list.clear()

        logger.info(f"Generating output paths for {len(self.input_file_path_list)} files to {self.destination}")

        for input_file_path in self.input_file_path_list:
            
            # convert string paths into Path objects
            path_obj = Path(input_file_path) if isinstance(input_file_path, str) else input_file_path

            if not isinstance(path_obj, Path):
                logger.error(f"Input file must be a string or Path object: {input_file_path}")
                raise TypeError(f"Input file must be a string or Path object: {input_file_path}")
            
            # generate new filename
            new_filename = f"{path_obj.stem}{self.operation.output_extension}"
            output_path = self.destination / new_filename

            logger.debug(f"Generated output path: {output_path} for input: {path_obj}")
            self.output_file_path_list.append(output_path)

        logger.info(f"Successfully generated {len(self.output_file_path_list)} output file paths")


    def process_files(self) -> Tuple[int, int]:
        """
        Process all files in the input_file_path_list using the specified FileFunction.

        Returns:
            Tuple[int, int]: A tuple containing (number of successfully processed files, number of errors). 

        Raises:
            AttributeError: If input_file_path_list or operation is uninitialized.
            TypeError: If operation is not a FileFunction or if it doesn't return a Path object.
            FileNotFoundError: If input files don't exist.
            PermissionError: If there are permission issues.
            Exception: For other, unexpected errors.
        """

        # validate inputs
        if self.input_file_path_list is None:
            logger.error("The file path list is uninitialized")
            raise AttributeError("The file path list is uninitialized")
        
        if self.operation is None:
            logger.error("No operation specified")
            raise AttributeError("No operation specified")
        
        # verify that operation is a FileFunction
        if not isinstance(self.operation, FileFunction):
            logger.error(f"Operation must be a FileFunction object, got {type(self.operation)}")
            raise TypeError(f"Operation must be a FileFunction object, got {type(self.operation)}")
        
        # ensure destination directory exists
        try:
            self.destination.mkdir(parents=True, exist_ok=True)
            logger.info(f"Destination directory ensured: {self.destination}")
        except PermissionError:
            logger.error(f"Permission error when creating destination directory: {self.destination}")
            raise
        except Exception as e:
            logger.error(f"Error creating destination directory: {e}")
            raise

        # generate output file paths if they don't exist or count doesn't match inputs
        if (self.output_file_path_list is None or 
            len(self.output_file_path_list) != len(self.input_file_path_list)):
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

        for i, file_path in enumerate(self.input_file_path_list):
            try: 
                # convert string paths to Path objects
                file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

                if not isinstance(file_path_obj, Path):
                    logger.error(f"File path must be a string or Path object: {file_path}")
                    raise TypeError(f"File path must be a string or Path object: {file_path}")
                
                if not file_path_obj.exists():
                    logger.error(f"File does not exist: {file_path_obj}")
                    raise FileNotFoundError(f"File not found: {file_path_obj}")
                
                # set input and output paths for operation
                self.operation.input_file_path = file_path_obj
                output_file_path = self.output_file_path_list[i]
                self.operation.output_file_path = Path(output_file_path) if isinstance(output_file_path, str) else output_file_path
                
                # apply operation
                logger.info(f"Processing file {file_path_obj} with operation: {self.operation.__class__.__name__}")
                new_file_path = self.operation.apply()

                # validate operation result
                if not isinstance(new_file_path, Path):
                    logger.error(f"Operation must return a pathlib.Path object, got {type(new_file_path)}")
                    raise TypeError("Operation must return a pathlib.Path object")
                
                if not new_file_path.exists():
                    logger.error(f"Operation did not produce a file at: {new_file_path}")
                    raise FileNotFoundError(f"Operation did not produce a file at: {new_file_path}")
                
                # move file to destination if needed
                if new_file_path != self.output_file_path_list[i]:
                    destination_path = self.output_file_path_list[i]
                    
                    if destination_path.exists():
                        logger.warning(f"Destination file already exists and will be overwritten: {destination_path}")
                    
                    # ensure parent directory exists
                    destination_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    shutil.move(new_file_path, destination_path)
                    logger.info(f"Successfully moved file to: {destination_path}")
                else:
                    logger.info(f"File already at destination: {new_file_path}")
                
                processed_count += 1

            except FileNotFoundError as e:
                logger.error(f"File not found error: {e}")
                error_count += 1
            except PermissionError as e:
                logger.error(f"Permission error: {e}")
                error_count += 1
            except TypeError as e:
                logger.error(f"Type error: {e}")
                error_count += 1
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                error_count += 1

        logger.info(f"Processing complete. Successfully processed {processed_count} files. Encountered {error_count} errors.")
        return processed_count, error_count


    def set_file_paths(self, input_file_path_list: List[Union[str, Path]]) -> None:
        
        """
        Set or update the list of files to be processed.
        
        Args:
            file_path_list: List of file paths to process.
        """

        self.input_file_path_list = input_file_path_list
        logger.info(f"Updated file list with {len(input_file_path_list)} files")
    

    def set_operation(self, operation: FileFunction) -> None:
        
        """
        Set or update the operation to be applied to files.
        
        Args:
            operation: FileFunction object to apply to each file.
        """

        if not isinstance(operation, FileFunction):
            logger.error(f"Operation must be a FileFunction object, got {type(operation)}")
            raise TypeError(f"Operation must be a FileFunction object")
            
        self.operation = operation
        logger.info(f"Updated file processing operation to {operation.__class__.__name__}")
    

    def set_destination(self, destination: Union[str, Path]) -> None:
        
        """
        Set or update the destination directory.
        
        Args:
            destination: Directory where processed files will be saved.
        """
        
        self.destination = Path(destination) if isinstance(destination, str) else destination
        logger.info(f"Updated destination directory to {self.destination}")
        


    

            
            