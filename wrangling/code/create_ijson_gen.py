from pathlib import Path
import ijson

def create_ijson_gen(input_file_paths, stop_token):
    
    """
    Takes a list of input file paths (JSON) and creates an ijson generator.
    
    For each file:
    - Checks if it contains a list of lists.
    - Adds each sublist to the ijson generator.
    - If the sublist contains only the stop token, it is disregarded.
    """
    
    for file_path in input_file_paths:
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        
        try:
            with open(file_path, 'rb', encoding='utf-8') as f:
                parser = ijson.items(f, 'item')  # adjust key based on actual JSON structure
                
                for sentence in parser:
                    if isinstance(sentence, list) and sentence != [stop_token]:
                        yield sentence
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue