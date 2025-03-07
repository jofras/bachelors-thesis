import json
import os
import glob

def simplify_jsonl_text_only(input_file, output_file):
    """
    Read each JSON line, extract only the turnText field, and save as new JSON.
    
    Args:
        input_file (str): Path to the input JSONL file
        output_file (str): Path to the output simplified JSONL file
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                try:
                    data = json.loads(line.strip())
                    if 'turnText' in data:
                        simplified = {"turnText": data["turnText"]}
                        outfile.write(json.dumps(simplified) + '\n')
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False
    
def simplify_jsonl_text_speaker(input_file, output_file):
    """
    Read each JSON line, extract only the turnText field, and save as new JSON.
    
    Args:
        input_file (str): Path to the input JSONL file
        output_file (str): Path to the output simplified JSONL file
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                try:
                    data = json.loads(line.strip())
                    if 'turnText' in data and 'inferredSpeakerName' in data:
                        simplified = {
                            "turnText": data["turnText"], 
                            "speaker": data["speaker"]
                        }
                        outfile.write(json.dumps(simplified) + '\n')
                    else:
                        print(f"Warning: Missing fields in line {line_num}, skipping.")
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False
    
def simplify_jsonl_text_url(input_file, output_file):
    """
    Read each JSON line, extract only the turnText field, and save as new JSON.
    
    Args:
        input_file (str): Path to the input JSONL file
        output_file (str): Path to the output simplified JSONL file
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
            for line_num, line in enumerate(infile, 1):
                try:
                    data = json.loads(line.strip())
                    if 'turnText' in data and 'mp3url' in data:
                        simplified = {
                            "turnText": data["turnText"], 
                            "mp3url": data["mp3url"]
                        }
                        outfile.write(json.dumps(simplified) + '\n')
                    else:
                        print(f"Warning: Missing fields in line {line_num}, skipping.")
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False

def batch_process_files(input_folder):
    """
    Process all seg*.jsonl files in the given folder, creating cseg*.jsonl files.
    
    Args:
        input_folder (str): Folder containing the segmented JSONL files
    """
    # Get all seg*.jsonl files
    file_pattern = os.path.join(input_folder, "seg*.jsonl")
    input_files = glob.glob(file_pattern)
    
    if not input_files:
        print(f"No files matching 'seg*.jsonl' found in {input_folder}")
        return
    
    # Sort files to process them in order
    input_files.sort()
    
    successful = 0
    failed = 0
    
    for input_file in input_files:
        # Extract the segment number and create the output filename
        base_name = os.path.basename(input_file)
        seg_num = base_name.replace("seg", "").replace(".jsonl", "")
        output_file = os.path.join(input_folder, f"useg{seg_num}.jsonl")
        
        print(f"Processing {base_name} -> {os.path.basename(output_file)}")
        
        if simplify_jsonl_text_url(input_file, output_file):
            successful += 1
        else:
            failed += 1
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {successful} files")
    print(f"Failed to process: {failed} files")

if __name__ == "__main__":
    # Process all files in the segmented_data folder
    batch_process_files("segmented_data/raw_segments")