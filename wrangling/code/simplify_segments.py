import json
import os
import glob

def simplify_jsonl_text_only(input_file, output_file):
    """
    read each json line, extract turnText field, and save as new json.
    
    args:
        input_file (str): path to the jsonl input file
        output_file (str): path to the simplified jsonl output file

    notes:
        doesn't work well for multiple-line-spanning sentences
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
                    print(f"warning: couldn't parse json at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False
    
def simplify_jsonl_text_speaker(input_file, output_file):
    """
    read each json line, extract turnText and inferredSpeaker fields, and save as new json.
    
    args:
        input_file (str): path to the jsonl input file
        output_file (str): path to the simplified jsonl output file

    notes:
        first approach to merging lines per sentence, doesn't work well though
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
                        print(f"warning: missing fields in line {line_num}, skipping.")
                except json.JSONDecodeError:
                    print(f"warning: Couldn't parse json at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False
    
def simplify_jsonl_text_url(input_file, output_file):
    """
    read each json line, extract turnText and  fields, and save as new json.
    
    args:
        input_file (str): path to the jsonl input file
        output_file (str): path to the simplified jsonl output file

    notes: 
        second attempt to merge lines based on speakers, but with urls -> creates full document for each podcast (great!!)
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
                        print(f"warning: missing fields in line {line_num}, skipping.")
                except json.JSONDecodeError:
                    print(f"warning: couldn't parse json at line {line_num}")
                    continue
        return True
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return False

def batch_simplify(input_folder, seg_prefix="useg", method=simplify_jsonl_text_url):
    """
    process all seg*.jsonl files in the given folder with an above method, creating {seg_prefix}seg*.jsonl files.
    
    args:
        input_folder (str): folder containing the segmented jsonl files
    """
    # get all seg*.jsonl files
    file_pattern = os.path.join(input_folder, "seg*.jsonl")
    input_files = glob.glob(file_pattern)
    
    if not input_files:
        print(f"No files matching 'seg*.jsonl' found in {input_folder}")
        return
    
    # sort files to process them in order
    input_files.sort()
    
    successful = 0
    failed = 0
    
    for input_file in input_files:
        # extract the segment number and create the output filename
        base_name = os.path.basename(input_file)
        seg_num = base_name.replace("seg", "").replace(".jsonl", "")
        output_file = os.path.join(input_folder, f"{seg_prefix}{seg_num}.jsonl")
        
        print(f"processing {base_name} -> {os.path.basename(output_file)}")
        
        if method(input_file, output_file):
            successful += 1
        else:
            failed += 1
    
    print(f"\nprocessing complete!")
    print(f"successfully processed: {successful} files")
    print(f"failed to process: {failed} files")

if __name__ == "__main__":
    # process all files in the segments folder
    batch_simplify("../data/segments/raw")