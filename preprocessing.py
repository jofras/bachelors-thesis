import json
import re
import string
import random
import os
import glob

"""
takes a json file and returns a list of sentences (lists of words)
- naive implementation, doesn't handle sentences spanning multiple jsons
@params:
- file_path: path to the json file
@return:
- list of sentences (lists of words)
limitations:
- doesn't handle sentences spanning multiple jsons
- lowercases all letters -> potential name-word ambiguity (Hunter vs. hunter)
- doesn't deal with apostrophes, i.e. i've -> ive, that's -> thats
"""
def create_sentence_list(file_path):
    
    sentence_list = []
    punctuation_to_remove = ''.join(char for char in string.punctuation if char not in '.!?:;')
    translator = str.maketrans('', '', punctuation_to_remove)
    
    with open(file_path, 'r') as file:
        for line in file:
            try:
                data = json.loads(line)
                turn_text = data.get('turnText', '')
                
                # skip empty entries
                if not turn_text.strip():
                    continue
                
                # remove all contents within brackets including whitespace
                clean_text = re.sub(r'\[\s*[^\]]*\s*\]', ' ', turn_text)  # [text]
                clean_text = re.sub(r'\(\s*[^)]*\s*\)', ' ', clean_text)  # (text)
                clean_text = re.sub(r'\{\s*[^}]*\s*\}', ' ', clean_text)  # {text}
                
                # handle partial left brackets
                clean_text = re.sub(r'\[\s*[^\]]*$', ' ', clean_text)
                clean_text = re.sub(r'\(\s*[^)]*$', ' ', clean_text)
                clean_text = re.sub(r'\{\s*[^}]*$', ' ', clean_text)
                
                # handle partial right brackets
                clean_text = re.sub(r'^[^\[]*\]', ' ', clean_text)
                clean_text = re.sub(r'^[^(]*\)', ' ', clean_text)
                clean_text = re.sub(r'^[^{]*\}', ' ', clean_text)
                
                clean_text = re.sub(r'\d+', '', clean_text)     # remove numbers
                clean_text = clean_text.lower()                 # lowercase
                clean_text = clean_text.translate(translator)   # remove punctuation
                
                # clean up extra spaces from bracket removals
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                
                # split according to sentence-ending punctuation
                sentences = re.split(r'(?<=[.!?:;])\s*', clean_text)

                for sentence in sentences:
                    sentence = re.sub(r'[.!?:;]', ' ', sentence)
                    sentence = re.sub(r'\s+', ' ', sentence)
                    sentence = sentence.strip()
                    
                    if sentence:
                        words = sentence.split()
                        if words:
                            sentence_list.append(words)

            except json.JSONDecodeError:
                continue
    
    return sentence_list

"""
same as above, but subsamples the sentences
@params:
- file_path: path to the json file
- subsample_rate: rate of subsampling
@return:
- list of sentences (lists of words)
"""
def create_sentence_list_from_subsample(file_path, subsample_rate=0.1):
    
    sentence_list = []
    punctuation_to_remove = ''.join(char for char in string.punctuation if char not in '.!?:;')
    translator = str.maketrans('', '', punctuation_to_remove)

    random.seed(42)
    
    with open(file_path, 'r') as file:
        for line in file:
            if (random.random() < subsample_rate):
                try:
                    data = json.loads(line)
                    turn_text = data.get('turnText', '')
                    
                    # skip empty entries
                    if not turn_text.strip():
                        continue
                    
                    # remove all contents within brackets including whitespace
                    clean_text = re.sub(r'\[\s*[^\]]*\s*\]', ' ', turn_text)  # [text]
                    clean_text = re.sub(r'\(\s*[^)]*\s*\)', ' ', clean_text)  # (text)
                    clean_text = re.sub(r'\{\s*[^}]*\s*\}', ' ', clean_text)  # {text}
                    
                    # handle partial left brackets
                    clean_text = re.sub(r'\[\s*[^\]]*$', ' ', clean_text)
                    clean_text = re.sub(r'\(\s*[^)]*$', ' ', clean_text)
                    clean_text = re.sub(r'\{\s*[^}]*$', ' ', clean_text)
                    
                    # handle partial right brackets
                    clean_text = re.sub(r'^[^\[]*\]', ' ', clean_text)
                    clean_text = re.sub(r'^[^(]*\)', ' ', clean_text)
                    clean_text = re.sub(r'^[^{]*\}', ' ', clean_text)
                    
                    clean_text = re.sub(r'\d+', '', clean_text)     # remove numbers
                    clean_text = clean_text.lower()                 # lowercase
                    clean_text = clean_text.translate(translator)   # remove punctuation
                    
                    # clean up extra spaces from bracket removals
                    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                    
                    # split according to sentence-ending punctuation
                    sentences = re.split(r'(?<=[.!?:;])\s*', clean_text)

                    for sentence in sentences:
                        sentence = re.sub(r'[.!?:;]', ' ', sentence)
                        sentence = re.sub(r'\s+', ' ', sentence)
                        sentence = sentence.strip()
                        
                        if sentence:
                            words = sentence.split()
                            if words:
                                sentence_list.append(words)

                except json.JSONDecodeError:
                    continue
    
    return sentence_list

"""
helper method to merge turnTexts with the same url
"""
def merge_on_url(input_file, output_file, cached_url, cached_turn_text):
    # initialize temp variables with cached values
    curr_url = cached_url
    curr_turn_text = cached_turn_text
    file_has_data = False
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                file_has_data = True
                try:
                    data = json.loads(line.strip())
                    url = data.get('mp3url', '')
                    turn_text = data.get('turnText', '')
                    
                    if url == curr_url:
                        # same URL, merge the turn text
                        curr_turn_text += turn_text
                    else:
                        # different URL, write the previous one (if exists)
                        if curr_url:
                            simplified = {
                                "mp3url": curr_url,
                                "turnText": curr_turn_text
                            }
                            outfile.write(json.dumps(simplified) + '\n')
                        # start tracking the new URL
                        curr_url = url
                        curr_turn_text = turn_text
                except json.JSONDecodeError:
                    print(f"warning: couldn't parse json at line {line_num}")
                    continue
            
            # if the file was empty but we had cached data, write it
            if not file_has_data and curr_url:
                simplified = {
                    "mp3url": curr_url,
                    "turnText": curr_turn_text
                }
                outfile.write(json.dumps(simplified) + '\n')
                
            # leave the last entry for the next file
            return curr_url, curr_turn_text
    except (IOError, OSError) as e:
        print(f"Error: {e}")
        return None, ''

"""
merge all jsons with the same url
"""
def batch_merge_on_url(input_folder="data/segmented_data/url_segments", output_folder="data/segmented_data/merged_url_segments"):
    # create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # get all url-turnText segments
    file_pattern = os.path.join(input_folder, "useg*.jsonl")
    input_files = glob.glob(file_pattern)
    
    if not input_files:
        print(f"no matching 'useg*.jsonl' files found in {input_folder}")
        return 0, 0
    
    input_files.sort()
    successful, failed = 0, 0
    cached_url, cached_turn_text = None, ''
    last_output_file = None
    
    for input_file in input_files:
        base_name = os.path.basename(input_file)
        seg_num = base_name.replace("useg", "").replace(".jsonl", "")
        output_file = os.path.join(output_folder, f"museg{seg_num}.jsonl")
        last_output_file = output_file
        print(f"processing {base_name} -> {os.path.basename(output_file)}")
        
        result = merge_on_url(input_file, output_file, cached_url, cached_turn_text)
        if result[0] is not None:
            cached_url, cached_turn_text = result
            successful += 1
        else:
            failed += 1
            print(f"failed")
            
    # write last entry from final file
    if cached_url and last_output_file:
        # check if the file exists before appending
        if os.path.exists(last_output_file):
            file_mode = 'a'  # append mode
        else:
            file_mode = 'w'  # write mode for new file
            
        with open(last_output_file, file_mode) as outfile:  # append to the last file
            simplified = {
                "mp3url": cached_url,
                "turnText": cached_turn_text
            }
            outfile.write(json.dumps(simplified) + '\n')
        
    print(f"\nprocessing complete. successfully processed: {successful} files, failed to process: {failed} files")
    return successful, failed

if __name__ == "__main__":
    # merge all files in the url_segments folder
    batch_merge_on_url()