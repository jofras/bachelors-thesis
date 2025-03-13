import json
import re
import string
import random
import os
import glob
import time

"""
contains methods to clean and turn a single turnText entry into a list of sentences
"""

def create_sentence_list(file_path):

    """
    takes a json file and returns a list of sentences (lists of words)
    - naive implementation, doesn't handle sentences spanning multiple jsons
    @params:
    - file_path: path to the json file
    @return:
    - list of sentences (lists of words)
    limitations:
    - lowercases all letters -> potential name-word ambiguity (Hunter vs. hunter)
    - doesn't deal with apostrophes, i.e. i've -> ive, that's -> thats
    """
    
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

            
                # handle partial left brackets (might be overkill for merged url segments)
                # clean_text = re.sub(r'\[\s*[^\]]*$', ' ', clean_text)
                # clean_text = re.sub(r'\(\s*[^)]*$', ' ', clean_text)
                # clean_text = re.sub(r'\{\s*[^}]*$', ' ', clean_text)

                # handle partial right brackets (same as above)
                # clean_text = re.sub(r'^[^\[]*\]', ' ', clean_text)
                # clean_text = re.sub(r'^[^(]*\)', ' ', clean_text)
                # clean_text = re.sub(r'^[^{]*\}', ' ', clean_text)
                
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

def create_sentence_list_from_subsample(file_path, subsample_rate=0.1):
    
    """
    same as above, but subsamples the sentences
    @params:
    - file_path: path to the json file
    - subsample_rate: rate of subsampling
    @return:
    - list of sentences (lists of words)
    """

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

def batch_create_sentence_list(input_folder="../data/segments/text_url/merged/batch1", seg_prefix="museg"):
    
    """
    generate sentences from all merged segments
    """

    files = sorted(glob.glob(os.path.join(input_folder, f"{seg_prefix}*.jsonl")))

    if not files:
        print(f"no files matching '{seg_prefix}*.jsonl' found in {input_folder}")
        return []
    
    sentence_list = []
    file_count = 0
    total_files = len(files)
    start_time = time.time()

    for file in files:
        print(f"processing {file}")
        sentences = create_sentence_list(file)
        sentence_list.extend(sentences)
        print(f"finished processing {file}")
        file_count += 1
        print(f"estimated time remaining: {((time.time() - start_time) / file_count) * (total_files - file_count)} seconds")

    return sentence_list

if __name__ == "__main__":
    # create sentences from merged url segments
    sentences = batch_create_sentence_list(input_folder="../data/segmented_data/merged_url_segments/batch10")
    print(f"\n{len(sentences)} sentences created")
    with open("../data/processed_sentences/sbatch10.json", 'w') as outfile:
        json.dump(sentences, outfile)