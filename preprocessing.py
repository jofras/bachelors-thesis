import json
import re
import string
import random

"""
this method has quite some assumptions. specifications (and potential improvements):
- splits according to sentences (in an entry), not just entries (which might carry more context across sentences too)
- removes all numbers and punctuation
- lowercases all letters -> potential name-word ambiguity (Hunter vs. hunter)
- doesn't adequately deal with apostrophes, i.e. i've -> ive, that's -> thats 
maybe write different versions and compare their performance on analogy and other tasks 
"""
def process_json_file_sentences(file_path):
    all_sentences = []
    
    punctuation_to_remove = ''.join(char for char in string.punctuation if char not in '.!?:;')
    translator = str.maketrans('', '', punctuation_to_remove)
    
    with open(file_path, 'r') as file:
        for line in file:
            try:
                data = json.loads(line)
                turn_text = data.get('turnText', '')
                
                # skip empty entries (might be problematic if you're concatenating)
                if not turn_text.strip():
                    continue
                
                # Remove all contents within brackets including whitespace
                # Using a more aggressive regex approach with DOTALL flag to match across lines
                cleaned_text = re.sub(r'\[\s*[^\]]*\s*\]', ' ', turn_text, flags=re.DOTALL)  # [text]
                cleaned_text = re.sub(r'\(\s*[^)]*\s*\)', ' ', cleaned_text, flags=re.DOTALL)  # (text)
                cleaned_text = re.sub(r'\{\s*[^}]*\s*\}', ' ', cleaned_text, flags=re.DOTALL)  # {text}
                
                # Handle partial brackets - open bracket with no close bracket
                cleaned_text = re.sub(r'\[\s*[^\]]*$', ' ', cleaned_text, flags=re.DOTALL)
                cleaned_text = re.sub(r'\(\s*[^)]*$', ' ', cleaned_text, flags=re.DOTALL)
                cleaned_text = re.sub(r'\{\s*[^}]*$', ' ', cleaned_text, flags=re.DOTALL)
                
                # Handle partial brackets - close bracket with no open bracket
                cleaned_text = re.sub(r'^[^\[]*\]', ' ', cleaned_text, flags=re.DOTALL)
                cleaned_text = re.sub(r'^[^(]*\)', ' ', cleaned_text, flags=re.DOTALL)
                cleaned_text = re.sub(r'^[^{]*\}', ' ', cleaned_text, flags=re.DOTALL)
                
                # Continue processing as before
                cleaned_text = re.sub(r'\d+', '', cleaned_text)
                cleaned_text = cleaned_text.lower()
                cleaned_text = cleaned_text.translate(translator)
                
                # Clean up extra spaces from bracket removals before splitting
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                sentences = re.split(r'(?<=[.!?:;])\s*', cleaned_text)
                for sentence in sentences:
                    sentence = re.sub(r'[.!?:;]', ' ', sentence)
                    sentence = re.sub(r'\s+', ' ', sentence)
                    sentence = sentence.strip()
                    
                    if sentence:
                        words = sentence.split()
                        if words:
                            all_sentences.append(words)
            except json.JSONDecodeError:
                continue
    
    return all_sentences

# random subsampling version

def process_json_file_sentences_subsample(file_path, subsample_rate=0.1):
    all_sentences = []
    random.seed(42)

    punctuation_to_remove = ''.join(char for char in string.punctuation if char not in '.!?:;')
    translator = str.maketrans('', '', punctuation_to_remove)
    
    with open(file_path, 'r') as file:
        for line in file:
            if (random.random() < subsample_rate):
                try:
                    data = json.loads(line)
                    turn_text = data.get('turnText', '')
                    
                    # Skip empty entries
                    if not turn_text.strip():
                        continue
                    
                    # Remove all contents within brackets including whitespace
                    # Using a more aggressive regex approach with DOTALL flag to match across lines
                    cleaned_text = re.sub(r'\[\s*[^\]]*\s*\]', ' ', turn_text, flags=re.DOTALL)  # [text]
                    cleaned_text = re.sub(r'\(\s*[^)]*\s*\)', ' ', cleaned_text, flags=re.DOTALL)  # (text)
                    cleaned_text = re.sub(r'\{\s*[^}]*\s*\}', ' ', cleaned_text, flags=re.DOTALL)  # {text}
                    
                    # Handle partial brackets - open bracket with no close bracket
                    cleaned_text = re.sub(r'\[\s*[^\]]*$', ' ', cleaned_text, flags=re.DOTALL)
                    cleaned_text = re.sub(r'\(\s*[^)]*$', ' ', cleaned_text, flags=re.DOTALL)
                    cleaned_text = re.sub(r'\{\s*[^}]*$', ' ', cleaned_text, flags=re.DOTALL)
                    
                    # Handle partial brackets - close bracket with no open bracket
                    cleaned_text = re.sub(r'^[^\[]*\]', ' ', cleaned_text, flags=re.DOTALL)
                    cleaned_text = re.sub(r'^[^(]*\)', ' ', cleaned_text, flags=re.DOTALL)
                    cleaned_text = re.sub(r'^[^{]*\}', ' ', cleaned_text, flags=re.DOTALL)
                    
                    # Continue processing as before
                    cleaned_text = re.sub(r'\d+', '', cleaned_text)
                    cleaned_text = cleaned_text.lower()
                    cleaned_text = cleaned_text.translate(translator)
                    
                    # Clean up extra spaces from bracket removals before splitting
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                    
                    sentences = re.split(r'(?<=[.!?:;])\s*', cleaned_text)
                    for sentence in sentences:
                        sentence = re.sub(r'[.!?:;]', ' ', sentence)
                        sentence = re.sub(r'\s+', ' ', sentence)
                        sentence = sentence.strip()
                        
                        if sentence:
                            words = sentence.split()
                            if words:
                                all_sentences.append(words)
                except json.JSONDecodeError:
                    continue
    
    return all_sentences