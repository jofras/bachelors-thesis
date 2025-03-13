import json
from pathlib import Path

"""
used this to check for unbalanced brackets in the full transcripts
"""

def test_brackets(input_folder="data/segments/text_url/merged", seg_prefix="museg"):
    """
    checks all files in the input folder for unbalanced brackets in the turnText field.

    notes:
        after manual inspection, there aren't too many unbalanced brackets
        i decided to only remove fully parenthesized parts of the text 
        removing everything after an unbalanced bracket would remove much of each sample, 
        especially now that each podcast is in a single turnText entry.
    """
    for file in Path(input_folder).glob(f"{seg_prefix}*.jsonl"):
        with open(file, 'r', encoding='utf-8') as infile:
            for line_num, line in enumerate(infile, 1):
                try:
                    data = json.loads(line.strip())
                    if 'turnText' in data:
                        text = data['turnText']
                        if not check_brackets(text):
                            print(f"warning: unbalanced brackets in line {line_num} of {file}")
                except json.JSONDecodeError:
                    print(f"warning: couldn't parse json at line {line_num} of {file}")
                    continue
        print(f"finished checking {file}")
    print("test complete")

def check_brackets(text):
    
    stack = []
    
    for char in text:
        if char in "([{":
            stack.append(char)
        elif char in ")]}":
            if not stack:
                return False
            if char == ')' and stack[-1] == '(':
                stack.pop()
            elif char == ']' and stack[-1] == '[':
                stack.pop()
            elif char == '}' and stack[-1] == '{':
                stack.pop()
            else:
                return False
            
    return not stack

if __name__ == "__main__":
    test_brackets()

