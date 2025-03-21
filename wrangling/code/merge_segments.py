import json
import os
import glob

"""
This script merges all turnText segments originating from the same URL (i.e. podcast).

I haven't integrated this into the filefinder/proc/op trio because it requires "caching" jsons across files, 
and instead of trying to build a prettier file preprocessing framework i should probably get on with training a
word2vec and gloVe model on the whole corpus. I might get back to it later though.
"""

def merge_on_url(input_file, output_file, cached_url, cached_turn_text):
    
    """
    helper method to merge turnTexts with the same url
    """
    
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

def batch_merge_on_url(input_folder="../data/segments/text_url/raw", output_folder="../data/segments/text_url/merged", in_prefix="u", out_prefix="mu"):

    """
    merge all jsons with the same url
    """

    # create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # get all url-turnText segments
    file_pattern = os.path.join(input_folder, f"{in_prefix}seg*.jsonl")
    input_files = glob.glob(file_pattern)
    
    if not input_files:
        print(f"no matching '{in_prefix}seg*.jsonl' files found in {input_folder}")
        return 0, 0
    
    input_files.sort()
    successful, failed = 0, 0
    cached_url, cached_turn_text = None, ''
    last_output_file = None
    
    for input_file in input_files:
        base_name = os.path.basename(input_file)
        seg_num = base_name.replace(f"{in_prefix}seg", "").replace(".jsonl", "")
        output_file = os.path.join(output_folder, f"{out_prefix}seg{seg_num}.jsonl")
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