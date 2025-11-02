import os

SOURCE_DIR = ""
OUTPUT_DIR = ""
TARGET_SIZE = 50 * 1024 * 1024  # 50 MB

# step 1: list files and sizes
files = []
for fname in os.listdir(SOURCE_DIR):
    fpath = os.path.join(SOURCE_DIR, fname)
    if os.path.isfile(fpath):
        fsize = os.path.getsize(fpath)
        files.append({'name': fname, 'size': fsize})

# step 2: sort descending
files.sort(key=lambda x: x['size'], reverse=True)

# step 3: best-fit-decreasing packing
batches = []  # Each batch: {'files': [...], 'used_size': int}

for file in files:
    best_batch_index = None
    min_leftover = TARGET_SIZE + 1  # Initialize large
    
    for i, batch in enumerate(batches):
        leftover = TARGET_SIZE - batch['used_size']
        if file['size'] <= leftover and leftover - file['size'] < min_leftover:
            min_leftover = leftover - file['size']
            best_batch_index = i
    
    if best_batch_index is None:
        # Create new batch
        batches.append({'files': [file], 'used_size': file['size']})
    else:
        # Add to best batch
        batches[best_batch_index]['files'].append(file)
        batches[best_batch_index]['used_size'] += file['size']

# step 4: print summary
print(f"Total files: {len(files)}")
print(f"Total batches: {len(batches)}")
for i, batch in enumerate(batches, 1):
    size_mb = batch['used_size'] / (1024*1024)
    print(f"Batch {i}: {len(batch['files'])} files, total size {size_mb:.2f} MB")

# step 5: write batches to concatenated files
os.makedirs(OUTPUT_DIR, exist_ok=True)

for i, batch in enumerate(batches):
    output_path = os.path.join(OUTPUT_DIR, f'bdf{i:01d}.txt')
    with open(output_path, 'wb') as outfile:
        for f in batch['files']:
            fpath = os.path.join(SOURCE_DIR, f['name'])
            with open(fpath, 'rb') as infile:
                while True:
                    chunk = infile.read(1024*1024)
                    if not chunk:
                        break
                    outfile.write(chunk)
    print(f"Created {output_path}")
