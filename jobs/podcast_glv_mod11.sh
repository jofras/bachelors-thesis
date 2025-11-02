#!/bin/bash
#SBATCH --job-name=podcast_glove_train
#SBATCH --array=0-10
#SBATCH --output=logs/pgt_%a_%j.out
#SBATCH --error=logs/pgt_%a_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G
#SBATCH --time=12:00:00

module load gcc/12.2.0
module load python/3.9.18

set -e

# --- Job Setup ---
MOD11=$SLURM_ARRAY_TASK_ID
BASE_DIR="/cluster/scratch/jquinn/stanford_glove"
WORKDIR="/cluster/scratch/jquinn/mod11runs/split11/task_${MOD11}"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# --- Paths ---
CORPUS="/cluster/scratch/jquinn/mod11runs/split11/podcast_glv${MOD11}.txt"
VOCAB_FILE="vocab_${MOD11}.txt"
COOCCURRENCE_FILE="cooccurrence_${MOD11}.bin"
COOCCURRENCE_SHUF_FILE="cooccurrence_${MOD11}.shuf.bin"
SAVE_FILE="podcast_vectors_${MOD11}"

# --- Hyperparameters ---
VERBOSE=2
MEMORY=32
VOCAB_MIN_COUNT=5
VECTOR_SIZE=300
MAX_ITER=50
WINDOW_SIZE=15
BINARY=2
NUM_THREADS=32
X_MAX=100

# --- Use correct Python binary ---
if hash python 2>/dev/null; then
    PYTHON=python
else
    PYTHON=python3
fi

# --- Copy prebuilt GloVe binaries ---
cp "$BASE_DIR/build/"* ./
chmod +x vocab_count cooccur shuffle glove

echo "STARTING JOB: $SLURM_JOB_ID | ARRAY_TASK_ID: $MOD11"
echo "Using corpus: $CORPUS"

# --- Step 1: Vocabulary count ---
echo "Step 1: Counting vocabulary..."
time ./vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < "$CORPUS" > "$VOCAB_FILE"

# --- Step 2: Cooccurrence ---
echo "Step 2: Computing cooccurrence matrix..."
time ./cooccur -memory $MEMORY -vocab-file "$VOCAB_FILE" -verbose $VERBOSE -window-size $WINDOW_SIZE < "$CORPUS" > "$COOCCURRENCE_FILE"

# --- Step 3: Shuffle ---
echo "Step 3: Shuffling cooccurrence data..."
time ./shuffle -memory $MEMORY -verbose $VERBOSE < "$COOCCURRENCE_FILE" > "$COOCCURRENCE_SHUF_FILE"

# --- Step 4: Train ---
echo "Step 4: Training GloVe model..."
time ./glove -save-file "$SAVE_FILE" -threads $NUM_THREADS -input-file "$COOCCURRENCE_SHUF_FILE" -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file "$VOCAB_FILE" -verbose $VERBOSE

echo "JOB FINISHED for task $MOD11"
