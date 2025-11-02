#!/bin/bash
#SBATCH --job-name=podcast_glove_train
#SBATCH --array=0-11
#SBATCH --output=logs/pgt_%a_%j.out
#SBATCH --error=logs/pgt_%a_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G
#SBATCH --time=12:00:00

module load gcc/12.2.0
module load python/3.9.18

set -e

# setup
MOD12=$SLURM_ARRAY_TASK_ID
BASE_DIR="/cluster/scratch/jquinn/stanford_glove"
WORKDIR="/cluster/scratch/jquinn/mod12runs/glv_split12/task_${MOD12}"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# paths
CORPUS="/cluster/scratch/jquinn/mod12runs/glv_split12/podcast_glv${MOD12}.txt"
VOCAB_FILE="vocab_${MOD12}.txt"
COOCCURRENCE_FILE="cooccurrence_${MOD12}.bin"
COOCCURRENCE_SHUF_FILE="cooccurrence_${MOD12}.shuf.bin"
SAVE_FILE="podcast_vectors_${MOD12}"

# hyperparams
VERBOSE=2
MEMORY=32
VOCAB_MIN_COUNT=5
VECTOR_SIZE=300
MAX_ITER=50
WINDOW_SIZE=15
BINARY=2
NUM_THREADS=32
X_MAX=100

# use correct python bin
if hash python 2>/dev/null; then
    PYTHON=python
else
    PYTHON=python3
fi

# copy glove bins
cp "$BASE_DIR/build/"* ./
chmod +x vocab_count cooccur shuffle glove

echo "STARTING JOB: $SLURM_JOB_ID | ARRAY_TASK_ID: $MOD12"
echo "Using corpus: $CORPUS"

# 1: vocab count
echo "Step 1: Counting vocabulary..."
time ./vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < "$CORPUS" > "$VOCAB_FILE"

# 2: cooccurrence
echo "Step 2: Computing cooccurrence matrix..."
time ./cooccur -memory $MEMORY -vocab-file "$VOCAB_FILE" -verbose $VERBOSE -window-size $WINDOW_SIZE < "$CORPUS" > "$COOCCURRENCE_FILE"

# 3: shuffle
echo "Step 3: Shuffling cooccurrence data..."
time ./shuffle -memory $MEMORY -verbose $VERBOSE < "$COOCCURRENCE_FILE" > "$COOCCURRENCE_SHUF_FILE"

# 4: train
echo "Step 4: Training GloVe model..."
time ./glove -save-file "$SAVE_FILE" -threads $NUM_THREADS -input-file "$COOCCURRENCE_SHUF_FILE" -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file "$VOCAB_FILE" -verbose $VERBOSE

echo "JOB FINISHED for task $MOD12"
