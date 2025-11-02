#!/bin/bash
#SBATCH --job-name=nela_glove_train
#SBATCH --output=nela_glove_train_%j.out
#SBATCH --error=nela_glove_train_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G
#SBATCH --time=12:00:00

module load gcc/9.3.0
module load python/3.8

set -e

make

CORPUS=/cluster/scratch/jquinn/r2_nela_new_glv.txt
VOCAB_FILE=r2__nela_vocab.txt
COOCCURRENCE_FILE=cooccurrence.bin
COOCCURRENCE_SHUF_FILE=cooccurrence.shuf.bin
BUILDDIR=build
SAVE_FILE=r2_nela_vectors
VERBOSE=2
MEMORY=32
VOCAB_MIN_COUNT=5
VECTOR_SIZE=300
MAX_ITER=50
WINDOW_SIZE=15
BINARY=2
NUM_THREADS=32
X_MAX=100

if hash python 2>/dev/null; then
    PYTHON=python
else
    PYTHON=python3
fi

echo "Step 1: Counting vocabulary..."
time $BUILDDIR/vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < $CORPUS > $VOCAB_FILE

echo "Step 2: Computing cooccurrence matrix..."
time $BUILDDIR/cooccur -memory $MEMORY -vocab-file $VOCAB_FILE -verbose $VERBOSE -window-size $WINDOW_SIZE < $CORPUS > $COOCCURRENCE_FILE

echo "Step 3: Shuffling cooccurrence data..."
time $BUILDDIR/shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE

echo "Step 4: Training GloVe model..."
time $BUILDDIR/glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE

# Optional evaluation if desired
# if [ "$1" = 'matlab' ]; then
#     echo "Step 5: Evaluating vectors with MATLAB..."
#     matlab -nodisplay -nodesktop -nojvm -nosplash < ./eval/matlab/read_and_evaluate.m 1>&2 
# elif [ "$1" = 'octave' ]; then
#     echo "Step 5: Evaluating vectors with Octave..."
#     octave < ./eval/octave/read_and_evaluate_octave.m 1>&2
# else
#     echo "Step 5: Evaluating vectors with Python..."
#     $PYTHON eval/python/evaluate.py
# fi
