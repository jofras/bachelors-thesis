#!/bin/bash
#SBATCH --job-name=tomotopy_podcast_lda
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=5G
#SBATCH --time=5-00:00:00
#SBATCH --output=tomotopy-lda-%j.out
#SBATCH --error=tomotopy-lda-%j.err

source /cluster/apps/local/env2lmod.sh
module load stack/2024-06 gcc/12.2.0
module load python/3.9.18

source ~/myenv/bin/activate
cd /cluster/scratch/jquinn/

echo "Starting tomotopy LDA job at $(date)"

python lda_new.py

echo "tomotopy LDA job finished at $(date)"