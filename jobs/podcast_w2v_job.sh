#!/bin/bash
#SBATCH --job-name=podcast_w2v
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=2000
#SBATCH --time=5:00:00
#SBATCH --output=slurm-%a.out
#SBATCH --error=slurm-%a.err

source /cluster/apps/local/env2lmod.sh
module load stack/2024-06 gcc/12.2.0
module load python/3.9.18

source ~/myenv/bin/activate
cd /cluster/scratch/jquinn/
python run_w2v.py
