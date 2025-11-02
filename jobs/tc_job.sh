#!/bin/bash
#SBATCH --job-name=tc
#SBATCH --array=0-69
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8000
#SBATCH --time=00:10:00
#SBATCH --output=slurm-%a.out
#SBATCH --error=slurm-%a.err

source /cluster/apps/local/env2lmod.sh
module load stack/2024-06 gcc/12.2.0
module load python/3.9.18

source ~/myenv/bin/activate
cd /cluster/scratch/jquinn/
python run_tc.py
