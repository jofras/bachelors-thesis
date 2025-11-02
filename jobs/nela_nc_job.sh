#!/bin/bash
#SBATCH --job-name=nc
#SBATCH --array=0-21
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2000
#SBATCH --time=00:05:00
#SBATCH --output=slurm-%a.out
#SBATCH --error=slurm-%a.err

source /cluster/apps/local/env2lmod.sh
module load stack/2024-06 gcc/12.2.0
module load python/3.9.18

source ~/myenv/bin/activate
cd /cluster/scratch/jquinn/
python run_nc.py
