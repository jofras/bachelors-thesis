#!/bin/bash
#SBATCH --job-name=podcast_mod12_w2v
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=1000
#SBATCH --time=2:00:00
#SBATCH --output=slurm-%A_%a.out
#SBATCH --error=slurm-%A_%a.err
#SBATCH --array=0-11

source /cluster/apps/local/env2lmod.sh
module load stack/2024-06 gcc/12.2.0
module load python/3.9.18

source ~/myenv/bin/activate
cd /cluster/scratch/jquinn/

# Pass the SLURM_ARRAY_TASK_ID as an argument to the Python script
python run_w2v_split12.py ${SLURM_ARRAY_TASK_ID}