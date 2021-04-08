#!/bin/bash
#SBATCH --comment="PySpark test - Bessemer Slurm job script"
#SBATCH --time=00:05:00
#SBATCH --mem=1G

# Exit immediately if a command exits with a non-zero exit status.
set -e

# Load dependencies
# https://docs.hpc.shef.ac.uk/en/latest/bessemer/software/apps/python.html
module load Anaconda3/2019.07
module load Java/11

# Set Java options
# This option is required for PySpark when using Java 11
# https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies
export JAVA_OPTS=$JAVA_OPTS" -Dio.netty.tryReflectionSetAccessible=true"

# Show Java context
echo $JAVA_HOME
echo $JAVA_OPTS
java -version

# Activate conda environment
# https://docs.hpc.shef.ac.uk/en/latest/bessemer/software/apps/python.html#using-conda-environments
source activate pyspark_env

# Show package versions
conda --version
python --version

# Run a simple PySpark job
python -m unittest discover --failfast pysparktest
