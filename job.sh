#!/bin/bash
#SBATCH --comment="PySpark test - Bessemer Slurm job script"
#SBATCH --time=00:05:00
#SBATCH --mem=1G

# What's this for? This is required for Conda to work properly for some reason
export SLURM_EXPORT_ENV=ALL

# Exit immediately if a command exits with a non-zero exit status.
set -e

# Load dependencies
# https://docs.hpc.shef.ac.uk/en/latest/bessemer/software/apps/python.html
module load Anaconda3/2019.07
module load Java/11

java -version

# Activate conda environment
# https://docs.hpc.shef.ac.uk/en/latest/bessemer/software/apps/python.html#using-conda-environments
source activate pyspark_env

# Show package versions
conda --version
python --version
conda env export
python -c "import pyspark.sql; print('PySpark version', pyspark.sql.SparkSession.builder.getOrCreate().version)"

# Run a simple PySpark job
# https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html
python -c "import pyspark.sql; spark = SparkSession.builder.getOrCreate();\
df = spark.createDataFrame([\
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),\
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),\
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))\
], schema='a long, b double, c string, d date, e timestamp');\
print(df);"

# TODO Jupyter notebook
