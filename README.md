# PySpark on Bessemer

This is a template for using [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), the interface for Apache Spark in Python, on the [Bessemer](https://docs.hpc.shef.ac.uk/en/latest/bessemer/index.html) High-Performance Computing cluster at [The University of Sheffield](https://www.sheffield.ac.uk/).

This example is designed to run a Spark instance in standalone mode (i.e. a Spark "cluster" will be launched for the duration of the job) on a single node and use the specified number of cores.

See also: [Spark on ShARC](https://docs.hpc.shef.ac.uk/en/latest/sharc/software/apps/spark.html)

# Installation

Log into Bessemer and set up a Python virtual environment in your home directory using Conda (see [Creating an environment from an environment.yml file](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)).

```bash
conda env create --file environment.yaml
```

# Usage

Submit the job to the Slurm scheduler:

```bash
sbatch job.sh
```

# Common problems

## Illegal reflective access operation

The warnings will be displayed whenever Spark is loaded using Java version >= 9.

```
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/home/cs1jsth/.conda/envs/pyspark_env/lib/python3.8/site-packages/pyspark/jars/spark-unsafe_2.12-3.1.1.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

## Unable to load native-hadoop library

This warning occurs because Hadoop isn't installed.