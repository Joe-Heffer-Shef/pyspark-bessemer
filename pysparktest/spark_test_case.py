import unittest

import pyspark.sql
from pyspark.sql.types import StructType

# Create Spark session (only one can exist per process)
# https://stackoverflow.com/a/41513805
SPARK_SESSION = pyspark.sql.SparkSession.builder.getOrCreate()


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        self.session = SPARK_SESSION

        self.df = self.session.createDataFrame(data=list(),
                                               schema=StructType(list()))

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
