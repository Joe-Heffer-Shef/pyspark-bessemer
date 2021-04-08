import unittest

import pyspark.sql
from pyspark.sql.types import StructType


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # Create Spark session
        self.session = pyspark.sql.SparkSession.builder.getOrCreate()

        self.df = self.session.createDataFrame(data=list(),
                                               schema=StructType(list()))

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
