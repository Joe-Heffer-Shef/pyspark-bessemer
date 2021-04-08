import unittest

import pyspark.sql


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # Create Spark context
        self.sc = pyspark.sql.SparkSession.builder.getOrCreate()

        self.df = self.sc.createDataFrame(data=list())

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
