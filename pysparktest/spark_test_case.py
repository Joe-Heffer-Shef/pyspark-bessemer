import unittest

import pyspark.sql


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # Create Spark context
        self.sc = pyspark.sql.SparkSession.builder.getOrCreate()
