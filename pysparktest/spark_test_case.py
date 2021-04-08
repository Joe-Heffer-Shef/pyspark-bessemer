import unittest

import pyspark.sql
from pyspark.sql.types import StructType


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # Create Spark session (only one can exist per process)
        # https://stackoverflow.com/a/41513805
        self.session = pyspark.sql.SparkSession.builder.getOrCreate()

        self.df = self.session.createDataFrame(data=list(),
                                               schema=StructType(list()))

    def tearDown(self) -> None:
        self.session.stop()

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
