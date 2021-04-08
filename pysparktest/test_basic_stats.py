"""
PySpark basic statistics example
http://spark.apache.org/docs/latest/ml-statistics.html
"""

import unittest

from pysparktest.spark_test_case import SparkTestCase
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation


class DataFrameTestCase(SparkTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Create data frame
        data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
                (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
                (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
                (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
        self.df = self.sc.createDataFrame(data, ["features"])

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
