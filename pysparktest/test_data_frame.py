"""
PySpark "Hello World" example

Based on the quick-start guide in the docs
https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html
"""

import datetime

from pysparktest.spark_test_case import SparkTestCase


class DataFrameTestCase(SparkTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Create data frame
        self.df = self.sc.createDataFrame([
            (1, 2., 'string1', datetime.date(2000, 1, 1), datetime.datetime(
                2000, 1, 1, 12, 0)),
            (2, 3., 'string2', datetime.date(2000, 2, 1),
             datetime.datetime(2000, 1, 2, 12, 0)),
            (3, 4., 'string3', datetime.date(2000, 3, 1),
             datetime.datetime(2000, 1, 3, 12, 0))
        ], schema='a long, b double, c string, d date, e timestamp')

    def test_print_schema(self):
        self.df.printSchema()

    def test_show(self):
        self.df.show()

    def test_collect(self):
        self.df.collect()
