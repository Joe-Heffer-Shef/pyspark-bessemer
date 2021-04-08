"""
PySpark "Hello World" example

Based on the quick-start guide in the docs
https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html
"""

import datetime
import random
import uuid

from pyspark.sql import SparkSession


def test_data_frame(session: SparkSession):
    # Create data frame
    df = session.createDataFrame([
        # Generate some random data
        (i, random.random(), uuid.uuid4().hex,
         datetime.date(1970, 1, 1) + datetime.timedelta(days=i),
         datetime.datetime(1970, 1, 1, 12) + datetime.timedelta(hours=i))
        for i in range(1000)],
        schema='a long, b double, c string, d date, e timestamp')

    df.collect()
