import unittest
import logging
import os

import pyspark.sql
from pyspark.sql.types import StructType

LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')

# Create Spark session (only one can exist per process) as global variable
# to be shared between all unit tests
# https://stackoverflow.com/a/41513805
SPARK_SESSION = pyspark.sql.SparkSession.builder.appName(
    __name__).getOrCreate()


class SparkTestCase(unittest.TestCase):
    session = SPARK_SESSION

    def setUp(self):
        # Configure logging
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=LOG_LEVEL)

        self.session.sparkContext.setLogLevel(LOG_LEVEL)

        # Log configuration
        for key, value in self.session.sparkContext.getConf().getAll():
            self.logger.info("%s=%s", key, value)

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
