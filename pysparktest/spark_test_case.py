import unittest
import logging
import os

import pyspark.sql
from pyspark.sql.types import StructType

# Log these config options
# https://spark.apache.org/docs/latest/configuration.html
INTERESTING_CONFIG_KEYS = {
    'spark.local.dir',
    'spark.executor.cores',
    'spark.default.parallelism',
    'spark.cores.max',
}


class SparkTestCase(unittest.TestCase):
    def setUp(self):
        # Configure logging
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=os.getenv('LOG_LEVEL', 'WARNING'))

        # Create Spark session (only one can exist per process)
        # https://stackoverflow.com/a/41513805
        self.session = pyspark.sql.SparkSession.builder.getOrCreate()

        # Log config values
        for key in INTERESTING_CONFIG_KEYS:
            self.logger.info("%s=%s", key, self.session.conf.get(key))

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
