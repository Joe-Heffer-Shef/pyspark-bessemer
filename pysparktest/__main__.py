"""
PySpark test examples
"""

import logging
import os

import pyspark.sql

import pysparktest.test_data_frame
import pysparktest.test_basic_stats

LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
LOGGER = logging.getLogger(__name__)


def run_tests(session: pyspark.sql.SparkSession):
    pysparktest.test_data_frame.test_data_frame(session)
    pysparktest.test_basic_stats.test_correlation(session)
    pysparktest.test_basic_stats.test_chi_square(session)


def main():
    # Create Spark session (only one can exist per process) as global variable
    # to be shared between all unit tests
    # https://stackoverflow.com/a/41513805
    with pyspark.sql.SparkSession.builder.appName(
            __name__).getOrCreate() as session:
        session.sparkContext.setLogLevel(LOG_LEVEL)

        # Log configuration
        for key, value in session.sparkContext.getConf().getAll():
            LOGGER.info("%s=%s", key, value)

        run_tests(session)


if __name__ == '__main__':
    main()
