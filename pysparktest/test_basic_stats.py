"""
PySpark basic statistics example
http://spark.apache.org/docs/latest/ml-statistics.html
"""

from pysparktest.spark_test_case import SparkTestCase
from pyspark.ml.linalg import Vectors


class DataFrameTestCase(SparkTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Create data frame
        data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
                (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
                (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
                (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
        self.df = self.session.createDataFrame(data, ["features"])

    def test_correlation(self):
        from pyspark.ml.stat import Correlation

        # http://spark.apache.org/docs/latest/ml-statistics.html#correlation
        result = Correlation.corr(self.df, 'features')
        result.show()
        print("Pearson correlation matrix:\n{}".format(result[0]))

    def test_chi_square(self):
        from pyspark.ml.stat import ChiSquareTest

        result = ChiSquareTest.test(self.df, "features", "label").head()
        print("pValues: " + str(result.pValues))
        print("degreesOfFreedom: " + str(result.degreesOfFreedom))
        print("statistics: " + str(result.statistics))
