"""
PySpark basic statistics example
http://spark.apache.org/docs/latest/ml-statistics.html
"""

from pyspark.ml.linalg import Vectors

from pysparktest.spark_test_case import SparkTestCase


class BasicStatisticsTestCase(SparkTestCase):
    def setUp(self) -> None:
        super().setUp()

        # Create data frame
        data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
                (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
                (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
                (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
        self.df = self.session.createDataFrame(data, ["features"])

    def test_correlation(self):
        # http://spark.apache.org/docs/latest/ml-statistics.html#correlation
        from pyspark.ml.stat import Correlation

        result = Correlation.corr(self.df, 'features')
        result.show()
        print("Pearson correlation matrix:\n{}".format(result[0]))

    def test_chi_square(self):
        # http://spark.apache.org/docs/latest/ml-statistics.html#chisquaretest
        from pyspark.ml.stat import ChiSquareTest

        data = [(0.0, Vectors.dense(0.5, 10.0)),
                (0.0, Vectors.dense(1.5, 20.0)),
                (1.0, Vectors.dense(1.5, 30.0)),
                (0.0, Vectors.dense(3.5, 30.0)),
                (0.0, Vectors.dense(3.5, 40.0)),
                (1.0, Vectors.dense(3.5, 40.0))]
        df = self.session.createDataFrame(data, ["label", "features"])

        r = ChiSquareTest.test(df, "features", "label").head()
        print("pValues: " + str(r.pValues))
        print("degreesOfFreedom: " + str(r.degreesOfFreedom))
        print("statistics: " + str(r.statistics))
