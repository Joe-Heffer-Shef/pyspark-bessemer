import numpy as np
import pandas as pd
import pyspark.sql


def test_pandas(session: pyspark.sql.SparkSession):
    # Enable Arrow-based columnar data transfers
    # session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Generate a Pandas DataFrame
    pdf = pd.DataFrame(np.random.rand(100, 3))

    # Create a Spark DataFrame from a Pandas DataFrame using Arrow
    df = session.createDataFrame(pdf)

    # Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
    result_pdf = df.select("*").toPandas()

    print(result_pdf.head())
