import os

from pyspark.sql import SparkSession


def load_data(file_path):

    BASE_DIR = os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )

    JDBC_JAR = os.path.join(
        BASE_DIR,
        "jars",
        "postgresql-42.7.13.jar"
    )

    spark = SparkSession.builder \
    .appName("BigBlackMoneyExtraction") \
    .config("spark.driver.extraClassPath", JDBC_JAR) \
    .config("spark.executor.extraClassPath", JDBC_JAR) \
    .getOrCreate()

    data = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    return data