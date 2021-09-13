from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "3g") \
        .appName('TRG') \
        .getOrCreate()

    return spark
