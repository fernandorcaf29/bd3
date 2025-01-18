from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars", "drivers/postgresql-42.7.4.jar").getOrCreate()
    return spark

