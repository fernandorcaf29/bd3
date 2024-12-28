from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "drivers/postgresql-42.7.4.jar").getOrCreate()