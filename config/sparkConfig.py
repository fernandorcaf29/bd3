from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars", "bd3/config/drivers/postgresql-42.7.4.jar").getOrCreate()
    return spark

data = [("Alice", 29), ("Bob", 35), ("Cathy", 23)]
columns = ["Nome", "Idade"]

spark = create_spark_session()

df = spark.createDataFrame(data, columns)

# Mostre o DataFrame
df.show()