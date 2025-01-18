import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresConfig import url, properties

def transform_table_into_view(spark, table_name, view_name):
    return spark.read.jdbc(url, table_name, properties=properties).createOrReplaceTempView(view_name)
