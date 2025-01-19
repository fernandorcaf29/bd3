import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresConfig import url, properties

def transform_tables_into_view(spark, table_names):
    for table_name in table_names:
        transform_table_into_view(spark, table_name)
    return 

def transform_table_into_view(spark, table_name):
    return spark.read.jdbc(url, f'"{table_name}"', properties=properties).createOrReplaceTempView(table_name)