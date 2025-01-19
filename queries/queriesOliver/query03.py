import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Qual o acréscimo médio de viagem nos 10 trechos com maiores atrasos

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""
    
""")

top_ten_segments.rdd.saveAsTextFile("queries/queryOliver/output")

spark.stop()