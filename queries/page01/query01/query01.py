import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Quais os principais trechos (pares <origem, destino>) e seu percentual de frequência - Vôos realizados

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""
    SELECT *
    FROM {ordered_segments_view}
    LIMIT 10
""")

top_ten_segments.rdd.saveAsTextFile("queries/page01/query01/output")

spark.stop()