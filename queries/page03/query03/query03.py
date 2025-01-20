import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_principais_justificativas_view

# Quais as 10 principais justificativas para vôos cancelados

spark = create_spark_session()

principais_justificativas_view = create_principais_justificativas_view(spark)

top_ten_segments = spark.sql(
    f"""
        SELECT * 
        FROM {principais_justificativas_view}
    """
)

top_ten_segments.show()

top_ten_segments.rdd.saveAsTextFile("queries/page03/query03/output")

spark.stop()