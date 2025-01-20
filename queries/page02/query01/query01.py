import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_segments_view

# Quais são os 10 trechos que mais tiveram vôos cancelados

spark = create_spark_session()

segments_view = create_segments_view(spark)

top_ten_segments = spark.sql(f"""
    SELECT
        cidade_origem,
        pais_origem,
        cidade_destino,
        pais_destino,
        qtdCanceladosTotal
    FROM
        {segments_view}     
    ORDER BY
        qtdCanceladosTotal
    DESC
    LIMIT 10;
""")

top_ten_segments.rdd.saveAsTextFile("queries/page02/query01/output")

spark.stop()