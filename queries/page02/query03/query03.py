import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_segments_view

# Qual o acréscimo médio de viagem nos 10 trechos com maiores atrasos

spark = create_spark_session()

segments_view = create_segments_view(spark)

top_ten_segments = spark.sql(f"""
    SELECT
        cidade_origem,
        pais_origem,
        cidade_destino,
        pais_destino,
        qtdAtrasadosTotal,
        ROUND(atrasoMinTotais/qtdVoosTotal, 2) AS acrescimoMedioMinutos
    FROM
        {segments_view}
    ORDER BY
	    qtdAtrasadosTotal
    DESC
    LIMIT 10
""")

top_ten_segments.rdd.saveAsTextFile("queries/page02/query03/output")

spark.stop()