import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dto.readTable import transform_table_into_view
from config.sparkConfig import create_spark_session
from utils.views import create_segments_view

# Quais os principais trechos (pares <origem, destino>) e seu percentual de frequência - Vôos realizados

spark = create_spark_session()

transform_table_into_view(spark, r'"fVoo"', "fVoo")

transform_table_into_view(spark, r'"dAeroporto"', "dAeroporto")

create_segments_view(spark)

topTrechos = spark.sql("""
SELECT 
	*, 
	ROUND(t.qtdVoosTotal/CAST((SELECT SUM(v.qtdVoos) FROM fVoo AS v) AS DECIMAL) * 100, 4) AS perc
FROM trechos_mais_frequentes AS t
""")

topTrechos.rdd.saveAsTextFile("queries/query01/output")

spark.stop()