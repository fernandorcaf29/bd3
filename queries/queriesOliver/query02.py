import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Quais são os 10 trechos que mais tiveram vôos atrasados

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""
    SELECT
    orig."Cidade" AS "Cidade de Origem",
	dest."Cidade" AS "Cidade de Destino",
    orig."Pais" AS "País de Origem",
    dest."Pais" AS "País de Destino",
    SUM(f."qtdAtrasados") AS "Total de Voos Atrasados"
FROM
    "fVoo" f
JOIN
	"dAeroporto" dest ON f."idAeroDest" = dest.id

JOIN 
    "dAeroporto" orig ON f."idAeroOrig" = orig.id
GROUP BY
    orig."Cidade", orig."Pais",
    dest."Cidade", dest."Pais"
ORDER BY
    "Total de Voos Atrasados" DESC
LIMIT 10;
""")

top_ten_segments.rdd.saveAsTextFile("queries/queryOliver/output")

spark.stop()