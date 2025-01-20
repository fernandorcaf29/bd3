import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Qual o acréscimo médio de viagem nos 10 trechos com maiores atrasos

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""
SELECT
	aOrig."Cidade" AS cidade_origem,
	aOrig."Pais" AS pais_origem,
	aDest."Cidade" AS cidade_destino,
	aDest."Pais" AS pais_destino,
	SUM("fVoo"."qtdAtrasados") AS "qtdAtrasadosTotal",
	ROUND(SUM("fVoo"."atrasoMinTotal")/ SUM("fVoo"."qtdVoos"), 2) AS "acrescimoMedioMinutos"
FROM 
	"fVoo"
INNER JOIN
	"dAeroporto" AS aDest ON "fVoo"."idAeroDest" = aDest."id"
INNER JOIN
	"dAeroporto" AS aOrig ON "fVoo"."idAeroOrig" = aOrig."id"
GROUP BY 
	aOrig."Cidade", aOrig."Pais", aDest."Cidade", aDest."Pais"
ORDER BY
	"qtdAtrasadosTotal" DESC
LIMIT 10; 
""")

top_ten_segments.rdd.saveAsTextFile("queries/page02/query03/output")

spark.stop()