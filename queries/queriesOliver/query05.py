import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Quais são os 10 trechos que mais tiveram vôos atrasados por aeroporto

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""
    SELECT 
    a."nome" AS "nome_aeroporto",
    vf."cidade_origem",
    vf."pais_origem",
    vf."cidade_destino",
    vf."pais_destino",
    vf."qtdAtrasadosTotal"
FROM 
    public."dAeroporto" AS a
CROSS JOIN LATERAL (
    SELECT
        aOrig."Cidade" AS cidade_origem,
        aOrig."Pais" AS pais_origem,
        aDest."Cidade" AS cidade_destino,
        aDest."Pais" AS pais_destino,
        SUM(v."qtdAtrasados") AS "qtdAtrasadosTotal"
    FROM 
        public."fVoo" AS v
    INNER JOIN
        public."dAeroporto" AS aDest ON v."idAeroDest" = aDest."id"
    INNER JOIN
        public."dAeroporto" AS aOrig ON v."idAeroOrig" = aOrig."id"
    WHERE a."id" = aOrig."id"
    GROUP BY 
        aOrig."Cidade", aOrig."Pais", aDest."Cidade", aDest."Pais"
) AS vf
ORDER BY
        "qtdAtrasadosTotal" DESC
LIMIT 10;
""")

top_ten_segments.rdd.saveAsTextFile("queries/queryOliver/output")

spark.stop()