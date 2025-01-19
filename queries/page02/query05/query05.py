import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_ordered_segments_view

# Quais são os 10 trechos que mais tiveram vôos atrasados por aeroporto

spark = create_spark_session()

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments = spark.sql(f"""

""")

top_ten_segments.rdd.saveAsTextFile("queries/page02/query05/output")

spark.stop()

"""

CONSULTA NÃO FUNCIONOU COM O SPARK MAS FUNCIONOU NO POSTGRES: ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED

    SELECT
        t.nome AS nome_aeroporto,
        vf.cidade_origem,
        vf.pais_origem,
        vf.cidade_destino,
        vf.pais_destino,
        vf.qtdAtrasadosTotal
    FROM 
        dAeroporto AS t
    CROSS JOIN LATERAL (
        SELECT
            aOrig.Cidade AS cidade_origem,
            aOrig.Pais AS pais_origem,
            aDest.Cidade AS cidade_destino,
            aDest.Pais AS pais_destino,
            SUM(v.qtdAtrasados) AS qtdAtrasadosTotal
        FROM 
            fVoo AS v
        INNER JOIN
            dAeroporto AS aDest ON v.idAeroDest = aDest.id
        INNER JOIN
            dAeroporto AS aOrig ON v.idAeroOrig = aOrig.id
        WHERE
            t.id = aOrig.id
        GROUP BY 
            aOrig.Cidade, aOrig.Pais, aDest.Cidade, aDest.Pais
        ORDER BY
            qtdAtrasadosTotal DESC
        LIMIT 10
    ) AS vf;

"""