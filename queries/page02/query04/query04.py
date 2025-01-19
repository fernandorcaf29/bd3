import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from dto.readTable import transform_tables_into_view


# Quais são os 10 trechos que mais tiveram vôos cancelados por aeroporto

spark = create_spark_session()

transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

top_ten_segments = spark.sql(
f"""

""")

top_ten_segments.rdd.saveAsTextFile("queries/page02/query04/output")

spark.stop()

"""

CONSULTA NÃO FUNCIONOU COM O SPARK MAS FUNCIONOU NO POSTGRES: ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED
 
    SELECT
        t.nome AS nome_aeroporto,
        vf.cidade_origem,
        vf.pais_origem,
        vf.cidade_destino,
        vf.pais_destino,
        vf.qtdCanceladosTotal
    FROM 
        dAeroporto AS t
    CROSS JOIN LATERAL (
        SELECT
            aOrig.Cidade AS cidade_origem,
            aOrig.Pais AS pais_origem,
            aDest.Cidade AS cidade_destino,
            aDest.Pais AS pais_destino,
            SUM(v.qtdCancelados) AS qtdCanceladosTotal
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
            qtdCanceladosTotal DESC
        LIMIT 10
    ) AS vf;

"""