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
        SELECT 
            a.nome AS nome_aeroporto,
            vf.cidade_origem,
            vf.pais_origem,
            vf.cidade_destino,
            vf.pais_destino,
            vf.qtdCanceladosTotal
        FROM 
            dAeroporto AS a
        JOIN LATERAL (
            SELECT
                aOrig.id AS id_origem,
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
            GROUP BY 
                aOrig.id, aOrig.Cidade, aOrig.Pais, aDest.Cidade, aDest.Pais
        ) AS vf ON a.id = vf.id_origem
        ORDER BY
            vf.qtdCanceladosTotal DESC
        LIMIT 10
    """
)

output_dir = "queries/page02/query04/output"
top_ten_segments.write.mode('overwrite').csv(output_dir, header=True)

spark.stop()
