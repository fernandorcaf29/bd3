import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dto.readTable import transform_table_into_view
from config.sparkConfig import create_spark_session
from utils.views import create_segments_view
from utils.views import create_ordered_segments_view

# Liste todas as companhias responsáveis por operar os principais trechos da pergunta anterior (ida OU volta)

spark = create_spark_session()

transform_table_into_view(spark, r'"fVoo"', "fVoo")

transform_table_into_view(spark, r'"dAeroporto"', "dAeroporto")

transform_table_into_view(spark, r'"dCompanhia"', "dCompanhia")

ordered_segments_view = create_ordered_segments_view(spark)

top_ten_segments_to_companies = spark.sql(
f"""
    SELECT
        DISTINCT(fv.companhia)
    FROM (    
            SELECT *
            FROM {ordered_segments_view}
            LIMIT 10
        ) AS t
    CROSS JOIN LATERAL (
        SELECT 
            c.nome AS companhia 
        FROM fVoo AS v
        INNER JOIN dAeroporto AS aDest 
            ON v.idAeroDest = aDest.id
        INNER JOIN dAeroporto AS aOrig 
            ON v.idAeroOrig = aOrig.id
        INNER JOIN dCompanhia AS c 
            ON v.idCompanhia = c.id
        WHERE t.cidade_destino = aDest.Cidade
        AND t.pais_destino = aDest.Pais
        AND t.cidade_origem = aOrig.Cidade
        AND t.pais_origem = aOrig.Pais
    ) AS fv ON TRUE;
""")

top_ten_segments_to_companies.rdd.saveAsTextFile("queries/queriesPag01/query02/output")

spark.stop()