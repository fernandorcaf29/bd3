import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from dto.readTable import transform_tables_into_view 
from config.sparkConfig import create_spark_session
spark = create_spark_session()

transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

query = {
    "name": "menor_taxa_cancelamento_por_mes",
        "query": """
WITH taxa_cancelamento AS (
    SELECT 
        f.mes, 
        a.nome AS nomeAeroporto, 
        (SUM(f.qtdCancelados) / NULLIF(SUM(f.qtdVoos), 0)) * 100 AS taxa_cancelamento,
        SUM(f.distTotal - f.distPercorrida) AS dist_nao_viajada,
        CASE
            WHEN SUM(f.distPercorrida) = 0 THEN CAST('Infinity' AS DOUBLE)
            ELSE (SUM(f.distTotal - f.distPercorrida) / SUM(f.distPercorrida)) * 100
        END AS proporcao_nao_viajada,
        SUM(f.qtdVoos) AS qtdVoos,
        SUM(f.distPercorrida) AS dist_viajada
    FROM fVoo f
    LEFT JOIN LATERAL (
        SELECT nome 
        FROM dAeroporto a 
        WHERE a.id = f.idAeroOrig OR a.id = f.idAeroDest
    ) a
    GROUP BY f.mes, a.nome
)
SELECT ranked.mes, ranked.nomeAeroporto, ranked.dist_nao_viajada, ranked.proporcao_nao_viajada, ranked.dist_viajada
FROM (
    SELECT t.*, 
           ROW_NUMBER() OVER (
               PARTITION BY t.mes 
               ORDER BY t.taxa_cancelamento ASC, (t.qtdVoos * t.dist_viajada) DESC
           ) AS row_num
    FROM taxa_cancelamento t
) AS ranked
WHERE ranked.row_num = 1
ORDER BY ranked.mes;
        """,
        "path": "queries/page04/query16/output"
}

print(f"Executando query: {query['name']}")
try:
    # Executando a query
    result = spark.sql(query["query"])
    
    # Salvando o resultado no caminho especificado
    result.write.mode('overwrite').csv(query["path"], header=True)
    print(f"Resultado salvo em: {query['path']}")
except Exception as e:
    print(f"Erro ao executar a query {query['name']}: {e}")

# Fechando a SparkSession
spark.stop()