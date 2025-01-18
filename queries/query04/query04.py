import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dto.readTable import readTable 
from config.sparkConfig import spark

dfVoo = readTable(r'"fVoo"')

dfVoo.createOrReplaceTempView("fVoo")

dfAllAeroporto = readTable(r'"dAeroporto"')
dfAllAeroporto.createOrReplaceTempView("allAeroporto")

# Lista das queries e seus paths originais
queries = [
    {
        "name": "maior_taxa_cancelamento_por_mes",
        "query": """
            WITH taxa_cancelamento AS (
                SELECT f.mes, a.nome AS nomeAeroporto, 
                       (SUM(f.qtdCancelados) / SUM(f.qtdVoos)) * 100 AS taxa_cancelamento,
                       SUM(f.distTotal - f.distPercorrida) AS dist_nao_viajada,
                       SUM(f.qtdVoos) AS qtdVoos,
                       CASE
                            WHEN SUM(f.distPercorrida) = 0 THEN CAST('Infinity' AS DOUBLE)
                            ELSE (SUM(f.distTotal - f.distPercorrida) / SUM(f.distPercorrida)) * 100
                       END AS proporcao_nao_viajada 
                FROM fVoo f
                LEFT JOIN allAeroporto a ON (a.id = f.idAeroOrig OR a.id = f.idAeroDest)
                GROUP BY f.mes, nomeAeroporto
            )
            SELECT ranked.mes, ranked.nomeAeroporto, ranked.dist_nao_viajada, ranked.proporcao_nao_viajada  
            FROM (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.mes ORDER BY t.taxa_cancelamento DESC, (t.qtdVoos * t.dist_nao_viajada) DESC) AS row_num
                FROM taxa_cancelamento t
            ) AS ranked
            WHERE ranked.row_num = 1
            ORDER BY ranked.mes 
        """,
        "path": "queries/query04/output/maior_taxa_cancelamento_por_mes"
    },
    {
        "name": "maior_taxa_cancelamento_por_mes_ano",
        "query": """
            WITH taxa_cancelamento AS (
                SELECT f.ano, f.mes, a.nome AS nomeAeroporto, 
                       (SUM(f.qtdCancelados) / SUM(f.qtdVoos)) * 100 AS taxa_cancelamento,
                       SUM(f.distTotal - f.distPercorrida) AS dist_nao_viajada,
                       SUM(f.qtdVoos) AS qtdVoos,
                       CASE
                            WHEN SUM(f.distPercorrida) = 0 THEN CAST('Infinity' AS DOUBLE)
                            ELSE (SUM(f.distTotal - f.distPercorrida) / SUM(f.distPercorrida)) * 100
                       END AS proporcao_nao_viajada 
                FROM fVoo f
                LEFT JOIN allAeroporto a ON (a.id = f.idAeroOrig OR a.id = f.idAeroDest)
                GROUP BY f.ano, f.mes, nomeAeroporto
            )
            SELECT ranked.mes, ranked.ano, ranked.nomeAeroporto, ranked.dist_nao_viajada, ranked.proporcao_nao_viajada  
            FROM (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.ano, t.mes ORDER BY t.taxa_cancelamento DESC, (t.qtdVoos * t.dist_nao_viajada) DESC) AS row_num
                FROM taxa_cancelamento t
            ) AS ranked
            WHERE ranked.row_num = 1
            ORDER BY ranked.ano, ranked.mes 
        """,
        "path": "queries/query04/output/maior_taxa_cancelamento_por_mes_ano"
    },
    {
        "name": "menor_taxa_cancelamento_por_mes",
        "query": """
            WITH taxa_cancelamento AS (
                SELECT f.mes, a.nome AS nomeAeroporto, 
                       (SUM(f.qtdCancelados) / SUM(f.qtdVoos)) * 100 AS taxa_cancelamento,
                       SUM(f.distTotal - f.distPercorrida) AS dist_nao_viajada,
                       SUM(f.qtdVoos) AS qtdVoos,
                       SUM(f.distPercorrida) AS dist_viajada,
                       CASE
                            WHEN SUM(f.distPercorrida) = 0 THEN CAST('Infinity' AS DOUBLE)
                            ELSE (SUM(f.distTotal - f.distPercorrida) / SUM(f.distPercorrida)) * 100
                       END AS proporcao_nao_viajada 
                FROM fVoo f
                LEFT JOIN allAeroporto a ON (a.id = f.idAeroOrig OR a.id = f.idAeroDest)
                GROUP BY f.mes, nomeAeroporto
            )
            SELECT ranked.mes, ranked.nomeAeroporto, ranked.dist_nao_viajada, ranked.proporcao_nao_viajada  
            FROM (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.mes ORDER BY t.taxa_cancelamento ASC, (t.qtdVoos * t.dist_viajada) DESC) AS row_num
                FROM taxa_cancelamento t
            ) AS ranked
            WHERE ranked.row_num = 1
            ORDER BY ranked.mes 
        """,
        "path": "queries/query04/output/menor_taxa_cancelamento_por_mes"
    },
    {
        "name": "menor_taxa_cancelamento_por_mes_ano",
        "query": """
            WITH taxa_cancelamento AS (
                SELECT f.ano, f.mes, a.nome AS nomeAeroporto, 
                       (SUM(f.qtdCancelados) / SUM(f.qtdVoos)) * 100 AS taxa_cancelamento,
                       SUM(f.distTotal - f.distPercorrida) AS dist_nao_viajada,
                       SUM(f.qtdVoos) AS qtdVoos,
                       SUM(f.distPercorrida) AS dist_viajada,
                       CASE
                            WHEN SUM(f.distPercorrida) = 0 THEN CAST('Infinity' AS DOUBLE)
                            ELSE (SUM(f.distTotal - f.distPercorrida) / SUM(f.distPercorrida)) * 100
                       END AS proporcao_nao_viajada 
                FROM fVoo f
                LEFT JOIN allAeroporto a ON (a.id = f.idAeroOrig OR a.id = f.idAeroDest)
                GROUP BY f.ano, f.mes, nomeAeroporto
            )
            SELECT ranked.mes, ranked.ano, ranked.nomeAeroporto, ranked.dist_nao_viajada, ranked.proporcao_nao_viajada  
            FROM (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.ano, t.mes ORDER BY t.taxa_cancelamento ASC, (t.qtdVoos * t.dist_viajada) DESC) AS row_num
                FROM taxa_cancelamento t
            ) AS ranked
            WHERE ranked.row_num = 1
            ORDER BY ranked.ano, ranked.mes 
        """,
        "path": "queries/query04/output/menor_taxa_cancelamento_por_mes_ano"
    }
]

# Executar e salvar os resultados das queries
for query in queries:
    print(f"Executando query: {query['name']}")
    result = spark.sql(query["query"]).write.mode('overwrite').csv(f"{query['path']}", header=True)
    print(f"Resultado salvo em: {query['path']}")
