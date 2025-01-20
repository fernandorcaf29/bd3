import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from pyspark.sql import SparkSession
from bd3.config.sparkConfig import create_spark_session
from bd3.dto.readTable import transform_tables_into_view

# Crie a SparkSession
spark = create_spark_session()

transform_tables_into_view(spark, ["dAeroporto", "fVoo"])

# query = {
#     "name": "Identifica os 10 aeroportos com a maior taxa de cancelamento de voos e calcula o percentual de voos cancelados.",
#         "query": """
        
#         SELECT *
#         FROM dAeroporto
#         """,
#         "path": "queries/outputteste"
# }
def aeroportos_cancelamento ():

    view_name = "aeroportos_cancelamento"

    spark.sql (
        f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
                SELECT 
                    a.nome AS aeroporto,
                    ROUND(SUM(v.qtdCancelados) * 100.0 / SUM(v.qtdVoos), 2) AS percCancelamento
                FROM
                    fVoo AS v
                INNER JOIN
                    dAeroporto AS a ON v.idAeroOrig = a.id
                GROUP BY
                    a.nome
                ORDER BY
                    percCancelamento DESC
                LIMIT 10;
        """
    )
    return view_name

# print(f"Executando query: {query['name']}")

# try:
#     # Executando a query
#     result = spark.sql(query["query"])
    
#     # Salvando o resultado no caminho especificado
#     result.write.mode('overwrite').csv(query["path"], header=True)
#     print(f"Resultado salvo em: {query['path']}")
# except Exception as e:
#     print(f"Erro ao executar a query {query['name']}: {e}")

# # Fechando a SparkSession
# spark.stop()

top_ten_segments = spark.sql(f"""
    SELECT *
    FROM {aeroportos_cancelamento()}
""")
top_ten_segments.show()
# top_ten_segments.write.mode('overwrite').text("bd3/queries/outputteste")
spark.stop()