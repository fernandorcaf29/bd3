import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from dto.readTable import transform_tables_into_view
from config.sparkConfig import create_spark_session
from utils.views import create_aeroportos_cancelamento_view

# Quais as 10 principais justificativas para vôos cancelados para os 10 aeroportos com maior taxa de cancelamento

spark = create_spark_session()

transform_tables_into_view(spark, ["fVooJustificativa", "dJustificativa"])

aeroportos_cancelamento_view = create_aeroportos_cancelamento_view(spark)

top_ten = spark.sql (
    f"""
        SELECT 
            j.cod AS justificativa,
            COUNT(vj.idJustificativa) AS qtdCancelamentos
        FROM 
            fVooJustificativa AS vj
        INNER JOIN 
            dJustificativa AS j ON vj.idJustificativa = j.id
        INNER JOIN LATERAL
        (
            SELECT * 
            FROM {aeroportos_cancelamento_view}
            ORDER BY
                percCancelamento DESC
            LIMIT 10
        ) AS ac ON vj.idAero = ac.aeroporto_id 
        GROUP BY 
            j.cod
        ORDER BY 
            qtdCancelamentos DESC
        LIMIT 10;
    """
)

top_ten.show()

top_ten.rdd.saveAsTextFile("queries/page03/query04/output")

spark.stop()