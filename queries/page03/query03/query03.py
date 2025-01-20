import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from dto.readTable import transform_tables_into_view
from config.sparkConfig import create_spark_session

# Quais as 10 principais justificativas para vôos cancelados

spark = create_spark_session()

transform_tables_into_view(spark, ["fVooJustificativa", "dJustificativa"])

top_ten = spark.sql (
    f"""
        SELECT 
            j.cod AS justificativa,
            COUNT(vj.idJustificativa) AS qtdCancelamentos
        FROM 
            fVooJustificativa AS vj
        INNER JOIN 
            dJustificativa AS j ON vj.idJustificativa = j.id
        GROUP BY 
            j.cod
        ORDER BY 
            qtdCancelamentos DESC
        LIMIT 10;
    """
)

top_ten.show()

top_ten.rdd.saveAsTextFile("queries/page03/query03/output")


spark.stop()