import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_principais_justificativas_maiores_taxas_view

# Quais as 10 principais justificativas para vôos cancelados para os 10 aeroportos com maior taxa de cancelamento

spark = create_spark_session()

principais_justificativas_maiores_taxas_view = create_principais_justificativas_maiores_taxas_view(spark)

top_ten = spark.sql(
    f"""
        SELECT * 
        FROM {principais_justificativas_maiores_taxas_view}
    """
)

top_ten.show()

top_ten.rdd.saveAsTextFile("queries/page03/query04/output")

spark.stop()