import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_aeroportos_cancelamento_view

# Qual o percentual de vôos cancelados para os 10 aeroportos com maior taxa de cancelamento

spark = create_spark_session()

aeroportos_cancelamento_view = create_aeroportos_cancelamento_view(spark)

top_ten = spark.sql(
    f"""
        SELECT aeroporto, percCancelamento 
        FROM {aeroportos_cancelamento_view}
        ORDER BY
            percCancelamento DESC
        LIMIT 10;
    """
)

top_ten.show()

top_ten.rdd.saveAsTextFile("queries/page03/query01/output")

spark.stop()