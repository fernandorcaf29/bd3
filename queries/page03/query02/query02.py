import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from config.sparkConfig import create_spark_session
from utils.views import create_aeroportos_cancelamento_view

# Qual o percentual de vôos cancelados para os 10 aeroportos com menor taxa de cancelamento

spark = create_spark_session()

aeroportos_cancelamento_view = create_aeroportos_cancelamento_view(spark)

top_ten = spark.sql(
    f"""
        SELECT * 
        FROM {aeroportos_cancelamento_view}
        ORDER BY
            percCancelamento ASC
        LIMIT 10;
    """
)

top_ten.show()

top_ten.rdd.saveAsTextFile("queries/page03/query02/output")

spark.stop()