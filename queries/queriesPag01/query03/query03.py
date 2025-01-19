import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.sparkConfig import create_spark_session
from utils.views import create_delayed_per_airport

# Qual o percentual de vôos atrasados (com tolerância de 5 minutos) para os 10 aeroportos com maior taxa de atraso

spark = create_spark_session()

delayed_per_airport = create_delayed_per_airport(spark)

delayed_per_airport_ordered_desc = spark.sql(
f"""
    SELECT 
        *
    FROM 
        {delayed_per_airport}
    ORDER BY 
        percAtraso
    DESC
    LIMIT 10;
"""
)

delayed_per_airport_ordered_desc.rdd.saveAsTextFile("queries/queriesPag01/query03/output")

spark.stop()