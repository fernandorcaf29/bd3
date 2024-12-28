import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.sparkConfig import spark
from config.postgresConfig import url, properties

def readTable(table_name):
    return spark.read.jdbc(url, table_name, properties=properties)
