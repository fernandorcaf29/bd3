import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dto.readTable import readTable

# Quais os principais trechos (pares <origem, destino>) e seu percentual de frequência - Vôos realizados

dfVoo = readTable(r'"fVoo"')

trechos = dfVoo.rdd.map(lambda row: ((row['idAeroOrig'], row['idAeroDest'] ), 1)).reduceByKey(lambda x, y: x + y)

trechos.saveAsTextFile("queries/query01/output")