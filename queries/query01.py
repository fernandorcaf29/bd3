import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dto.readTable import readTable

# Quais os principais trechos (pares <origem, destino>) e seu percentual de frequência - Vôos realizados

df = readTable(r'"fVoo"')

df.show()