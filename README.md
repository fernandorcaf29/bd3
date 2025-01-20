# Trabalho final de BD3

Este repositório contém todas as entregas relacionadas ao trabalho final de BD3.

## ETL

Para realizar a etapa de ETL foi escolhida a ferramenta KNIME. Para o caso do script em Python é possível implementar um evento hospedado em serviço cloud serverless como Lambdas na AWS ou Functions na GCP para disparar rotinas periódicas de injeção de dados.


### Instruções para executar:
  1. Abrir o arquivo ```BD3.knwf``` encontrado na raíz do projeto
  2. Preencher o nó de conexão do banco de dados com as credenciais e rotas de acesso ao banco de dados local
  3. Pressionar "Execute All"


## Aplicação

Para a aplicação foi utilizado PySpark como cliente para lidar com conexões ao servidor Spark. Cada diretório page0X corresponde a uma página de consultas das especificações do trabalho e cada arquivo query0X corresponde à uma consulta da página de consultas em ordem numérica.

### Instruções para executar:
  1. Retirar a extensão  ```.sample``` do arquivo ```.env.sample``` e preencher com as credenciais e rotas de acesso ao banco de dados local
  2. Rodar os comandos ```pip install python-dotenv``` e ```pip install pyspark```
  3. Rodar o interpretador sobre os arquivos query0X.py localizados nos diretórios ```queries/page0X/query0X/```

## Modelagem

Também é possível entregar o seguinte modelo na raíz do projeto como .png.

![bd3 drawio (2)](https://github.com/user-attachments/assets/9c617bf9-c693-4fbf-b01e-0ee5e128eac9)
