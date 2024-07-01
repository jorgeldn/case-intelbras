from google.cloud import bigquery
import pandas as pd
from tabulate import tabulate
import os

# Defina o caminho para o arquivo de credenciais
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../credentials.json'

# Crie um cliente BigQuery
client = bigquery.Client()


# Defina a consulta SQL que você deseja executar
query1 = """
    SELECT
        *
    FROM
        `intelbras.vendas`
    LIMIT 10
"""

query2 = """
    SELECT
        EXTRACT(MONTH FROM ano) AS MES,
        SUM(valor) AS VALOR_TOTAL_VENDIDO
    FROM
        `intelbras.vendas`
    GROUP BY
        MES
    ORDER BY
        MES
"""

df = client.query(query2).to_dataframe()
print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))