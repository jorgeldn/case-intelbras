from google.cloud import bigquery
import os

# Defina o caminho para o arquivo de credenciais
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../credentials.json'

# Crie um cliente BigQuery
client = bigquery.Client()

# Defina o ID do projeto e o ID da tabela
dataset_id = 'intelbras'
table_id = 'vendas'

# Referência à tabela
table_ref = client.dataset(dataset_id).table(table_id)

# Obtenha a tabela
table = client.get_table(table_ref)

# Imprima o schema da tabela
print("Table Schema:")
for field in table.schema:
    print(f"Column: {field.name}, Type: {field.field_type}, Mode: {field.mode}")
