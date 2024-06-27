import os
from google.cloud import bigquery

# Define o caminho para o arquivo de credenciais
credenciais_path = "credentials.json"

# Define a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credenciais_path

# Configura o cliente
client = bigquery.Client()

# Define o ID do dataset (projeto_id.dataset_id)
dataset_id = f"{client.project}.intelbras"

# Cria um dataset
def criar_dataset():
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-east1"  # Define a localização (opcional)
    dataset = client.create_dataset(dataset, timeout=30)  # Cria o dataset
    print(f"Dataset {dataset.dataset_id} criado.")

# Apaga um dataset
def apagar_dataset():
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    print(f"Dataset {dataset_id} apagado.")

# Executa as funções
if __name__ == "__main__":
    apagar_dataset()
    criar_dataset()  # Cria o dataset


