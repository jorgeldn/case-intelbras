import os
from google.cloud import storage


def delete_files_in_bucket(bucket_name, credentials_path):
    # Configurar credenciais
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # Inicializar cliente do GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Listar e deletar todos os blobs (arquivos) no bucket
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()
        print(f"Arquivo {blob.name} deletado.")


def delete_bucket(bucket_name, credentials_path):
    # Configurar credenciais
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # Inicializar cliente do GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Deletar o bucket
    bucket.delete()
    print(f"Bucket {bucket_name} deletado.")


if __name__ == "__main__":
    # Definir variáveis
    bucket_name = "intelbras-bucket"
    credentials_path = "credentials.json"

    # Deletar todos os arquivos no bucket
    delete_files_in_bucket(bucket_name, credentials_path)

    # Deletar o próprio bucket
    delete_bucket(bucket_name, credentials_path)
