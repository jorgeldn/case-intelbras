import os
from google.cloud import storage


def create_folder(bucket, folder_name):
    """Cria uma 'pasta' no GCS"""
    blob = bucket.blob(f"{folder_name}/")
    blob.upload_from_string('')


def upload_files_to_gcs(bucket_name, source_folder, raw_folder,
                        trusted_folder, refined_folder, credentials_path):
    # Configurar credenciais
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # Inicializar cliente do GCS e cria o bucket
    client = storage.Client()
    # Cria o bucket
    bucket = client.create_bucket(bucket_name)

    # Cria as pastas (camadas)
    create_folder(bucket, raw_folder)
    create_folder(bucket, trusted_folder)
    create_folder(bucket, refined_folder)

    # Listar todos os arquivos na pasta fonte
    for filename in os.listdir(source_folder):
        file_path = os.path.join(source_folder, filename)

        if os.path.isfile(file_path) and filename.lower().endswith('.csv'):
            # Definir o caminho de destino no GCS
            destination_blob_name = f"{raw_folder}/{filename}"

            # Fazer upload do arquivo
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(file_path)
            print(f"Arquivo {file_path} carregado para {destination_blob_name}.")


if __name__ == "__main__":
    # Definir variáveis
    bucket_name = "intelbras-bucket"
    source_folder = "files"
    raw_folder = "raw"
    trusted_folder = "trusted"
    refined_folder = "refined"
    credentials_path = "credentials.json"

    # Chamar a função para fazer upload dos arquivos
    upload_files_to_gcs(bucket_name, source_folder, raw_folder, trusted_folder, refined_folder, credentials_path)
    print(f"Bucket {bucket_name} criado com sucesso.")
