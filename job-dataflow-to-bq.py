import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
import os


def parse_csv(line):
    fields = line.split(";")
    return {
        'cod_cliente': fields[0],
        'vendedor_nome': fields[1],
        'nome_cliente': fields[2],
        'uf_cliente': fields[3],
        'email_cliente': fields[4],
        'fone_cliente': fields[5],
    }


def run(credentials_path, bucket_name, project_id, region, job_name, argv=None):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    try:
        # Testa se as credenciais estão configuradas corretamente
        default()
    except DefaultCredentialsError:
        print("As credenciais não estão configuradas corretamente.")
        return

    # Configurações do pipeline
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.job_name = job_name
    google_cloud_options.staging_location = f'gs://{bucket_name}/raw/staging'
    google_cloud_options.temp_location = f'gs://{bucket_name}/raw/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    # options.view_as(SetupOptions).save_main_session = True

    input_file = 'gs://intelbras-bucket/refined/clientes_vendedores_grouped.csv'
    output_table = 'intelbras.clientes_vendedores'

    # Defina o esquema do BigQuery
    schema = 'cod_cliente:STRING,vendedor_nome:STRING,nome_cliente:STRING,uf_cliente:STRING,email_cliente:STRING,fone_cliente:STRING'

    with beam.Pipeline(options=options) as p:
        (
                p
                | 'Readfile Vendedor' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
                | 'ParseCSV' >> beam.Map(parse_csv)
                | 'WriteToBigQuery' >> WriteToBigQuery(
                                                        output_table,
                                                        schema=schema,
                                                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                    )
        )


if __name__ == '__main__':
    bucket_name = "intelbras-bucket"
    credentials_path = "credentials.json"
    project_id = "logical-voyage-427513-a6"
    region = "us-east1"
    job_name = 'df-pipeline-bigquery'
    run(credentials_path, bucket_name, project_id, region, job_name)
