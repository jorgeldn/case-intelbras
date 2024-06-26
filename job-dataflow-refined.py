import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
import os


def text_to_list(elemento, delimitador=';'):
    return elemento.split(delimitador)


def list_to_dict(lista, colunas=None):
    return dict(zip(colunas, lista))


def tuple_to_csv(elemento, delimitador=';'):
    return ';'.join(elemento)


def cliente_key(elemento):
    chave = elemento['cod_cliente']
    return (chave, elemento)


class GroupByCodCliente(beam.DoFn):
    def process(self, element):
        # Extrai a chave 'cod_cliente' e o valor original do elemento
        cod_cliente = element['cod_cliente']
        yield (cod_cliente, element)

def tupla_para_csv(data):
    cod_cliente = data[0]
    vendedor_info = data[1]['vendedor'][0][0]
    cliente_info = data[1]['cliente'][0][0]

    vendedor_nome = vendedor_info['vendedor']
    nome_cliente = cliente_info['nome_cliente']
    uf_cliente = cliente_info['uf']
    email_cliente = cliente_info['email']
    fone_cliente = cliente_info['fone']

    # Organizando valores em uma lista
    csv_line = [
        cod_cliente,
        vendedor_nome,
        nome_cliente,
        uf_cliente,
        email_cliente,
        fone_cliente
    ]

    # Transformando a lista em uma string delimitada por ";"
    return ";".join(csv_line)


def run(credentials_path, bucket_name, project_id, region, job_name, argv=None):
    # Configurar credenciais
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

    vendedor_cols = ['vendedor', 'cod_cliente']
    clientes_cols = ['nome_cliente', 'cod_cliente', 'uf', 'email', 'fone']
    result_cols = ['cod_cliente', 'vendedor_nome', 'nome_cliente', 'uf_cliente', 'email_cliente', 'fone_cliente']

    # Cria o Pipeline
    with beam.Pipeline(options=options) as p:
        vendedor = (
                p
                | 'Readfile Vendedor' >> beam.io.ReadFromText('gs://intelbras-bucket/trusted/vendedores.csv',
                                                              skip_header_lines=1)
                | 'Vendedor to List' >> beam.Map(text_to_list)
                | 'Vendedor do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_cols, lista)))
                | 'Cria V Chaves' >> beam.Map(cliente_key)
                | 'Agrupa V Por chave' >> beam.GroupByKey()
        )

        cliente = (
                p
                | 'Readfile Clientes' >> beam.io.ReadFromText('gs://intelbras-bucket/trusted/clientes.csv',
                                                              skip_header_lines=1)
                | 'Clientes to List' >> beam.Map(text_to_list)
                | 'Clientes do Dict' >> beam.Map(lambda lista: dict(zip(clientes_cols, lista)))
                | 'Cria C Chaves' >> beam.Map(cliente_key)
                | 'Agrupa C Por chave' >> beam.GroupByKey()
        )
        grouped = (
                {'vendedor': vendedor, 'cliente': cliente}
                | 'CoGroupByCodCliente' >> beam.CoGroupByKey()
                | 'Gera grouped CSV' >> beam.Map(tupla_para_csv)
                | 'Write Result' >> beam.io.WriteToText('gs://intelbras-bucket/refined/clientes_vendedores_grouped',
                                                        file_name_suffix='.csv',

                                                        header=';'.join(result_cols),
                                                        num_shards=1,
                                                        shard_name_template='')
        )


if __name__ == '__main__':
    bucket_name = "intelbras-bucket"
    credentials_path = "credentials.json"
    project_id = "logical-voyage-427513-a6"
    region = "us-east1"
    job_name = 'df-pipeline-refined'
    run(credentials_path, bucket_name, project_id, region, job_name)
