import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
import os


def text_to_list(elemento, delimitador=';'):
    return elemento.split(delimitador)


def transform_vendedor(elemento):
    vendedor = elemento['vendedor'].lower() if elemento['vendedor'] else ''
    cod_cliente = elemento['cod_cliente'] if elemento['cod_cliente'] else ''
    return f'{vendedor};{cod_cliente}'


def transform_cliente(elemento):
    cliente = elemento['nome_cliente'].lstrip('- ') if elemento['nome_cliente'] else 'n/a'
    codigo = elemento['cod_cliente'] if elemento['cod_cliente'] else ''
    uf = elemento['uf'] if elemento['uf'] else 'n/a'
    email = elemento['email'].lower() if elemento['email'] else 'n/a'
    fone = elemento['fone'] if elemento['fone'] else 'n/a'
    return f'{cliente};{codigo};{uf};{email};{fone}'


def transform_filial(elemento):
    codigo = elemento['cod_filial'] if elemento['cod_filial'] else ''
    descricao = elemento['descricao'] if elemento['descricao'] else ''
    regiao = elemento['regiao'] if elemento['regiao'] else ''

    # Altera para o valor 'INTERNACIONAL' se a descrição começar com '-'
    if regiao.startswith('-'):
        regiao = 'INTERNACIONAL'
    return f'{codigo};{descricao};{regiao}'


def transform_produtos(elemento):
    descricao = elemento['descricao'] if elemento['descricao'] else ''
    codigo = elemento['cod_produto'] if elemento['cod_produto'] else ''
    erp = elemento['erp'] if elemento['erp'] else ''
    ano = elemento['ano'] if elemento['ano'] else ''
    return f'{descricao};{codigo};{erp};{ano}'


def transform_vendas(elemento):
    codigo = elemento['cod_produto'] if elemento['cod_produto'] else ''
    cod_filial = elemento['cod_filial'] if elemento['cod_filial'] else ''
    deposito = elemento['deposito'] if elemento['deposito'] else ''
    pedido = elemento['pedido'] if elemento['pedido'] else ''
    mes = elemento['mes'] if elemento['mes'] else ''
    cod_cliente = elemento['cod_cliente'] if elemento['cod_cliente'] else ''
    dt_emissao = elemento['dt_emissao'] if elemento['dt_emissao'] else ''
    ano = elemento['ano'] if elemento['ano'] else ''
    volume = elemento['volume'] if elemento['volume'] else ''
    valor = elemento['valor'] if elemento['valor'] else ''
    return f'{codigo};{cod_filial};{deposito};{pedido};{mes};{cod_cliente};{dt_emissao};{ano};{volume};{valor}'


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
    filial_cols = ['cod_filial', 'descricao', 'regiao']
    produtos_cols = ['descricao', 'cod_produto', 'erp', 'ano']
    vendas_cols = ['cod_produto','cod_filial','deposito','pedido','mes','cod_cliente','dt_emissao','ano','volume','valor']

    # Cria o Pipeline
    with beam.Pipeline(options=options) as p:
        vendedor = (
                p
                | 'Readfile Vendedor' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/vendedor.csv',
                                                              skip_header_lines=1)
                | 'Vendedor to List' >> beam.Map(text_to_list)
                | 'Vendedor do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_cols, lista)))
                | 'Transform Vendedor Data' >> beam.Map(transform_vendedor)
                | 'Write Vendedor CSV to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/vendedores',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(vendedor_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        cliente = (
                p
                | 'Readfile Clientes' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/clientes.csv',
                                                              skip_header_lines=1)
                | 'Clientes to List' >> beam.Map(text_to_list)
                | 'Clientes do Dict' >> beam.Map(lambda lista: dict(zip(clientes_cols, lista)))
                | 'Transform Clientes Data' >> beam.Map(transform_cliente)
                | 'Write Clientes CSV to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/clientes',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(clientes_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        filial = (
                p
                | 'Read Filial CSV from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/filial.csv',
                                                                     skip_header_lines=1)
                | 'Filial to List' >> beam.Map(text_to_list)
                | 'Filial do Dict' >> beam.Map(lambda lista: dict(zip(filial_cols, lista)))
                | 'Transform Filial Data' >> beam.Map(transform_filial)
                | 'Write Filial CSV to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/filiais',
                                                                   file_name_suffix='.csv',
                                                                   header=';'.join(filial_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        produtos = (
                p
                | 'Read Produtos CSV from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/produtos.csv',
                                                                       skip_header_lines=1)
                | 'Produtos to List' >> beam.Map(text_to_list)
                | 'Produtos do Dict' >> beam.Map(lambda lista: dict(zip(produtos_cols, lista)))
                | 'Transform Produtos Data' >> beam.Map(transform_produtos)
                | 'Write Produtos CSV to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/produtos',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(produtos_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        vendas = (
                p
                | 'Read Vendas CSV from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/vendas.csv',
                                                                     skip_header_lines=1)
                | 'Vendas to List' >> beam.Map(text_to_list)
                | 'Vendas to Dict' >> beam.Map(lambda lista: dict(zip(vendas_cols, lista)))
                | 'Transform Vendas Data' >> beam.Map(transform_vendas)
                | 'Write Vendas CSV to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/vendas',
                                                                   file_name_suffix='.csv',
                                                                   header=';'.join(vendas_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )


if __name__ == '__main__':
    bucket_name = "intelbras-bucket"
    credentials_path = "credentials.json"
    project_id = "logical-voyage-427513-a6"
    region = "us-east1"
    job_name = 'df-pipeline-trusted'
    run(credentials_path, bucket_name, project_id, region, job_name)
