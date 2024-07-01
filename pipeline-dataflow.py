import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
import os
from apache_beam.metrics.metric import Metrics
import logging
from datetime import datetime

# Obter o timestamp atual
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/log_{timestamp}.txt"

# Configure o logger para salvar os logs em um arquivo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Opcional: para também imprimir no console
    ]
)

class LogAndCountElements(beam.DoFn):
    def __init__(self):
        self.element_counter = Metrics.counter(self.__class__, 'elements')

    def process(self, element):
        logging.info(f"Processando elemento: {element}")
        self.element_counter.inc()
        yield element


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
    volume = elemento['volume'] if elemento['volume'] else 0
    valor = elemento['valor'] if elemento['valor'] else 0
    return f'{codigo};{cod_filial};{deposito};{pedido};{mes};{cod_cliente};{dt_emissao};{ano};{volume};{valor}'


def parse_vendedor(line):
    fields = line.split(";")
    return {
        'vendedor': fields[0],
        'cod_cliente': fields[1],
    }


def parse_cliente(line):
    fields = line.split(";")
    return {
        'nome_cliente': fields[0],
        'cod_cliente': fields[1],
        'uf': fields[2],
        'email': fields[3],
        'fone': fields[4],
    }


def parse_filial(line):
    fields = line.split(";")
    return {
        'cod_filial': fields[0],
        'descricao': fields[1],
        'regiao': fields[2],
    }


def parse_produto(line):
    fields = line.split(";")
    return {
        'descricao': fields[0],
        'cod_produto': fields[1],
        'erp': fields[2],
        'ano': fields[3],
    }


def parse_venda(line):
    fields = line.split(";")
    return {
        'cod_produto': fields[0],
        'cod_filial': fields[1],
        'deposito': fields[2],
        'pedido': fields[3],
        'mes': fields[4],
        'cod_cliente': fields[5],
        'dt_emissao': fields[6],
        'ano': fields[7],
        'volume': float(fields[8]),
        'valor': float(fields[9]),
    }


def parse_grouped(line):
    fields = line.split(";")
    return {
        'cod_cliente': fields[0],
        'vendedor_nome': fields[1],
        'nome_cliente': fields[2],
        'uf_cliente': fields[3],
        'email_cliente': fields[4],
        'fone_cliente': fields[5],
    }


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


def cliente_key(elemento):
    chave = elemento['cod_cliente']
    return (chave, elemento)


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
    vendas_cols = ['cod_produto', 'cod_filial', 'deposito', 'pedido', 'mes', 'cod_cliente', 'dt_emissao', 'ano',
                   'volume', 'valor']
    grouped_cols = ['cod_cliente', 'vendedor_nome', 'nome_cliente', 'uf_cliente', 'email_cliente', 'fone_cliente']

    vendedor_schema = 'vendedor:STRING,cod_cliente:STRING'
    cliente_schema = 'nome_cliente:STRING,cod_cliente:STRING,uf:STRING,email:STRING,fone:STRING'
    filial_schema = 'cod_filial:STRING,descricao:STRING,regiao:STRING'
    produto_schema = 'descricao:STRING,cod_produto:STRING,erp:STRING,ano:STRING'
    venda_schema = ('cod_produto:STRING,cod_filial:STRING,deposito:STRING,pedido:STRING,mes:STRING,'
                    'cod_cliente:STRING,dt_emissao:STRING,ano:DATE,volume:FLOAT,valor:FLOAT')
    grouped_schema = 'cod_cliente:STRING,vendedor_nome:STRING,nome_cliente:STRING,uf_cliente:STRING,email_cliente:STRING,fone_cliente:STRING'

    # Cria o Pipeline
    with beam.Pipeline(options=options) as p:
        vendedor = (
                p
                | 'Readfile Vendedor' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/vendedor.csv',
                                                              skip_header_lines=1)
                | 'Vendedor to List' >> beam.Map(text_to_list)
                | 'Vendedor do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_cols, lista)))
                | 'Transform Vendedor Data' >> beam.Map(transform_vendedor)
        )

        vendedor_to_gcs = (
                vendedor
                | 'Write Vendedor to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/vendedores',
                                                                 file_name_suffix='.csv',
                                                                 header=';'.join(vendedor_cols),
                                                                 num_shards=1,
                                                                 shard_name_template='')
        )

        vendedor_to_bq = (
                vendedor
                | 'Parse Vendedor' >> beam.Map(parse_vendedor)
                | 'Vendedor WriteToBigQuery' >> WriteToBigQuery(
                                                            'intelbras.vendedores',
                                                            schema=vendedor_schema,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                        )
        )

        cliente = (
                p
                | 'Readfile Clientes' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/clientes.csv',
                                                              skip_header_lines=1)
                | 'Clientes to List' >> beam.Map(text_to_list)
                | 'Clientes do Dict' >> beam.Map(lambda lista: dict(zip(clientes_cols, lista)))
                | 'Transform Clientes Data' >> beam.Map(transform_cliente)
        )

        cliente_to_gcs = (
                cliente
                | 'Write Clientes to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/clientes',
                                                                 file_name_suffix='.csv',
                                                                 header=';'.join(clientes_cols),
                                                                 num_shards=1,
                                                                 shard_name_template='')
        )

        cliente_to_bq = (
                cliente
                | 'Parse Clientes' >> beam.Map(parse_cliente)
                | 'Clientes WriteToBigQuery' >> WriteToBigQuery(
                                                            'intelbras.clientes',
                                                            schema=cliente_schema,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                        )
        )

        filial = (
                p
                | 'Read Filial from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/filial.csv',
                                                                 skip_header_lines=1)
                | 'Filial to List' >> beam.Map(text_to_list)
                | 'Filial do Dict' >> beam.Map(lambda lista: dict(zip(filial_cols, lista)))
                | 'Transform Filial Data' >> beam.Map(transform_filial)

        )
        filial_to_gcs = (
                filial
                | 'Write Filial to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/filiais',
                                                               file_name_suffix='.csv',
                                                               header=';'.join(filial_cols),
                                                               num_shards=1,
                                                               shard_name_template='')
        )

        filial_to_bq = (
                filial
                | 'Parse Filiais' >> beam.Map(parse_filial)
                | 'FiliaisWriteToBigQuery' >> WriteToBigQuery(
                                                        'intelbras.filiais',
                                                        schema=filial_schema,
                                                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                    )
        )

        produto = (
                p
                | 'Read Produtos CSV from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/produtos.csv',
                                                                       skip_header_lines=1)
                | 'Produtos to List' >> beam.Map(text_to_list)
                | 'Produtos do Dict' >> beam.Map(lambda lista: dict(zip(produtos_cols, lista)))
                | 'Transform Produtos Data' >> beam.Map(transform_produtos)
        )

        produto_to_gcs = (
                produto
                | 'Write Produtos to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/produtos',
                                                                 file_name_suffix='.csv',
                                                                 header=';'.join(produtos_cols),
                                                                 num_shards=1,
                                                                 shard_name_template='')
        )

        produto_to_bq = (
                produto
                | 'Parse Produtos' >> beam.Map(parse_produto)
                | 'Produtos WriteToBigQuery' >> WriteToBigQuery(
                                                            'intelbras.produtos',
                                                            schema=produto_schema,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                        )
        )

        vendas = (
                p
                | 'Read Vendas CSV from GCS' >> beam.io.ReadFromText('gs://intelbras-bucket/raw/vendas.csv',
                                                                     skip_header_lines=1)
                | 'Vendas to List' >> beam.Map(text_to_list)
                | 'Vendas to Dict' >> beam.Map(lambda lista: dict(zip(vendas_cols, lista)))
                | 'Transform Vendas Data' >> beam.Map(transform_vendas)
        )

        venda_to_gcs = (
                vendas
                | 'Write Vendas to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/trusted/vendas',
                                                               file_name_suffix='.csv',
                                                               header=';'.join(vendas_cols),
                                                               num_shards=1,
                                                               shard_name_template='')
        )

        venda_to_bq = (
                vendas
                | 'Parse Vendas' >> beam.Map(parse_venda)
                | 'Vendas WriteToBigQuery' >> WriteToBigQuery(
                                                            'intelbras.vendas',
                                                            schema=venda_schema,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                        )
        )

        vendedor_group = (
                vendedor
                | 'G-Vendedor to List' >> beam.Map(text_to_list)
                | 'G-Vendedor do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_cols, lista)))
                | 'Cria V Chaves' >> beam.Map(cliente_key)
                | 'Agrupa V Por chave' >> beam.GroupByKey()
        )

        cliente_group = (
                cliente
                | 'G-Clientes to List' >> beam.Map(text_to_list)
                | 'G-Clientes do Dict' >> beam.Map(lambda lista: dict(zip(clientes_cols, lista)))
                | 'Cria C Chaves' >> beam.Map(cliente_key)
                | 'Agrupa C Por chave' >> beam.GroupByKey()
        )
        grouped = (
                {'vendedor': vendedor_group, 'cliente': cliente_group}
                | 'CoGroupByCodCliente' >> beam.CoGroupByKey()
                | 'Gera grouped CSV' >> beam.Map(tupla_para_csv)
        )

        grouped_to_gcs = (
                grouped
                | 'Write Grouped to GCS' >> beam.io.WriteToText('gs://intelbras-bucket/refined/clientes_vendedores_grouped',
                                                        file_name_suffix='.csv',

                                                        header=';'.join(grouped_cols),
                                                        num_shards=1,
                                                        shard_name_template='')
        )

        grouped_to_bq = (
                grouped
                | 'Parse Grouped CSV' >> beam.Map(parse_grouped)
                | 'Log and Count Elements' >> beam.ParDo(LogAndCountElements())
                | 'Grouped WriteToBigQuery' >> WriteToBigQuery(
                                                            'intelbras.clientes_vendedores',
                                                            schema=grouped_schema,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                                                        )
        )


if __name__ == '__main__':
    bucket_name = "intelbras-bucket"
    credentials_path = "credentials.json"
    project_id = "logical-voyage-427513-a6"
    region = "us-central1"
    job_name = 'df-pipeline-complete'
    #logging.getLogger().setLevel(logging.INFO)
    run(credentials_path, bucket_name, project_id, region, job_name)
