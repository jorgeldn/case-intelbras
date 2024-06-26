import apache_beam as beam


def text_to_list(elemento, delimitador=';'):
    return elemento.split(delimitador)


def list_to_dict(lista, colunas=None):
    return dict(zip(colunas, lista))


def tuple_to_csv(elemento, delimitador=';'):
    return ';'.join(elemento)


def vendedor_key(elemento):
    chave = elemento['vendedor']
    return (chave, elemento)


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


def run():
    vendedor_cols = ['vendedor', 'cod_cliente']
    clientes_cols = ['nome_cliente', 'cod_cliente', 'uf', 'email', 'fone']
    filial_cols = ['cod_filial', 'descricao', 'regiao']
    produtos_cols = ['descricao', 'cod_produto', 'erp', 'ano']
    vendas_cols = ['cod_produto', 'cod_filial', 'deposito', 'pedido', 'mes', 'cod_cliente', 'dt_emissao', 'ano',
                   'volume', 'valor']

    # Crie um pipeline usando o DirectRunner
    with beam.Pipeline(runner='DirectRunner') as p:
        """"
        rows = (
                p
                | 'ReadFile' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
                | 'Text to list' >> beam.Map(text_to_list)
                | 'List do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_colunas, lista)))
                | 'Limpa dados Vendedores' >> beam.Map(transform_vendedor)
                #| 'CSV Vendedor' >> beam.Map(tuple_to_csv)
                | 'Write Vendedor CSV' >> beam.io.WriteToText('files_output/vendedor_processed',file_name_suffix='.csv',header=';'.join(vendedor_colunas), num_shards=1, shard_name_template='')
                #| 'Cria Chaves' >> beam.Map(vendedor_key)
                #| 'Agrupa Por chave' >> beam.GroupByKey()
                | 'PrintElements' >> beam.Map(print)
        )"""
        vendedor = (
                p
                | 'Readfile Vendedor' >> beam.io.ReadFromText('files/vendedor.csv',
                                                              skip_header_lines=1)
                | 'Vendedor to List' >> beam.Map(text_to_list)
                | 'Vendedor do Dict' >> beam.Map(lambda lista: dict(zip(vendedor_cols, lista)))
                | 'Transform Vendedor Data' >> beam.Map(transform_vendedor)
                | 'Write Vendedor CSV to GCS' >> beam.io.WriteToText('files_output/vendedores',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(vendedor_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        cliente = (
                p
                | 'Readfile Clientes' >> beam.io.ReadFromText('files/clientes.csv',
                                                              skip_header_lines=1)
                | 'Clientes to List' >> beam.Map(text_to_list)
                | 'Clientes do Dict' >> beam.Map(lambda lista: dict(zip(clientes_cols, lista)))
                | 'Transform Clientes Data' >> beam.Map(transform_cliente)
                | 'Write Clientes CSV to GCS' >> beam.io.WriteToText('files_output/clientes',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(clientes_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        filial = (
                p
                | 'Read Filial CSV from GCS' >> beam.io.ReadFromText('files/filial.csv',
                                                                     skip_header_lines=1)
                | 'Filial to List' >> beam.Map(text_to_list)
                | 'Filial do Dict' >> beam.Map(lambda lista: dict(zip(filial_cols, lista)))
                | 'Transform Filial Data' >> beam.Map(transform_filial)
                | 'Write Filial CSV to GCS' >> beam.io.WriteToText('files_output/filiais',
                                                                   file_name_suffix='.csv',
                                                                   header=';'.join(filial_cols),
                                                                   num_shards=1,
                                                                   shard_name_template='')
        )

        produtos = (
                p
                | 'Read Produtos CSV from GCS' >> beam.io.ReadFromText('files/produtos.csv',
                                                                       skip_header_lines=1)
                | 'Produtos to List' >> beam.Map(text_to_list)
                | 'Produtos do Dict' >> beam.Map(lambda lista: dict(zip(produtos_cols, lista)))
                | 'Transform Produtos Data' >> beam.Map(transform_produtos)
                | 'Write Produtos CSV to GCS' >> beam.io.WriteToText('files_output/produtos',
                                                                     file_name_suffix='.csv',
                                                                     header=';'.join(produtos_cols),
                                                                     num_shards=1,
                                                                     shard_name_template='')
        )

        vendas = (
                p
                | 'Read Vendas CSV from GCS' >> beam.io.ReadFromText('files/vendas.csv',
                                                                     skip_header_lines=1)
                | 'Vendas to List' >> beam.Map(text_to_list)
                | 'Vendas to Dict' >> beam.Map(lambda lista: dict(zip(vendas_cols, lista)))
                | 'Transform Vendas Data' >> beam.Map(transform_vendas)
                | 'Write Vendas CSV to GCS' >> beam.io.WriteToText('files_output/vendas',
                                                                   file_name_suffix='.csv',
                                                                   header=';'.join(vendas_cols),
                                                                   num_shards=1,
                                                                   shard_name_template='')
        )


if __name__ == '__main__':
    run()
