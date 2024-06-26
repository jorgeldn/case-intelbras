import pandas as pd
import os

import pandas as pd
import os

def convert_to_csv(input_file, output_file):
    """
    Converte um arquivo Excel (.xls ou .xlsx) para CSV usando ';' como separador.

    Args:
    - input_file (str): Caminho para o arquivo de entrada Excel (.xls ou .xlsx).
    - output_file (str): Caminho para o arquivo de saída CSV.

    Returns:
    - True se a conversão for bem-sucedida, False caso contrário.
    """
    try:
        # Verifica se o arquivo de entrada existe
        if not os.path.isfile(input_file):
            print(f"Erro: O arquivo '{input_file}' não existe.")
            return False

        # Carrega o arquivo Excel usando pandas
        if input_file.endswith('.xls'):
            df = pd.read_excel(input_file)
        elif input_file.endswith('.xlsx'):
            df = pd.read_excel(input_file, engine='openpyxl')
        else:
            print(f"Erro: Formato de arquivo não suportado para '{input_file}'.")
            return False

        # Salva o DataFrame como CSV usando ponto e vírgula como separador
        df.to_csv(output_file, sep=';', index=False)

        print(f"Conversão concluída: '{input_file}' convertido para '{output_file}'.")
        return True

    except Exception as e:
        print(f"Erro durante a conversão: {e}")
        return False


if __name__ == "__main__":
    convert_to_csv("files/Clientes.xlsx", "files/clientes.csv")
    convert_to_csv("files/Filial.xlsx", "files/filial.csv")
    convert_to_csv("files/Produtos.xlsx", "files/produtos.csv")
    convert_to_csv("files/Vendas.xls", "files/vendas.csv")
