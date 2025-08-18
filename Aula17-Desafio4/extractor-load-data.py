import os
import zipfile
import sqlite3
import pandas as pd
import re
import unicodedata
from dotenv import load_dotenv

ARQUIVO_ZIP = "resources/Desafio 4 - Dados.zip"
DIR_EXTRACAO = "resources/temp"
BANCO_DADOS = "desafio4.db"
FILE_LIST_HEADER2 = ["Base dias uteis.xlsx"]
BLACK_LIST = ["VR MENSAL 05.2025.xlsx"]

def clean_temp(pasta):
  for filename in os.listdir(pasta):
    filepath = os.path.join(pasta, filename)
    try:
      if os.path.isfile(filepath):
        os.remove(filepath)
    except Exception as e:
      print(f"Erro ao apagar {filepath}: {e}")

def extract_file(file):
    try:
        if os.path.exists(DIR_EXTRACAO):
            clean_temp(DIR_EXTRACAO)
        else:
            os.makedirs(DIR_EXTRACAO)

        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(DIR_EXTRACAO)

        filelist = []
        for arquivo in os.listdir(DIR_EXTRACAO):
            if arquivo.endswith('.csv'):
                filelist.append(os.path.join(DIR_EXTRACAO,arquivo))
        return filelist
    except Exception as ex:
        print(f'@@@ Error ao extrair arquivo ZIP.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')

def sanitize_name(nome):
    nome = str(nome).replace(' ', '_')
    nome = ''.join(c for c in unicodedata.normalize('NFD', nome) if unicodedata.category(c) != 'Mn')
    nome = re.sub(r'[^a-zA-Z0-9_]', '', nome)
    return nome.lower()


def create_table(db_conn, arquivo_xlsx, nome_tabela):
    for fileblack in BLACK_LIST:
        if str(arquivo_xlsx).endswith(fileblack):
            return
    hd2 = False
    for filehd2 in FILE_LIST_HEADER2:
        if str(arquivo_xlsx).endswith(filehd2):
            hd2 = True
    if hd2:
        df = pd.read_excel(arquivo_xlsx, skiprows=1, header=0)
    else:
        df = pd.read_excel(arquivo_xlsx)

    for coluna in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[coluna]):
            df[coluna] = df[coluna].astype(str)

    colunas = df.columns.tolist()

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({', '.join([f'"{sanitize_name(col)}" TEXT' for col in colunas])})"
    db_conn.execute(create_table_sql)
    db_conn.execute(f"DELETE FROM {nome_tabela};")

    insert_sql = f"INSERT INTO {nome_tabela} VALUES ({', '.join(['?' for _ in colunas])})"
    for _, row in df.iterrows():
        db_conn.execute(insert_sql, tuple(row))

    db_conn.commit()
    return create_table_sql

def main():
    extract_file(ARQUIVO_ZIP)
    db_nome = os.environ.get('db_name')

    conn = sqlite3.connect(db_nome)

    lista_tabelas = []

    for arquivo in os.listdir(DIR_EXTRACAO):
        if arquivo.endswith('.xlsx'):
            caminho_arquivo = os.path.join(DIR_EXTRACAO, arquivo)
            nome_arquivo_sem_ext = os.path.splitext(arquivo)[0]
            nome_tabela = sanitize_name(nome_arquivo_sem_ext)

            if nome_tabela:
                print(f"Processando {arquivo} -> Tabela: {nome_tabela}")
                script_tabela = create_table(conn, caminho_arquivo, nome_tabela)
                lista_tabelas.append(script_tabela)
            else:
                print(f"Nome inválido para {arquivo}, pulando.")

    conn.close()
    print("Processamento concluído.")
    print(lista_tabelas)

if __name__ == "__main__":
    load_dotenv()
    main()