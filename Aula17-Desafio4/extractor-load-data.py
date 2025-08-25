import os
import zipfile
import sqlite3
from idlelib.iomenu import encoding

import pandas as pd
import re
import unicodedata
import datetime
from dotenv import load_dotenv
from dataclasses import dataclass, field

from pandas.core.interchange.dataframe_protocol import DataFrame
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext, format_as_xml
from typing import List, Dict
from dotenv import load_dotenv
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider

ARQUIVO_ZIP = "resources/Desafio 4 - Dados.zip"
DIR_EXTRACAO = "resources/temp"
BANCO_DADOS = "desafio4.db"
FILE_LIST_HEADER2 = ["Base dias uteis.xlsx"]
BLACK_LIST = ["VR MENSAL 05.2025.xlsx"]

DB_SCHEMA = ""


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
    nome = nome.strip()
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


@dataclass
class SQLQuery:
    query:str

@dataclass
class XLSFileList:
    xlslist: List[str] = field(default_factory=list)

class XLSFileListOutput(BaseModel):
    output_message: str
    filelist: List[str]

class TableListOutput(BaseModel):
    output_message: str
    tablelist: List[str]

def agente_extracao(zip_file: str) -> XLSFileListOutput:
    model = GoogleModel(
        'gemini-2.0-flash', provider=GoogleProvider(api_key=os.environ.get('gem_key'))
    )

    agente = Agent(
        model,
        deps_type=str,
        retries=1,
        output_type=XLSFileListOutput,
        system_prompt=("Você é um analista de suporte que receberá o endereço de um arquivo do tipo .ZIP "
                       "e utilizará a função 'tool_extrair_zip' informando como parâmetro o nome do arquivo a ser descompactado "
                       "e retornará como resultado a lista de arquivos descompactados."
       )
    )

    @agente.tool
    def tool_extrair_zip(ctx: RunContext[str], filename:str) -> List[str]:
        try:
            print(f'   tool_extrair_zip: {filename}')
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(DIR_EXTRACAO)

            # Encontra o primeiro arquivo CSV extraído
            filelist = []
            for arquivo in os.listdir(DIR_EXTRACAO):
                if arquivo.endswith('.xlsx'):
                    filelist.append(f'{DIR_EXTRACAO}/{arquivo}')
            return filelist
        except Exception as ex:
            print(f'@@@ Error ao extrair arquivo ZIP.\n'
                  f'   @@@ {type(ex)}\n'
                  f'   @@@{ex=}')

    retorno_ext = agente.run_sync(zip_file, deps=zip_file)
    return retorno_ext.output

def agente_load(xlslist: List[str]) -> List[str]:
    model = GoogleModel(
        'gemini-2.0-flash', provider=GoogleProvider(api_key=os.environ.get('gem_key'))
    )

    agente = Agent(
        model,
        retries=1,
        deps_type=XLSFileList,
        output_type=TableListOutput,
        system_prompt=(
            """### PAPEL: Você é um analista de suporte especializado em receber uma lista de planilhas, avaliar seu conteúdo e criar as tabelas em SQLite com a devida carga dos dados recebidos nas mesmas. 
               ### OPBJETIVO: Utilizando apenas a lista de arquivos recebida, você deverá iterar por cada planilha XLS, pegar seu conteúdo através da tool 'get_file' e seguir as seguintes etapas: 
                - Criar o script DDL em SQLite3 para criação da tabela seguindo os critérios listados abaixo e executar a criação utilizando a tool 'tool_executar_comando_sql'.
                - Por cada linha apresentada na planilna, gerar o script de INSERT e executá-lo utilizando a tool 'tool_executar_comando_sql'.
                - Ao executar todas as planilhas, retorne uma List[Str] contendo o script DDL de criação de todas as tabelas criadas.
               ### CRITÉRIOS PARA NOMENCLATURA DAS TABELAS E COLUNAS:
                - O nome da tabela deverá ser o nome do arquivo seguindo os demais critérios aqui estipulados.
                - O nome de cada coluna deverá ser o nome apresentado na primeira linha seguindo os demais critérios aqui estipulados.
                - O nome da tabela e colunas não deverá conter caracteres especiais, para isso, substitua-os pelo caracter referente. Exemplo: 'Ç' => 'c'; 'â' => 'a'; 'Õ' => 'o'; 'É' => 'e'.
                - Removas os espaços exedentes (inclusive no início ou fim do nome).
                - Utilizar apenas caracteres minúsculos (lower).  
                - Substitua os espaços entre as palavras por '_'.
                - Para o arquivo 'Base dias uteis.xlsx', ignorar a primeira linha, obtendo o nome das colunas conforme o apresentado na segunda linha.
                - Ignorar o arquivo 'VR MENSAL 05.2025.xlsx'. 
               ### PONTOS DE ATENÇÃO
                - Ao acionar a função 'tool_executar_comando_sql', envie apenas a query SQL a ser executada, sem nenhum texto ou caractere adicional.
                - Poderá utilizar a tool 'tool_executar_consulta' para efetuar consultas e avaliar seus conteúdos.
            """
                       )
    )

    @agente.tool
    def get_file(ctx: RunContext[str], filename:str) -> str:
        for fileblack in BLACK_LIST:
            if str(filename).endswith(fileblack):
                return
        hd2 = False
        for filehd2 in FILE_LIST_HEADER2:
            if str(filename).endswith(filehd2):
                hd2 = True
        if hd2:
            df = pd.read_excel(filename, skiprows=1, header=0)
        else:
            df = pd.read_excel(filename)

        return df.to_csv()

    @agente.tool
    def tool_executar_comando_sql(ctx: RunContext[str], query: str):
        try:
            print(f'tool_executar_comando_sql - SQL gerado: \n{query}\n\n')
            conn = sqlite3.connect(BANCO_DADOS)
            cursor = conn.execute(query)
            conn.commit()
            # resultados = cursor.fetchall()
            # colunas = [desc[0] for desc in cursor.description]
            # return [dict(zip(colunas, linha)) for linha in resultados]
        except Exception as ex:
            print(f'@@@ Error ao executar consulta.\n'
                  f'   @@@ {type(ex)}\n'
                  f'   @@@{ex=}')
        finally:
            conn.close()

    @agente.tool
    def tool_executar_consulta(ctx: RunContext[str], query: str) -> List[Dict]:
        try:
            print(f'tool_executar_consulta - SQL gerado: \n{query}\n\n')
            conn = sqlite3.connect(BANCO_DADOS)
            cursor = conn.execute(query)
            resultados = cursor.fetchall()
            colunas = [desc[0] for desc in cursor.description]
            return [dict(zip(colunas, linha)) for linha in resultados]
        except Exception as ex:
            print(f'@@@ Error ao executar consulta.\n'
                  f'   @@@ {type(ex)}\n'
                  f'   @@@{ex=}')
        finally:
            conn.close()

    @agente.instructions
    async def get_file_list(ctx: RunContext[XLSFileList]) -> str:
        return str(ctx.deps.xlslist)

    deps = XLSFileList(xlslist=xlslist)
    retorno_load = agente.run_sync('Processar esta lista de arquivos.', deps=deps)
    print(retorno_load.output)

def agente_consulta_query() -> Agent:
    model = GoogleModel(
        'gemini-2.0-flash', provider=GoogleProvider(api_key=os.environ.get('gem_key'))
    )

    agente = Agent(
        model,
        retries=1,
        deps_type=str,
        system_prompt=f"""### PAPEL: Especialista em SQLite3.
            ### AÇÃO: Considerando as tabelas enviadas, crie uma query para consultar a lista de colaboradores ativos para pagamento de vale refeição conforme as regras detalhadas e execute a consulta através da tool 'executar_consulta' enviando como parâmetro apenas a query SQL a ser executada e retorne como resultado o endereço do arquivo CSV gerado.
            
            ### RESPONSABILIDADE DE CADA TABELA:
            - ativos: Tabela com a lista de colaboradores ativos com matrícula, sindicato e situação.
            - admissao_abril: Tabela com a lista de admitidos em abril de 2025 com matrícula e data de admissão.
            - afastamentos: Tabela com a lista de colaboradores afastados com matrícula e tipo de afastamento.
            - desligados: Tabela com a lista de colaboradores desligados com matrícula, data de demissão e status de comunicado.
            - ferias: Tabela com a lista de colaboradores em férias com matrícula e dias de férias.
            - exterior: Tabela com a lista de colaboradores que atuam no exterior.
            - base_dias_uteis: Tabela de dias úteis por sindicato.
            - base_sindicato_x_valor: Tabela de valores diários de VR por sindicato.
            
            ### REGRAS DETALHADAS:
            - Retorne todos os colaboradores ativos, apresentando os campos:
              -- matricula
              -- empresa
              -- titulo_do_cargo
              -- sindicato
              -- valor_diario_sindicato: valor apresentado conforme o estado no qual o sindicato seja referente. Relação de de-para para identificação do estado: quando o sindicato começar com "SINDPD SP" o estado será "São Paulo"; quando o sindicato começar com "SINDPPD RS" o estado será "Rio Grande do Sul"; quando o sindicato começar com "SITEPD PR" o estado será "Paraná"; quando o sindicato começar com "SINDPD RJ" o estado será "Rio de Janeiro";
              -- dias_uteis: dias uteis conforme a base estipulada pelo sindicato.
              -- qtde_dias_ferias: sub-query que, caso o colaborador esteja de férias com retorno anterior a 15/05, calcular a diferença em dias da data de retorno das férias até o dia 15/05).
              -- qtde_dias_licenca: sub-query que, caso o colaborador esteja de licença com retorno anterior a 15/05, calcular a diferença em dias da data de retorno da licença até o dia 15/05.
            - Utilize a coluna [matricula] para efetuar o relacionamento entre as tabelas.
            - Desconsiderar os colaboradores que estão de férias.
            - Desconsiderar os colaboradores que não apresentam sindicato.
            - Desconsiderar os colaboradores que estão afastados (utilizar o campo unnamed_3 para avaliar os retornos de licença, caso o colaborador retorne de licença antes de 15/05 subtraia os dias em que estava de licença do dias_uteis).
            - Desconsiderar os colaboradores que estão no exterior.
            - Desconsiderar os colaboradores que são estagiários, aprendizes, diretores ou não tenha um título informado.
            - Desconsiderar os colaboradores que foram desligados cuja data_demissao maior ou igual a 15/05 e comunicado_de_desligamento Ok.
            - Identifique o [dias_uteis] através sindicado da tabela base_dias_uteis.
            
            ### INFORMAÇÕES IMPORTANTES
            - Ao acionar a tool 'executar_consulta', envie apenas a query SQL a ser executada, sem nenhum texto ou caractér adicional.
            - A tool 'executar_consulta' retorna o endereço do arquivo CSV gerado.
            - Utilize alias para nomear a coluna retornada conforme o contexto dela.
            - Para procurar conteúdo de texto utilizar sempre a função UPPER para garantir que não tenha problemas decorrentes de consistência de dados.
            """
    )

    @agente.tool
    def executar_consulta(ctx: RunContext[SQLQuery], query: str) -> List[Dict]:
        try:
            print(f'### SQL gerado: {query}\n\n')
            conn = sqlite3.connect(BANCO_DADOS)
            cursor = conn.execute(query)
            resultados = cursor.fetchall()
            # print(f'### Resultado: {resultados}\n\n')
            colunas = [desc[0] for desc in cursor.description]
            # return [dict(zip(colunas, linha)) for linha in resultados]
            df = pd.DataFrame(resultados, columns=colunas)
            df.to_csv(os.path.join(DIR_EXTRACAO, f'ListaAtivosVR_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv'), encoding='UTF-8', sep=';')
            return resultados
        except Exception as ex:
            print(f'@@@ Error ao executar consulta.\n'
                  f'   @@@ {type(ex)}\n'
                  f'   @@@{ex=}')
        finally:
            conn.close()

    return agente

def main():
    extract_file(ARQUIVO_ZIP)
    conn = sqlite3.connect(BANCO_DADOS)

    lista_tabelas = []

    for arquivo in os.listdir(DIR_EXTRACAO):
        if arquivo.endswith('.xlsx'):
            caminho_arquivo = os.path.join(DIR_EXTRACAO, arquivo)
            nome_arquivo_sem_ext = os.path.splitext(arquivo)[0]
            nome_tabela = sanitize_name(nome_arquivo_sem_ext)

            if nome_tabela:
                # print(f"Processando {arquivo} -> Tabela: {nome_tabela}")
                script_tabela = create_table(conn, caminho_arquivo, nome_tabela)
                lista_tabelas.append(script_tabela)
            else:
                print(f"Nome inválido para {arquivo}, pulando.")

    conn.close()
    print("*** Arquivo descompactado e disponibilizado em BD ***")
    # print(f'Tabelas Geradas: {lista_tabelas}\n\n\n')
    DB_SCHEMA = lista_tabelas

    agente = agente_consulta_query()
    sqlQuery = SQLQuery(query='')
    resultado = agente.run_sync(str(lista_tabelas), deps=sqlQuery)
    print(f'Resultado:\n {resultado.output}')

if __name__ == "__main__":
    load_dotenv()
    # main()
    retorno_extracao = agente_extracao(ARQUIVO_ZIP)

    retorno_load = agente_load(retorno_extracao.filelist)
