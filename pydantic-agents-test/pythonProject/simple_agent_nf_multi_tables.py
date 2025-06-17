import os
import zipfile
import sqlite3
import pandas as pd
from datetime import date
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext, format_as_xml
from typing import List, Dict
from dotenv import load_dotenv
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider

from notas_fiscais_cabecalho_db import NotasFiscaisCabecalhoDB
from notas_fiscais_itens_db import NotasFiscaisItensDB

load_dotenv()

# Configuração inicial
ARQUIVO_ZIP = "resources/202401_NFs.zip"
DIR_EXTRACAO = "resources/temp"
BANCO_DADOS = "notas.db"
DB_SCHEMA = """
        CREATE TABLE IF NOT EXISTS notas_fiscais_cabecalho (
            chave_acesso TEXT,
            modelo TEXT,
            serie TEXT,
            numero TEXT,
            natureza_operacao TEXT,
            data_emissao TEXT,
            evento_mais_recente TEXT,
            data_hora_evento TEXT,
            cpf_cnpj_emitente TEXT,
            razao_social_emitente TEXT,
            inscricao_estadual_emitente TEXT,
            uf_emitente TEXT,
            municipio_emitente TEXT,
            cnpj_destinatario TEXT,
            nome_destinatario TEXT,
            uf_destinatario TEXT,
            indicador_ie_destinatario TEXT,
            destino_operacao TEXT,
            consumidor_final TEXT,
            presenca_comprador TEXT,
            valor_nota_fiscal REAL
        );
        CREATE TABLE IF NOT EXISTS notas_fiscais_itens (
            chave_acesso TEXT,
            modelo TEXT,
            serie TEXT,
            numero TEXT,
            natureza_operacao TEXT,
            data_emissao TEXT,
            cpf_cnpj_emitente TEXT,
            razao_social_emitente TEXT,
            inscricao_estadual_emitente TEXT,
            uf_emitente TEXT,
            municipio_emitente TEXT,
            cnpj_destinatario TEXT,
            nome_destinatario TEXT,
            uf_destinatario TEXT,
            indicador_ie_destinatario TEXT,
            destino_operacao TEXT,
            consumidor_final TEXT,
            presenca_comprador TEXT,
            numero_produto INTEGER,
            descricao_produto TEXT,
            codigo_ncm_sh TEXT,
            ncm_sh TEXT,
            cfop TEXT,
            quantidade INTEGER,
            unidade TEXT,
            valor_unitario REAL,
            valor_total REAL
        );        
"""
SQL_EXEMPLOS = [
    {
        'solicitação': 'Quantas notas ficais foram emitidas?',
        'resposta': "SELECT COUNT(*) AS Quantidade_Notas_Fiscais FROM notas_fiscais_cabecalho;",
    },
    {
        'solicitação': "Quantos itens tinha a nota fiscal com chave de acesso 35240134028316923228550010003691801935917886?",
        'resposta': "SELECT COUNT(*) AS Quantidade_Itens FROM notas_fiscais_cabecalho AS C, notas_fiscais_itens AS I WHERE C.chave_acesso=I.chave_acesso AND upper(C.chave_acesso)=upper('35240134028316923228550010003691801935917886');",
    },
    {
        'solicitação': 'Qual o valor médio dos itens das notas fiscais emitidas na cidade de São Paulo?',
        'resposta': "SELECT AVG(I.valor_total) AS Media_Nota_Fiscal_Sao_Paulo FROM notas_fiscais_cabecalho AS C, notas_fiscais_itens AS I WHERE C.chave_acesso=I.chave_acesso AND upper(C.municipio_emitente)=Upper('BARBALHA');",
    },
    {
        'solicitação': 'Retorne os estados que apresentaram maior valor faturado com o respectivo valor juntamente com o valor da nota fisca com maior valor.',
        'resposta': "SELECT SUM(valor_nota_fiscal) AS Soma_Nota_Fiscal, MAX(valor_nota_fiscal) AS Maior_Valor_Nota_Fiscal, uf_emitente FROM notas_fiscais_cabecalho GROUP BY uf_emitente;",
    },
]

# Descompactação do arquivo ZIP
def extrair_zip(file):
    try:
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(DIR_EXTRACAO)

        # Encontra o primeiro arquivo CSV extraído
        filelist = []
        for arquivo in os.listdir(DIR_EXTRACAO):
            if arquivo.endswith('.csv'):
                filelist.append(os.path.join(DIR_EXTRACAO,arquivo))
        return filelist
    except Exception as ex:
        print(f'@@@ Error ao extrair arquivo ZIP.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')

# Processamento do CSV brasileiro
def processar_csv(csv_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            csv_path,
            sep=',',
            decimal='.',
            encoding='UTF-8',
            dayfirst=True
        )
    except Exception as ex:
        print(f'@@@ Error ao processar CSV.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')

# Criação de cabecalho no banco SQLite
def insere_cabecalhos_banco(df: pd.DataFrame):
    try:
        db = NotasFiscaisCabecalhoDB(db_name=BANCO_DADOS)
        for _, linha in df.iterrows():
            db.inserir_cabecalho(
                chave_acesso=str(linha['CHAVE DE ACESSO']),
                modelo=str(linha['MODELO']),
                serie=str(linha['SÉRIE']),
                numero=str(linha['NÚMERO']),
                natureza_operacao=str(linha['NATUREZA DA OPERAÇÃO']),
                data_emissao=str(linha['DATA EMISSÃO']),
                evento_mais_recente=str(linha['EVENTO MAIS RECENTE']),
                data_hora_evento=str(linha['DATA/HORA EVENTO MAIS RECENTE']),
                cpf_cnpj_emitente=str(linha['CPF/CNPJ Emitente']),
                razao_social_emitente=str(linha['RAZÃO SOCIAL EMITENTE']),
                inscricao_estadual_emitente=str(linha['INSCRIÇÃO ESTADUAL EMITENTE']),
                uf_emitente=str(linha['UF EMITENTE']),
                municipio_emitente=str(linha['MUNICÍPIO EMITENTE']),
                cnpj_destinatario=str(linha['CNPJ DESTINATÁRIO']),
                nome_destinatario=str(linha['NOME DESTINATÁRIO']),
                uf_destinatario=str(linha['UF DESTINATÁRIO']),
                indicador_ie_destinatario=str(linha['INDICADOR IE DESTINATÁRIO']),
                destino_operacao=str(linha['DESTINO DA OPERAÇÃO']),
                consumidor_final=str(linha['CONSUMIDOR FINAL']),
                presenca_comprador=str(linha['PRESENÇA DO COMPRADOR']),
                valor_nota_fiscal=str(linha['VALOR NOTA FISCAL'])
            )
    except Exception as ex:
        print(f'@@@ Error ao inserir CABECALHO no DB.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')
    finally:
        db.fechar_conexao()

# Criação de item no banco SQLite
def insere_itens_banco(df: pd.DataFrame):
    try:
        db = NotasFiscaisItensDB(db_name=BANCO_DADOS)
        for _, linha in df.iterrows():
            db.inserir_itens(
                chave_acesso=str(linha['CHAVE DE ACESSO']),
                modelo=str(linha['MODELO']),
                serie=str(linha['SÉRIE']),
                numero=str(linha['NÚMERO']),
                natureza_operacao=str(linha['NATUREZA DA OPERAÇÃO']),
                data_emissao=str(linha['DATA EMISSÃO']),
                cpf_cnpj_emitente=str(linha['CPF/CNPJ Emitente']),
                razao_social_emitente=str(linha['RAZÃO SOCIAL EMITENTE']),
                inscricao_estadual_emitente=str(linha['INSCRIÇÃO ESTADUAL EMITENTE']),
                uf_emitente=str(linha['UF EMITENTE']),
                municipio_emitente=str(linha['MUNICÍPIO EMITENTE']),
                cnpj_destinatario=str(linha['CNPJ DESTINATÁRIO']),
                nome_destinatario=str(linha['NOME DESTINATÁRIO']),
                uf_destinatario=str(linha['UF DESTINATÁRIO']),
                indicador_ie_destinatario=str(linha['INDICADOR IE DESTINATÁRIO']),
                destino_operacao=str(linha['DESTINO DA OPERAÇÃO']),
                consumidor_final=str(linha['CONSUMIDOR FINAL']),
                presenca_comprador=str(linha['PRESENÇA DO COMPRADOR']),
                numero_produto=str(linha['NÚMERO PRODUTO']),
                descricao_produto=str(linha['DESCRIÇÃO DO PRODUTO/SERVIÇO']),
                codigo_ncm_sh=str(linha['CÓDIGO NCM/SH']),
                ncm_sh=str(linha['NCM/SH (TIPO DE PRODUTO)']),
                cfop=str(linha['CFOP']),
                quantidade=str(linha['QUANTIDADE']),
                unidade=str(linha['UNIDADE']),
                valor_unitario=str(linha['VALOR UNITÁRIO']),
                valor_total=str(linha['VALOR TOTAL'])
            )
    except Exception as ex:
        print(f'@@@ Error ao inserir ITEM no DB.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')
    finally:
        db.fechar_conexao()

# Inicialização do sistema
def inicializar_sistema(fileZip: str):
    filelist = extrair_zip(fileZip)
    for csv_path in filelist:
        df = processar_csv(csv_path)
        if str(csv_path).endswith('Cabecalho.csv'):
            insere_cabecalhos_banco(df)
        if str(csv_path).endswith('Itens.csv'):
            insere_itens_banco(df)

    model = GeminiModel(
        'gemini-1.5-flash', provider=GoogleGLAProvider(api_key=os.environ.get('gem_key'))
    )

    agente = Agent(
        model,
        retries=3,
        deps_type=str,
        system_prompt=f"""### PAPEL: Especialista em SQLite3.
        ### AÇÃO: Crie a query, execute a consulta através da função 'executar_consulta' enviando como parâmetro apenas a query SQL a ser executada e retorne como resultado apenas os registros em formato markdown.
        ### ESTRUTURA DA TABELA:
        {DB_SCHEMA}
        ### DATA ATUAL: {date.today()}
        ### EXEMPLOS:
        {format_as_xml(SQL_EXEMPLOS)}
        ### PONTOS DE ATENÇÃO
        - Ao acionar a função 'executar_consulta', envie apenas a query SQL a ser executada, sem nenhum texto ou caractér adicional.
        - Caso seja solicitado algo fora do seu contexto retorne vazio ('').
        - Utilize alias para nomear a coluna retornada conforme o contexto dela.
        - Para procurar conteúdo de texto utilizar sempre a função UPPER para garantir que não tenha problemas decorrentes de consistência de dados.
        """
    )

    @agente.tool
    def executar_consulta(ctx: RunContext[sqlite3.Connection], query: str) -> List[Dict]:
        try:
            print(f'SQL gerado: {query}')
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

    return agente

# Interface de consulta
def main():
    agente = inicializar_sistema(ARQUIVO_ZIP)

    while True:
        try:
            pergunta = input("\nDigite sua consulta (ou '.quit'): ")
            if pergunta.lower() == '.quit':
                break

            resultado = agente.run_sync(pergunta, deps=ARQUIVO_ZIP)
            print(resultado.output)

        except Exception as ex:
            print(f'@@@ Error ao executar main.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')


if __name__ == "__main__":
    main()
