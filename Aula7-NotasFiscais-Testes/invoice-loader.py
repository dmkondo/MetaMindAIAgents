import pandas as pd

from notas_fiscais_cabecalho_db import NotasFiscaisCabecalhoDB
from notas_fiscais_itens_db import NotasFiscaisItensDB
from notas_fiscais_teste_db import NotasFiscaisTesteDB
from zipfile import ZipFile
import os
import datetime
import openpyxl

def extract_invoice(zippath, destpath):
    try:
        with ZipFile(zippath) as invoice_zip:
            if not os.path.exists(destpath):
                os.makedirs(destpath)
            invoice_zip.extractall(path=destpath)

    except Exception as ex:
        print(f'@@@ Error ao extrair NF.\n'
              f'   @@@ {type(ex)}\n'
              f'   @@@{ex=}')


def invoice_test_loader(filepath):
    """
    Processa arquivo XLS e insere dados no banco SQLite
    """
    # Ler arquivo Excel usando pandas
    df = pd.read_excel(filepath, sheet_name='Planilha1')

    # Instanciar controlador do banco de dados
    db = NotasFiscaisTesteDB()

    try:
        # Inserir cada registro da planilha
        counter=0
        for _, linha in df.iterrows():
            db.inserir_nota(
                cnpj=str(linha['cnpj']),
                razao_social=linha['razao_social'],
                num_fiscal=str(linha['num_fiscal']),
                cpf_cnpj_cliente=str(linha['cpf_cnpj_cliente']),
                valor=float(linha['valor'])
            )
            counter=counter+1

        # Recuperar todos os registros inseridos
        return counter

    finally:
        db.fechar_conexao()

def invoice_header_loader(filepath):
    """
    Processa arquivo CSV com os cabecalhos de NF e insere-os no banco SQLite
    """
    # Ler arquivo Excel usando pandas
    df = pd.read_csv(filepath)

    # Instanciar controlador do banco de dados
    db = NotasFiscaisCabecalhoDB()

    try:
        # Inserir cada registro de cabecalho
        counter=0
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
            counter=counter+1
        # Retorna cabecalhos inseridos
        return counter
    finally:
        db.fechar_conexao()

def invoice_itens_loader(filepath):
    """
    Processa arquivo CSV com os itens de NF e insere-os no banco SQLite
    """
    # Ler arquivo csv usando pandas
    df = pd.read_csv(filepath)

    # Instanciar controlador do banco de dados
    db = NotasFiscaisItensDB()

    try:
        # Inserir cada registro de cabecalho
        counter=0
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
            counter=counter+1
        # Retorna itens inseridos
        return counter
    finally:
        db.fechar_conexao()

if __name__ == "__main__":
    ### Insere NFs na tabela de NF Teste
    # registros = invoice_test_loader('resources/planilha_teste.xlsx')
    # print(f"Registros inseridos:{registros}")

    ### Testa extração de arquivos .zip
    # zippath = 'resources/202401_NFs.zip'
    # destpath = f'resources/nfs_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}'
    # extract_invoice(zippath, destpath)

    ### Testa load de cabecalhos
    # registros = invoice_header_loader('resources/nfs_20250608_222700/202401_NFs_Cabecalho.csv')
    # print(f"Registros inseridos:{registros}")

    ### Testa load de itens
    registros = invoice_itens_loader('resources/nfs_20250608_222700/202401_NFs_Itens.csv')
    print(f"Registros inseridos:{registros}")