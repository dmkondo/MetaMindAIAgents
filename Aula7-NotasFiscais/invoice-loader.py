import pandas as pd
from notas_fiscais_db import NotasFiscaisTesteDB
import openpyxl


def sheet_loader(filepath):
    """
    Processa arquivo XLS e insere dados no banco SQLite
    Retorna todos os registros da tabela após inserção
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


if __name__ == "__main__":
    registros = sheet_loader('resources/planilha_teste.xlsx')
    print(f"Registros inseridos:{registros}")
