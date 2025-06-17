import sqlite3

class NotasFiscaisTesteDB:
    def __init__(self, db_name='resources/nfbase.db'):
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        query = '''
        CREATE TABLE IF NOT EXISTS notas_fiscais_teste (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cnpj TEXT,
            razao_social TEXT,
            num_fiscal TEXT,
            cpf_cnpj_cliente TEXT,
            valor REAL
        )
        '''
        self.conn.execute(query)
        self.conn.commit()

    def inserir_nota(self, cnpj, razao_social, num_fiscal, cpf_cnpj_cliente, valor):
        query = '''
        INSERT INTO notas_fiscais_teste (cnpj, razao_social, num_fiscal, cpf_cnpj_cliente, valor)
        VALUES (?, ?, ?, ?, ?)
        '''
        self.conn.execute(query, (cnpj, razao_social, num_fiscal, cpf_cnpj_cliente, valor))
        self.conn.commit()

    def consultar_notas(self):
        query = 'SELECT * FROM notas_fiscais_teste'
        cursor = self.conn.execute(query)
        return cursor.fetchall()

    def consultar_notas_sql(self, sql):
        cursor = self.conn.execute(sql)
        return cursor.fetchall()

    def fechar_conexao(self):
        self.conn.close()
