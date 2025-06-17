import sqlite3

class NotasFiscaisCabecalhoDB:
    def __init__(self, db_name='resources/nfbase.db'):
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        query = '''
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
        '''
        self.conn.execute(query)
        self.conn.commit()
        self.conn.execute('DELETE FROM notas_fiscais_cabecalho;')

    def inserir_cabecalho(self,chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,evento_mais_recente,data_hora_evento,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,valor_nota_fiscal):
        query = '''
        INSERT INTO notas_fiscais_cabecalho (chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,evento_mais_recente,data_hora_evento,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,valor_nota_fiscal)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        self.conn.execute(query, (chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,evento_mais_recente,data_hora_evento,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,valor_nota_fiscal))
        self.conn.commit()

    def consultar_cabecalho(self):
        query = 'SELECT * FROM notas_fiscais_cabecalho'
        cursor = self.conn.execute(query)
        return cursor.fetchall()

    def consultar_sql(self, sql):
        cursor = self.conn.execute(sql)
        return cursor.fetchall()

    def fechar_conexao(self):
        self.conn.close()
