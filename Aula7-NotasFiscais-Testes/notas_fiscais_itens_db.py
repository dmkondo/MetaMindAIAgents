import sqlite3

class NotasFiscaisItensDB:
    def __init__(self, db_name='resources/nfbase.db'):
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        query = '''
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
        )
        '''
        self.conn.execute(query)
        self.conn.commit()

    def inserir_itens(self, chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,numero_produto,descricao_produto,codigo_ncm_sh,ncm_sh,cfop,quantidade,unidade,valor_unitario,valor_total):
        query = '''
        INSERT INTO notas_fiscais_itens (chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,numero_produto,descricao_produto,codigo_ncm_sh,ncm_sh,cfop,quantidade,unidade,valor_unitario,valor_total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        self.conn.execute(query, (chave_acesso,modelo,serie,numero,natureza_operacao,data_emissao,cpf_cnpj_emitente,razao_social_emitente,inscricao_estadual_emitente,uf_emitente,municipio_emitente,cnpj_destinatario,nome_destinatario,uf_destinatario,indicador_ie_destinatario,destino_operacao,consumidor_final,presenca_comprador,numero_produto,descricao_produto,codigo_ncm_sh,ncm_sh,cfop,quantidade,unidade,valor_unitario,valor_total))
        self.conn.commit()

    def consultar_itens(self):
        query = 'SELECT * FROM notas_fiscais_itens'
        cursor = self.conn.execute(query)
        return cursor.fetchall()

    def consultar_sql(self, sql):
        cursor = self.conn.execute(sql)
        return cursor.fetchall()

    def fechar_conexao(self):
        self.conn.close()
