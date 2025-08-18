import os
import sqlite3
from dotenv import load_dotenv

class DBManager:
    load_dotenv()
    dbname = os.environ.get('db_name')
    def __init__(self, db_name=dbname):
        self.conn = sqlite3.connect(db_name)

    def create_table(self, query):
        if str(query).startswith('CREATE TABLE'):
            self.conn.execute(query)
            self.conn.commit()

    def select(self, query):
        if str(query).startswith('SELECT'):
            cursor = self.conn.execute(query)
            return cursor.fetchall()

    def execute(self, sql):
        cursor = self.conn.execute(sql)

    def close_conexao(self):
        self.conn.close()
