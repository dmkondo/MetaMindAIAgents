from notas_fiscais_teste_db import NotasFiscaisTesteDB
from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from dotenv import load_dotenv
import os

load_dotenv()

model = GeminiModel(
    'gemini-1.5-flash', provider=GoogleGLAProvider(api_key=os.environ.get('gem_key'))
)

agent = Agent(
    model,
    system_prompt=f'''### PAPEL: Especialista em SQLite3.
### AÇÃO: Crie e retorne apenas uma query conforme o solicitado, pronto para ser executada.
### ESTRUTURA DA TABELA:
CREATE TABLE notas_fiscais_teste (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cnpj TEXT,
            razao_social TEXT,
            num_fiscal TEXT,
            cpf_cnpj_cliente TEXT,
            valor REAL
        );
### EXEMPLO
SELECT * FROM notas_fiscais_teste WHERE id % 2 != 0;
### PONTOS DE ATENÇÃO
- Retorne único e exclusivamente a query a ser executada, sem nenhum texto ou caractér adicional.
- Caso seja solicitado algo fora do seu contexto retorne vazio ('').
''',
)

b=True

# Instanciar controlador do banco de dados
db = NotasFiscaisTesteDB()

while b==True:
    consulta = input('### Informe o que deseja consultar: ')
    if consulta=='.quit':
        b=False
    else:
        result = agent.run_sync(consulta)
        if type(result.output) is str and result.output!='' and str(result.output).startswith('SELECT'):
            print(result.output)
            registros = db.consultar_notas_sql(result.output)
            print("### Registros identificados:")
            for registro in registros:
                print(registro)
            print('\n\n\n')
        else:
            print('   ### Comando não identificado.\n\n\n')

db.fechar_conexao()