import google.generativeai as genai
from dotenv import load_dotenv
import os
from notas_fiscais_db import NotasFiscaisTesteDB

load_dotenv()

genai.configure(api_key=os.environ.get('gem_key'))
model = genai.GenerativeModel(model_name='gemini-1.5-flash')


consulta = input('Informe o que deseja consultar: ')
promp = f'''### PAPEL: Considerando um especialista em SQLite3.

### AÇÃO: Crie e retorne apenas uma query que {consulta}, pronto para ser executada.

### ESTRUTURA DA TABELA:
CREATE TABLE notas_fiscais_teste (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cnpj TEXT,
            razao_social TEXT,
            num_fiscal TEXT,
            cpf_cnpj_cliente TEXT,
            valor REAL
        );

### RESTRIÇÕES
- Retorne único e exclusivamente a query a ser executada, sem nenhum texto ou caractér adicional.
'''

print(promp)

response = model.generate_content([promp],
                                  generation_config=genai.GenerationConfig(
                                      temperature=0.5, top_p=1, top_k=32
                                  ))

resposta = f"*** {promp} ***\n\n\n\n{response.text}\n\n"

# Instanciar controlador do banco de dados
db = NotasFiscaisTesteDB()

print(f'\n\n\n{response.text}')

registros = db.consultar_notas_sql(response.text)
print("Registros inseridos:")
for registro in registros:
    print(registro)
