# poc-fastapi-llm
POC Fastapi com Gemini. Criado para o segundo trabalho do curso da I1A2 em Junho de 2025.

## Run

```
fastapi run app/main.py --port 8000

```

## Live reload

```
uvicorn app.main:app --reload --port 8000
```

## LLM Mistral 

para testar endpoit invoices/extract/mistral, instale:

```
sudo apt update
sudo apt install tesseract-ocr -y
sudo apt install tesseract-ocr-por
```
## Acessar Swagger

```
http://127.0.0.1:8000/docs#/
```

## API Swagger

<div align="center">

![](SourceCode/poc-fastapi-llm-ocr/notas-fiscais/api.png)

</div>

## Teste

### teste no swagger

<div align="center">

![](SourceCode/poc-fastapi-llm-ocr/notas-fiscais/teste1.PNG)

![](SourceCode/poc-fastapi-llm-ocr/notas-fiscais/teste1b.PNG)

</div>

### Nota utilizada

<div align="center">

![](SourceCode/poc-fastapi-llm-ocr/notas-fiscais/nota1.PNG) 

</div>


# poc-fastapi-llm-ocr-frontend-react

Frontend utilizado na POC Fastapi com Gemini. Criado para o segundo trabalho do curso da I1A2 em Junho de 2025.

<https://github.com/dmkondo/MetaMindAIAgents/tree/main/Projeto%20Final%20-%20Artefatos/SourceCode/poc-fastapi-llm-ocr>

## Run

```bash
npm run start:dev
```

<div align="center">

## Lista de Notas Fiscais

![](SourceCode/poc-fastapi-llm-ocr-frontend-react/assets/tela1.png)

## Adicionar Nota

![](SourceCode/poc-fastapi-llm-ocr-frontend-react/assets/tela2.png)

## Extração de dados

![](SourceCode/poc-fastapi-llm-ocr-frontend-react/assets/tela3.png)

</div>
