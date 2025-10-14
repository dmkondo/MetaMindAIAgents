import * as React from 'react';
import { Button, Form, FormGroup, PageSection, TextArea, TextInput, Title } from '@patternfly/react-core';
import { useEffect, useState } from 'react';
import Block from '@app/utils/Block';

const GeneralSettings: React.FunctionComponent = () => {

  const exemplo = "Analise esta imagem de nota fiscal. Extraia as seguintes informações e formate-as como um objeto JSON. Se um dado não for encontrado, use `null`. Não adicione nenhum texto antes ou depois do JSON. Certifique-se de que o JSON é válido: {\"cnpj\":[CNPJ, com 14 números ou mais], \"tipo_despesa\":[TIPO_DESPESA, classifique como: Alimentaçao, Aluguel, Combustível (se encontrar: Gasolina, Etanol, Diesel, GNV), Energia (Se encontrar companhias elétricas), Escritório, Internet, Telefone (telefonia, celular, oi, claro, tim, etc)  ] ,\"data\":[Data da emissão no formato DD/MM/AAAA], \"valor\":[Valor total pago da nota fiscal, em formato numérico com ponto como separador decimal, ex: 123.45]}";
  const [loading, setLoading] = useState(false);
  const [form, setForm] = useState({ prompt: '' });

  useEffect(() => {
    fetchData()
  }, [])

  function fetchData() {
    async function buscarJson() {
      try {
        const resposta = await fetch(process.env.APP_URL + '/configuration'); // 'http://localhost:3000/tarefas' https://jsonplaceholder.typicode.com/posts http://localhost:3000/tarefas
        if (!resposta.ok) {
          throw new Error('Erro ao carregar JSON');
        }
        const json = await resposta.json();
        setForm({ prompt: json.prompt || '' });

      } catch (erro) {
        console.error('Erro:', erro);
      } finally {
        setLoading(false);
      }
    }
    buscarJson();
  }


  function salvar(): void {
    async function salvarJson() {
      try {
        setLoading(true);
        const resposta = await fetch(process.env.APP_URL + '/configuration', {
          method: 'PUT',
          body: form.prompt ? JSON.stringify({ prompt: form.prompt }) : '{}',
          headers: {
            'Content-Type': 'application/json'
          }
        });
        if (!resposta.ok) {
          throw new Error('Erro ao carregar JSON');
        }
        const json = await resposta.json();
        json.prompt = form.prompt;
      } catch (erro) {
        console.error('Erro:', erro);
      }
      finally {
        setLoading(false);
      }
    }
    salvarJson();
  }

  return (
    <>
      <PageSection hasBodyWrapper={false}>
        <Title headingLevel="h1" size="lg">
          Prompt de Extração
        </Title>

        <h2>Prompt Exemplo</h2>
        <p>
          {exemplo}
        </p>
        <Form>

          <FormGroup label="Prompt Utililzado" fieldId="prompt">
            <TextArea
              id="prompt"
              value={form.prompt}
              rows={8}
              onChange={(e, v) => setForm({ ...form, ['prompt']: v })}
            />
          </FormGroup>

        </Form>

        <Button key="save" variant="primary" onClick={() => salvar()}>
          Salvar
        </Button>

      </PageSection>

      {loading && <Block message={"Salvando"} />}

    </>
  );
}

export { GeneralSettings };
