import * as React from 'react';
import { ExclamationTriangleIcon } from '@patternfly/react-icons';
import {
  Button,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  PageSection,
} from '@patternfly/react-core';
import { useNavigate } from 'react-router-dom';

const NotFound: React.FunctionComponent = () => {
  function GoHomeBtn() {
    const navigate = useNavigate();
    function handleClick() {
      navigate('/');
    }
    return (
      <Button onClick={handleClick}>Ir para cadastro de notas fiscais</Button>
    );
  }

  return (
    <PageSection hasBodyWrapper={false}>
      <EmptyState titleText="Extração Inteligente de Dados de Notas Fiscais" variant="full" icon={ExclamationTriangleIcon} >
        <EmptyStateBody>
          IAA2 - Trabalho de término de curso / SETEMBRO 2025 <br />
          Participantes do Grupo: <br />
          Bruno Kenzo  <br />
          Dario Volnei <br />
          Douglas Kondo <br />
          Elizabeth Marinho <br />
          Felipe Santos <br />
          Henrique Sardelli <br />
          Henry Monteiro <br />
          Lyndon Tavares
        </EmptyStateBody><EmptyStateFooter>
          <GoHomeBtn />
        </EmptyStateFooter></EmptyState>
    </PageSection>
  )
};

export { NotFound };
