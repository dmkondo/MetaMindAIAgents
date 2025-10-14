import * as React from 'react';
import { PageSection, Title } from '@patternfly/react-core';
import { TableColumnManagement } from './Table1';

const Dashboard: React.FunctionComponent = () => (
  <PageSection hasBodyWrapper={false}>
    <Title headingLevel="h1" size="lg">Cadastro de Notas Fiscais</Title>

<TableColumnManagement />

  </PageSection>
)

export { Dashboard };
