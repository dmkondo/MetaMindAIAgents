import * as React from 'react';
import { Card, CardBody, CardHeader, CardTitle, Checkbox, Gallery, PageSection, Title } from '@patternfly/react-core';
import { useState } from 'react';

const ProfileSettings: React.FunctionComponent = () => {

  const [isChecked1, setIsChecked1] = useState(true);
  const [isChecked2, setIsChecked2] = useState(false);
  const [isChecked3, setIsChecked3] = useState(false);
  const [isSecondary, setIsSecondary] = useState<boolean>(false);

  const id1 = 'selectable-card-input-1';
  const id2 = 'selectable-card-input-2';
  const id3 = 'selectable-card-input-3';

  const onChange = (event: React.FormEvent<HTMLInputElement>, checked: boolean) => {
    const name = event.currentTarget.name;

    /*     switch (name) {
          case id1:
            setIsChecked1(checked);
            setIsChecked2(!checked);
            setIsChecked3(!checked);
            break;
          case id2:
            setIsChecked1(!checked);
            setIsChecked2(checked);
            setIsChecked3(!checked);
            break;
          case id3:
            setIsChecked1(!checked);
            setIsChecked2(!checked);
            setIsChecked3(checked);
            break;
        } */
  };

  return (
    <PageSection hasBodyWrapper={false}>
      <Title headingLevel="h1" size="lg">
        Selecionar LLM
      </Title>


      <div style={{ marginTop: '15px' }}>
        <Gallery hasGutter>
          <Card
            id="selectable-card-example-1"
            isSelectable
            isSelected={isChecked1}
            variant={isSecondary ? 'secondary' : 'default'}
          >
            <CardHeader
              selectableActions={{
                selectableActionId: id1,
                selectableActionAriaLabelledby: 'selectable-card-example-1',
                name: id1,
                onChange,
                hasNoOffset: true
              }}
            >
              <CardTitle>Gemini</CardTitle>
            </CardHeader>
            <CardBody>https://gemini.google.com/</CardBody>
          </Card>
          <Card
            id="selectable-card-example-2"
            isSelectable
            isDisabled
            variant={isSecondary ? 'secondary' : 'default'}
          >
            <CardHeader
              selectableActions={{
                selectableActionId: id2,
                selectableActionAriaLabelledby: 'selectable-card-example-2',
                name: id2,
                onChange,
                hasNoOffset: true
              }}
            >
              <CardTitle>Mistral</CardTitle>
            </CardHeader>
            <CardBody>https://mistral.ai/</CardBody>
          </Card>
          <Card
            id="selectable-card-example-3"
            isSelectable
            isDisabled
            isSelected={isChecked3}
            variant={isSecondary ? 'secondary' : 'default'}
          >
            <CardHeader
              selectableActions={{
                selectableActionId: id3,
                selectableActionAriaLabelledby: 'selectable-card-example-3',
                name: id3,
                onChange,
                hasNoOffset: true
              }}
            >
              <CardTitle>ChatGPT</CardTitle>
            </CardHeader>
            <CardBody>https://chatgpt.com/</CardBody>
          </Card>
        </Gallery>
      </div>



    </PageSection>
  );

}

export { ProfileSettings };
