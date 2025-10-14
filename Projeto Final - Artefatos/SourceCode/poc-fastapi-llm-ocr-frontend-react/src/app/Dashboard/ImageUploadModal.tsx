import React, { useRef, useState } from 'react';
import {
  Modal,
  Button,
  DatePicker,
  Form,
  FormGroup,
  TextInput,
  FileUpload,
  TextArea,
  ModalBody,
  ModalFooter,
  ModalHeader,
  Content,
  Alert,
  AlertProps,
  AlertGroup,
  AlertActionCloseButton,
  AlertVariant,
  Flex,
  FlexItem,
  ValidatedOptions,
  FormSelect,
  FormSelectOption
} from '@patternfly/react-core';
import Block from '@app/utils/Block';
import { validaCNPJ } from '@app/utils/cnpjUtil';
import { formatDateBRtoISO, isValidDateBR } from '@app/utils/dateUtil';

export default function ImageUploadModal({ isOpen, onClose, onSubmit }) {
  const [imageFile, setImageFile] = useState(null);
  const [imagePreview, setImagePreview] = useState(null);
  const [zoomed, setZoomed] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [mensagem, setMensagem] = useState('');

  const [rotation, setRotation] = useState(0);
  const [isValidDate, setIsValidDate] = useState(true);

  const listaDespesas = [
    { value: '', label: '', disabled: false },
    { value: 'Alimentação', label: 'Alimentação', disabled: false },
    { value: 'Aluguel', label: 'Aluguel', disabled: false },
    { value: 'Combustível', label: 'Combustível', disabled: false },
    { value: 'Energia', label: 'Energia', disabled: false },
    { value: 'Escritório', label: 'Escritório', disabled: false },
    { value: 'Internet', label: 'Internet', disabled: false },
    { value: 'Telefone', label: 'Telefone', disabled: false },
  ];

  const rotate = () => {
    setRotation((prev) => (prev + 90) % 360); // gira 90° por clique
  };

  const dateFormat = (date: Date) =>
    date.toLocaleDateString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit' }).replace(/\//g, '-');


  const dateParse = (date: string) => {
    const split = date.split('-');
    if (split.length !== 3) {
      return new Date();
    }
    const month = split[0];
    const day = split[1];
    const year = split[2];
    return new Date(`${year.padStart(4, '0')}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T00:00:00`);
  };

  const [form, setForm] = useState({
    id: '',
    cnpj: '',
    tipo_despesa: '',
    data_emissao: '',
    valor_total: '',
    imagem_hash: '',
    status: 'CHECKING'
  });

  const [alerts, setAlerts] = useState<Partial<AlertProps>[]>([]);
  const addAlert = (title: string, variant: AlertProps['variant'], key: React.Key) => {
    setAlerts((prevAlerts) => [{ title, variant, key }, ...prevAlerts]);
  };
  const removeAlert = (key: React.Key) => {
    setAlerts((prevAlerts) => [...prevAlerts.filter((alert) => alert.key !== key)]);
  };

  function handleChange(name, value) {
    setForm({ ...form, [name]: value });
    console.log(">>>handleChange ", name, value);
    console.log(">>>form ", form);
  }

  const handleDateChange = (value: string, date) => {
    setForm({ ...form, ["data_emissao"]: date });
    console.log(">>>handleDateChange ", value, date);
    console.log(">>>form ", form);
  };

  const processingRef = useRef(false);
  async function handleFileChange(event, file) {
    event?.preventDefault?.();

    // Evita execução duplicada
    if (processingRef.current) return;
    processingRef.current = true;

    setIsLoading(true);
    setMensagem('Processando extração de dados...');
    setImageFile(file);

    if (file) {
      const reader = new FileReader();
      reader.onload = () => {
        setImagePreview(reader.result);
      };
      reader.readAsDataURL(file);
    }

    try {
      const payload = new FormData();
      payload.append('file', file);

      const resposta = await fetch(process.env.APP_URL + '/invoices/extract/check', {
        method: 'POST',
        body: payload,
        /*         headers: {
                  'Content-Type': 'application/json'
                } */
      })
      const json = await resposta.json();


      setForm({
        ...form,
        id: json.id || '',
        cnpj: json.cnpj || '',
        tipo_despesa: json.tipo_despesa || '',
        data_emissao: converterData(json.data_emissao) || '',
        valor_total: json.valor_total || '',
        imagem_hash: json.imagem_hash || '',
        status: json.status || 'CHECKING'
      });

      console.log('>>>json', json);
      console.log('>>>form', form);

    } catch (error) {
      processingRef.current = false;
      addAlert('Erro ao processar extração de dados.', 'danger', getUniqueId());
    } finally {
      processingRef.current = false;
      setIsLoading(false);
    }

  }

  function converterData(dataBr) {
    const [dia, mes, ano] = dataBr.split('/');
    if (!dia || !mes || !ano || dia.length !== 2 || mes.length !== 2 || ano.length !== 4) {
      throw new Error("Formato de data inválido. Esperado: dd/mm/yyyy");
    }
    //return `${ano}-${mes}-${dia}`;
    return `${dia}/${mes}/${ano}`;
  }

  function handleClearImage() {
    setImageFile(null);
    setImagePreview(null);
    setZoomed(false);
    clearform();
  }

  function clearform() {
    setForm({
      id: '',
      tipo_despesa: '',
      cnpj: '',
      data_emissao: '',
      valor_total: '',
      imagem_hash: '',
      status: 'CHECKING'
    });
  }

  function handleSubmit() {
    const payload = {
      ...form,
      imagem: imageFile
    };
    onSubmit(payload);
    onClose();
    resetForm();
  }

  function resetForm() {
    setForm({
      id: '',
      cnpj: '',
      tipo_despesa: '',
      data_emissao: '',
      valor_total: '',
      imagem_hash: '',
      status: ''
    });
    setImageFile(null);
    setImagePreview(null);
    setZoomed(false);
  }
  const getUniqueId = () => new Date().getTime();

  function formCheck() {
    if (!form.tipo_despesa) {
      addAlert('[TIPO DE DESPESA] é obrigatório.', 'warning', getUniqueId());
      return false;
    }
    if (!form.cnpj) {
      addAlert('[CNPJ] é obrigatório.', 'warning', getUniqueId());
      return false;
    }
    if (!validaCNPJ(form.cnpj)) {
      addAlert('[CNPJ] inválido.', 'warning', getUniqueId());
      return false;
    }

    if (!form.data_emissao) {
      addAlert('[Data emissão] é obrigatório.', 'warning', getUniqueId());
      return false;
    }

    if (!isValidDateBR(form.data_emissao)) {
      addAlert('[Data emissão] inválida.', 'warning', getUniqueId());
      return false;
    }

    if (!form.valor_total) {
      addAlert('[Valor] é obrigatório.', 'warning', getUniqueId());
      return false;
    }
    if (!imageFile) {
      addAlert('[Imagem] é obrigatório.', 'warning', getUniqueId());
      return false;
    }
    if (form.status === 'PROCESSADO') {
      addAlert('A nota fiscal já processada e salva na base de dados. Selecione uma nova Nota Fiscal', 'warning', getUniqueId());
      return false;
    }
    return true
  }



  async function salvar() {
    if (!formCheck()) {
      return;
    }

    setMensagem('Salvando...');
    setIsLoading(true);

    const payload = new FormData();
    payload.append('id', '0');
    payload.append('tipo_despesa', form.tipo_despesa);
    payload.append('data_emissao', formatDateBRtoISO(form.data_emissao));
    payload.append('valor_total', form.valor_total);
    payload.append('cnpj', form.cnpj);
    payload.append('imagem_hash', form.imagem_hash);
    payload.append('status', form.status);

    const json = JSON.stringify(Object.fromEntries(payload.entries()));

    try {
      const response = await fetch(process.env.APP_URL + '/invoices/add', {
        method: 'POST',
        body: json,
        headers: {
          'Content-Type': 'application/json'
        }
      });

      console.log(response);

      if (response.ok) {
        clearform();
        //addAlert('Salvo com sucesso.', 'success', getUniqueId());
        onClose();
      } else {
        setIsLoading(false)
        addAlert('Erro ao enviar nots fiscal', 'danger', getUniqueId());
      }
    } catch (error) {
      setIsLoading(false)
      console.error(error);
      addAlert('Erro ao enviar nots fiscal', 'danger', getUniqueId());
    } finally {
      setIsLoading(false);
      resetForm();
    }
  };


  return (
    <Modal
      aria-label="Formulário de adicionar nota fiscal"
      isOpen={isOpen}
      onClose={(event) => { clearform(); onClose(event) }}
    >

      <ModalHeader
        title="Adicionar Nota Fiscal"
        labelId="basic-modal-title"
        description={
          <Content>
            <p>Faça upload de notas fiscais para extraçõ de dados.</p>
          </Content>
        }
      />

      <ModalBody id="modal-box-body-basic">


        <Form >
          <div style={{ display: 'flex', gap: '2rem' }}>
            {/* Coluna 1: Campos do formulário */}
            <div style={{ flex: 1 }}>
              <FormGroup label="ID" fieldId="id">
                <TextInput
                  readOnly
                  id="id"
                  value={form.id}
                  onChange={(e, val) => handleChange('id', val)}
                />
              </FormGroup>

              <FormGroup label="Tipo de Despesa" isRequired fieldId="tipo_despesa">
                <FormSelect
                  value={form.tipo_despesa}
                  onChange={(e, val) => handleChange('tipo_despesa', val)}
                  aria-label="FormSelect Input"
                  ouiaId="BasicFormSelect">
                  {listaDespesas.map((option, index) => (
                    <FormSelectOption isDisabled={option.disabled}
                      key={index}
                      value={option.value}
                      label={option.label} />
                  ))}
                </FormSelect>
              </FormGroup>

              <FormGroup label="CNPJ" isRequired fieldId="cnpj">
                <TextInput
                  id="cnpj"
                  value={form.cnpj}
                  onChange={(e, val) => handleChange('cnpj', val)}
                  isRequired
                /* isRequired
                validated={form.cnpj ? ValidatedOptions.success : ValidatedOptions.error} */

                />
              </FormGroup>

              <FormGroup label="Data de Emissão" isRequired fieldId="data_emissao" >
                <DatePicker
                  onBlur={(_event, str, date) => console.log('onBlur', str, date)}
                  id="data_emissao"
                  value={form.data_emissao}
                  onChange={(e, val) => handleDateChange('data_emissao', val)}
                  requiredDateOptions={{ isRequired: true, emptyDateText: 'Data emissão é obrigatória' }}
                  invalidFormatText='Formato inválido. Use DD/MM/AAAA'
                  placeholder='DD/MM/AAAA'
                  dateFormat={(date: Date) =>
                    new Intl.DateTimeFormat("pt-BR", {
                      day: "2-digit",
                      month: "2-digit",
                      year: "numeric",
                    }).format(date)
                  }
                  dateParse={dateParse}
                />

              </FormGroup>

              <FormGroup label="Valor Pago" isRequired fieldId="valor_total">
                <TextInput
                  id="valor_total"
                  type="number"
                  step="0.01"
                  value={form.valor_total}
                  onChange={(e, val) => handleChange('valor_total', val)}

                />
              </FormGroup>

              <FormGroup label="Imagem Hash" fieldId="imagem_hash">
                <TextInput
                  readOnly
                  id="imagem_hash"
                  value={form.imagem_hash}
                  onChange={(e, val) => handleChange('imagem_hash', val)}
                />
              </FormGroup>

              <FormGroup label="Status" fieldId="status">
                <TextInput
                  readOnly
                  id="status"
                  value={form.status}
                  onChange={(e, val) => handleChange('status', val)}
                />
              </FormGroup>
            </div>

            {/* Coluna 2: Upload e preview */}
            <div style={{ flex: 1, textAlign: 'center' }}>
              <FormGroup label="Upload Imagem" fieldId="imagem">
                <FileUpload
                  id="imagem"
                  value={imageFile ? imageFile.name : ''}
                  filename={imageFile?.name}
                  onChange={() => { }}
                  onFileInputChange={handleFileChange}
                  onClearClick={handleClearImage}
                  hideDefaultPreview
                  browseButtonText='Selecione...'
                  filenamePlaceholder='Arraste e solte ou clique para selecionar uma imagem'
                  clearButtonText='Limpar Formulário'

                />
              </FormGroup>

              {imagePreview && (
                <div
                  style={{
                    marginTop: '1rem',
                    textAlign: 'center',
                    cursor: zoomed ? 'zoom-out' : 'zoom-in',
                  }}
                  onClick={() => setZoomed(!zoomed)}
                >
                  {(() => {
                    const fileName = imageFile?.name?.toLowerCase() || '';
                    const isImage = /\.(jpg|jpeg|png|gif|bmp|webp)$/i.test(fileName);
                    const isPDF = /\.pdf$/i.test(fileName);
                    const isXML = /\.xml$/i.test(fileName);

                    if (isImage) {
                      return (
                        <>
                          <img
                            src={imagePreview}
                            alt="Pré-visualização"
                            style={{
                              maxWidth: zoomed ? '90%' : '200px',
                              maxHeight: zoomed ? '90vh' : '200px',
                              border: '1px solid #ccc',
                              transition: 'transform 0.2s',
                              transform: `rotate(${rotation}deg)`,
                              borderRadius: '8px',
                            }}
                          />
                          <div style={{ fontSize: '0.8rem', color: '#666' }}>
                            (Clique para {zoomed ? 'reduzir' : 'ampliar'})
                          </div>

                          <div style={{ marginTop: '10px', display: 'flex', gap: '10px', justifyContent: 'center' }}>
                            <Button
                              onClick={(e) => {
                                e.stopPropagation();
                                setRotation(rotation - 90);
                              }}
                              variant="secondary"
                            >
                              ↺ Girar esquerda
                            </Button>
                            <Button
                              onClick={(e) => {
                                e.stopPropagation();
                                setRotation(rotation + 90);
                              }}
                              variant="secondary"
                            >
                              ↻ Girar direita
                            </Button>
                          </div>
                        </>
                      );
                    } else if (isPDF) {
                      return (
                        <>
                          <embed
                            src={imagePreview}
                            type="application/pdf"
                            width={zoomed ? '100%' : '680px'}
                            height={zoomed ? '100vh' : '400px'}
                            style={{
                              border: '1px solid #ccc',
                              borderRadius: '8px',
                              minHeight: '400px',
                              minWidth: '680px'
                            }}
                          />
                          <div style={{ fontSize: '0.8rem', color: '#666' }}>
                            (Clique para {zoomed ? 'reduzir' : 'ampliar'})
                          </div>
                        </>
                      );
                    } else if (isXML) {
                      return (
                        <>
                          <pre
                            style={{
                              textAlign: 'left',
                              backgroundColor: '#f8f8f8',
                              padding: '10px',
                              borderRadius: '8px',
                              maxWidth: '90%',
                              maxHeight: zoomed ? '90vh' : '400px',
                              overflow: 'auto',
                              whiteSpace: 'pre-wrap',
                              margin: 'auto',
                              border: '1px solid #ddd',
                            }}
                          >
                            {imagePreview}
                          </pre>
                          <div style={{ fontSize: '0.8rem', color: '#666' }}>
                            (Clique para {zoomed ? 'reduzir' : 'ampliar'})
                          </div>
                        </>
                      );
                    } else {
                      return (
                        <p style={{ color: '#888' }}>
                          Tipo de arquivo não suportado para visualização.
                        </p>
                      );
                    }
                  })()}
                </div>
              )}

            </div>
          </div>
        </Form>

      </ModalBody>

      <ModalFooter>
        <Button key="save" variant="primary" onClick={() => salvar()}>
          Salvar
        </Button>
        <Button
          key="cancel"
          variant="link"
          onClick={(event: React.MouseEvent<Element, MouseEvent> | KeyboardEvent) => {
            //setColumnData(initialColData);
            //handleModalToggle(event);
            resetForm();
            onClose();
          }}
        >
          Cancelar
        </Button>
      </ModalFooter>

      <AlertGroup hasAnimations isToast isLiveRegion>
        {alerts.map(({ key, variant, title }) => (
          <Alert
            variant={variant as AlertVariant || AlertVariant.info}
            title={title}
            actionClose={
              <AlertActionCloseButton
                title={title as string}
                variantLabel={`${variant} alert`}
                onClose={() => { if (key !== undefined && key !== null) removeAlert(key); }}
              />
            }
            key={key}
          />
        ))}
      </AlertGroup>

      {isLoading && <Block message={mensagem} />}

    </Modal>
  );
}
