// ConfirmDialog.tsx
import React, { useState } from 'react';
import {
  Button,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  ModalVariant
} from '@patternfly/react-core';

interface ConfirmDialogProps {
  buttonTitle: string;
  title?: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
  onConfirm: () => void;
  onCancel?: () => void;
  trigger?: React.ReactNode;
}

export const ConfirmDialog: React.FC<ConfirmDialogProps> = ({
  buttonTitle='Abrir Confirmação',
  title = 'Confirmação',
  message,
  confirmText = 'Confirmar',
  cancelText = 'Cancelar',
  onConfirm,
  onCancel,
  trigger,
}) => {
  const [isOpen, setIsOpen] = useState(false);

  const handleCancel = () => {
    onCancel?.();
    setIsOpen(false);
  };

  const handleConfirm = () => {
    onConfirm();
    setIsOpen(false);
  };

  return (
    <>

     <span onClick={() => setIsOpen(true)} style={{ display: 'inline-block' }}>
        {trigger ?? (
          <Button variant="secondary" ouiaId="Secondary">{buttonTitle}</Button> 
        )}
      </span>
   
      <Modal
        variant="small"
        isOpen={isOpen}
        onClose={handleCancel}
        ouiaId="BasicModal"
        aria-labelledby="basic-modal-title"
        aria-describedby="modal-box-body-basic"
      >
        <ModalHeader title={title} labelId="basic-modal-title" />
        <ModalBody id="modal-box-body-basic">
          {message}
        </ModalBody>
        <ModalFooter>
          <Button key="confirm" variant="primary" onClick={handleConfirm}>
            {confirmText}
          </Button>
          <Button key="cancel" variant="link" onClick={handleCancel}>
            {cancelText}
          </Button>
        </ModalFooter>
      </Modal>
    </>
  );
};
