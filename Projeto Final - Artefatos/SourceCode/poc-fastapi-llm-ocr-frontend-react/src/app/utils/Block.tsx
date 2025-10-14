import * as React from 'react';

export default function Block({ message = 'Carregando...' }) {
  return (
    <div className="blocker">
      <div className="loader">
      </div> 
      <div className="spinner-text">{message}</div>
    </div>
  );
}