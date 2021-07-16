import React from "react";
import { SyncSchemaField } from "core/domain/catalog";
import styled from "styled-components";

const RowsContainer = styled.div<{ depth?: number }>`
  background: ${({ theme }) => theme.whiteColor5};
  border-radius: 4px;
  margin: 0
    ${({ depth = 0 }) => `${depth * 38}px ${depth * 5}px ${depth * 38}px`};
`;

const Rows: React.FC<{
  fields: SyncSchemaField[];
  children: (field: SyncSchemaField) => React.ReactNode;
  depth?: number;
}> = (props) => (
  <RowsContainer depth={props.depth}>
    {props.fields.map((field) => (
      <React.Fragment key={field.key}>
        {props.children(field)}
        {field.fields && <Rows fields={field.fields}>{props.children}</Rows>}
      </React.Fragment>
    ))}
  </RowsContainer>
);

export { Rows };
