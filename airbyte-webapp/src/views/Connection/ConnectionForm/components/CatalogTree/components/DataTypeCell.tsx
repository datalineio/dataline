import React from "react";
import { Cell } from "components/SimpleTableComponents";
import styled from "styled-components";
import { FormattedMessage } from "react-intl";

type DataTypeCellProps = {
  nullable?: boolean;
};

const Description = styled.div`
  color: ${({ theme }) => theme.greyColor40};
  font-size: 11px;
`;

const DataTypeCell: React.FC<DataTypeCellProps> = ({ children, nullable }) => {
  return (
    <Cell>
      {children}
      {nullable && (
        <Description>
          <FormattedMessage id="form.nullable" />
        </Description>
      )}
    </Cell>
  );
};

export default DataTypeCell;
