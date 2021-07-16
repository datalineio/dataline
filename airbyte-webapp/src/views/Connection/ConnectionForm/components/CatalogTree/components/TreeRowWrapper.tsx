import React from "react";
import styled from "styled-components";
import { Row } from "components/SimpleTableComponents";

const RowWrapper = styled.div<{ depth?: number; noBorder?: boolean }>`
  height: 40px;
  border-bottom: ${({ theme, noBorder }) =>
    noBorder ? "none" : `1px solid ${theme.greyColor20}`};

  &:last-child {
    border: ${({ depth = 0 }) => depth > 0 && "none"};
  }
`;

const RowContent = styled(Row)`
  height: 100%;
  white-space: nowrap;
`;

const TreeRowWrapper: React.FC<{ depth?: number; noBorder?: boolean }> = ({
  depth,
  children,
  noBorder,
}) => (
  <RowWrapper depth={depth} noBorder={noBorder}>
    <RowContent>{children}</RowContent>
  </RowWrapper>
);

export { TreeRowWrapper };
