import React from "react";
import { LightCell } from "components/SimpleTableComponents";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

const Name = styled.div<{ depth?: number }>`
  padding-left: ${({ depth }) => (depth ? depth * 59 : 0)}px;
`;

const FieldHeader: React.FC<{ depth?: number }> = (props) => {
  return (
    <>
      <LightCell flex={1.5}>
        <Name depth={props.depth}>
          <FormattedMessage id="form.field.name" />
        </Name>
      </LightCell>
      <LightCell />
      <LightCell>
        <FormattedMessage id="form.field.dataType" />
      </LightCell>
      <LightCell>
        <FormattedMessage id="form.field.destinationName" />
      </LightCell>
      <LightCell flex={1.5} />
      <LightCell>
        <FormattedMessage id="form.field.primaryKey" />
      </LightCell>
      <LightCell>
        <FormattedMessage id="form.field.cursorField" />
      </LightCell>
    </>
  );
};
export { FieldHeader };
