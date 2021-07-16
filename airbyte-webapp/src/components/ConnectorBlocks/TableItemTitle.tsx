import React from "react";
import styled from "styled-components";
import { FormattedMessage, useIntl } from "react-intl";

import { Button, DropDownRow, H3, H5 } from "components";
import { Popout } from "components/base/Popout/Popout";

type IProps = {
  type: "source" | "destination";
  dropDownData: DropDownRow.IDataItem[];
  onSelect: (item: DropDownRow.IDataItem) => void;
  entity: string;
  entityName: string;
  entityIcon?: React.ReactNode;
};

const Content = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  padding: 0 32px 18px 27px;
`;

const EntityType = styled(H5)`
  color: ${({ theme }) => theme.greyColor55};
`;

const EntityInfo = styled(Content)`
  justify-content: left;
  padding-top: 15px;
  padding-bottom: 39px;
`;

const EntityIcon = styled.div`
  margin-right: 15px;
  height: 40px;
  width: 40px;
`;

const TableItemTitle: React.FC<IProps> = ({
  type,
  dropDownData,
  onSelect,
  entity,
  entityName,
  entityIcon,
}) => {
  const formatMessage = useIntl().formatMessage;

  return (
    <>
      <EntityInfo>
        {entityIcon && <EntityIcon>{entityIcon}</EntityIcon>}
        <div>
          <H3 bold>{entityName}</H3>
          <EntityType>{entity}</EntityType>
        </div>
      </EntityInfo>
      <Content>
        <H5>
          <FormattedMessage id="tables.connections" />
        </H5>
        <Popout
          onChange={onSelect}
          targetComponent={({ onOpen }: any) => (
            <Button onClick={onOpen}>
              <FormattedMessage id={`tables.${type}Add`} />
            </Button>
          )}
          options={[
            {
              label: formatMessage({
                id: `tables.${type}AddNew`,
              }),
              value: "create-new-item",
              primary: true,
            },
            ...dropDownData,
          ]}
          hasFilter
        />
      </Content>
    </>
  );
};

export default TableItemTitle;
