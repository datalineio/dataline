import React, { useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useField } from "formik";
import styled from "styled-components";

import List from "react-widgets/lib/List";

import { ControlLabels, DropDown, DropDownRow, ImageBlock } from "components";

import { FormBaseItem } from "core/form/types";
import { SourceDefinition } from "core/resources/SourceDefinition";
import { DestinationDefinition } from "core/resources/DestinationDefinition";
import { isSourceDefinition } from "core/domain/connector/source";

import Instruction from "./Instruction";

const BottomElement = styled.div`
  background: ${(props) => props.theme.greyColro0};
  padding: 6px 16px 8px;
  width: 100%;
  min-height: 34px;
  border-top: 1px solid ${(props) => props.theme.greyColor20};
`;

const Block = styled.div`
  cursor: pointer;
  color: ${({ theme }) => theme.textColor};

  &:hover {
    color: ${({ theme }) => theme.primaryColor};
  }
`;

const ConnectorList = React.forwardRef(
  ({ onClick, ...listProps }: { onClick: () => void }, ref) => (
    <>
      <List ref={ref} {...listProps} />
      <BottomElement>
        <Block onClick={onClick}>
          <FormattedMessage id="connector.requestConnectorBlock" />
        </Block>
      </BottomElement>
    </>
  )
);

const DropdownLabels = styled(ControlLabels)`
  max-width: 202px;
`;

const ConnectorServiceTypeControl: React.FC<{
  property: FormBaseItem;
  formType: "source" | "destination";
  availableServices: (SourceDefinition | DestinationDefinition)[];
  isEditMode?: boolean;
  documentationUrl?: string;
  allowChangeConnector?: boolean;
  onChangeServiceType?: (id: string) => void;
  onOpenRequestConnectorModal: () => void;
}> = ({
  property,
  formType,
  isEditMode,
  allowChangeConnector,
  onChangeServiceType,
  availableServices,
  documentationUrl,
  onOpenRequestConnectorModal,
}) => {
  const formatMessage = useIntl().formatMessage;
  const [field, fieldMeta, { setValue }] = useField(property.path);

  const sortedDropDownData = useMemo(
    () =>
      availableServices
        .map((item: SourceDefinition | DestinationDefinition) => ({
          label: item.name,
          value: isSourceDefinition(item)
            ? item.sourceDefinitionId
            : item.destinationDefinitionId,
          img: <ImageBlock img={item.icon} />,
        }))
        .sort(DropDownRow.defaultDataItemSort),
    [availableServices]
  );

  const selectedService = React.useMemo(
    () =>
      availableServices.find(
        (s) =>
          (isSourceDefinition(s)
            ? s.sourceDefinitionId
            : s.destinationDefinitionId) === field.value
      ),
    [field.value, availableServices]
  );

  const handleSelect = useCallback(
    (item: DropDownRow.IDataItem | null) => {
      if (item) {
        setValue(item.value);
        if (onChangeServiceType) {
          onChangeServiceType(item.value);
        }
      }
    },
    [setValue, onChangeServiceType]
  );

  return (
    <>
      <DropdownLabels
        label={formatMessage({
          id: `form.${formType}Type`,
        })}
      >
        <DropDown
          {...field}
          listComponent={ConnectorList}
          listProps={{ onClick: onOpenRequestConnectorModal }}
          error={!!fieldMeta.error && fieldMeta.touched}
          isDisabled={isEditMode && !allowChangeConnector}
          hasFilter
          placeholder={formatMessage({
            id: "form.selectConnector",
          })}
          filterPlaceholder={formatMessage({
            id: "form.searchName",
          })}
          options={sortedDropDownData}
          onChange={handleSelect}
        />
      </DropdownLabels>
      {selectedService && documentationUrl && (
        <Instruction
          selectedService={selectedService}
          documentationUrl={documentationUrl}
        />
      )}
    </>
  );
};

export { ConnectorServiceTypeControl };
