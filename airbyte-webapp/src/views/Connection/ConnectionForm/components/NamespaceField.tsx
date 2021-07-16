import React, { useMemo } from "react";
import { Field, FieldProps } from "formik";
import { FormattedMessage, useIntl } from "react-intl";
import styled from "styled-components";

import config from "config";

import { ControlLabels, DropDown, Input } from "components";
import { ConnectionNamespaceDefinition } from "core/domain/connection";

const NamespaceConfigurationLabel = styled(ControlLabels)`
  flex: 3 0 0;
  max-width: 300px;
  & a {
    color: ${({ theme }) => theme.greyColor40};
  }
`;

const NamespaceFormatLabel = styled(ControlLabels)`
  margin-left: 21px;
  flex: 5 0 0;
`;

const Row = styled.div`
  display: flex;
  margin-bottom: 26px;
`;

const NamespaceField: React.FC = () => {
  const formatMessage = useIntl().formatMessage;

  const definitions = useMemo(
    () => [
      {
        value: ConnectionNamespaceDefinition.Source,
        label: <FormattedMessage id="connectionForm.sourceFormat" />,
      },
      {
        value: ConnectionNamespaceDefinition.Destination,
        label: <FormattedMessage id="connectionForm.destinationFormat" />,
      },
      {
        value: ConnectionNamespaceDefinition.CustomFormat,
        label: <FormattedMessage id="connectionForm.customFormat" />,
      },
    ],
    []
  );
  return (
    <Row>
      <Field name="namespaceDefinition">
        {({ field, form }: FieldProps<ConnectionNamespaceDefinition>) => (
          <>
            <NamespaceConfigurationLabel
              labelAdditionLength={0}
              label={
                <FormattedMessage id="connectionForm.namespaceDefinition.title" />
              }
              message={
                <FormattedMessage
                  id="connectionForm.namespaceDefinition.subtitle"
                  values={{
                    lnk: (...lnk: React.ReactNode[]) => (
                      <a target="_blank" href={config.ui.namespaceLink}>
                        {lnk}
                      </a>
                    ),
                  }}
                />
              }
            >
              <DropDown
                options={definitions}
                value={field.value}
                onChange={({ value }) => form.setFieldValue(field.name, value)}
              />
            </NamespaceConfigurationLabel>
            {field.value === ConnectionNamespaceDefinition.CustomFormat && (
              <Field name="namespaceFormat">
                {({ field, meta }: FieldProps<string>) => (
                  <NamespaceFormatLabel
                    nextLine
                    error={!!meta.error}
                    label={
                      <FormattedMessage id="connectionForm.namespaceFormat.title" />
                    }
                    message={
                      <FormattedMessage id="connectionForm.namespaceFormat.subtitle" />
                    }
                  >
                    <Input
                      {...field}
                      error={!!meta.error}
                      placeholder={formatMessage({
                        id: "connectionForm.namespaceFormat.placeholder",
                      })}
                    />
                  </NamespaceFormatLabel>
                )}
              </Field>
            )}
          </>
        )}
      </Field>
    </Row>
  );
};

export { NamespaceField };
