import React from "react";
import styled from "styled-components";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";
import { getIn, useFormik } from "formik";

import { Button, ControlLabels, DropDown, Input } from "components";
import { Transformation } from "core/domain/connection/operation";
import { operationService } from "core/domain/connection/OperationService";
import { equal } from "utils/objects";
import { FormikErrors } from "formik/dist/types";

const Content = styled.div`
  display: flex;
  flex-direction: row;
`;

const Column = styled.div`
  flex: 1 0 0;

  &:first-child {
    margin-right: 18px;
  }
`;

const Label = styled(ControlLabels)`
  margin-bottom: 20px;
`;

const ButtonContainer = styled.div`
  display: flex;
  justify-content: flex-end;
`;

const SmallButton = styled(Button)`
  margin-left: 8px;
  padding: 6px 8px 7px;
`;

interface TransformationProps {
  transformation: Transformation;
  onCancel: () => void;
  onDone: (tr: Transformation) => void;
}

const validationSchema = yup.object({
  name: yup.string().required("form.empty.error"),
  operatorConfiguration: yup.object({
    dbt: yup.object({
      gitRepoUrl: yup.string().required("form.empty.error"),
      dockerImage: yup.string().required("form.empty.error"),
      dbtArguments: yup.string().required("form.empty.error"),
      gitRepoBranch: yup.string().nullable(),
    }),
  }),
});

function prepareLabelFields(
  errors: FormikErrors<Transformation>,
  name: string
): { error?: boolean; message?: React.ReactNode } {
  const error = getIn(errors, name);

  const fields: { error?: boolean; message?: React.ReactNode } = {};

  if (error) {
    fields.error = true;
    fields.message = <FormattedMessage id={error} />;
  }

  return fields;
}

// enum with only one value for the moment
const TransformationTypes = [{ value: "custom", text: "Custom DBT" }];

const TransformationForm: React.FC<TransformationProps> = ({
  transformation,
  onCancel,
  onDone,
}) => {
  const formatMessage = useIntl().formatMessage;

  const formik = useFormik({
    initialValues: transformation,
    validationSchema: validationSchema,
    onSubmit: async (values) => {
      await operationService.check(values);
      onDone(values);
    },
  });

  return (
    <>
      <Content>
        <Column>
          <Label
            {...prepareLabelFields(formik.errors, "name")}
            label={<FormattedMessage id="form.transformationName" />}
          >
            <Input {...formik.getFieldProps("name")} />
          </Label>

          <Label
            {...prepareLabelFields(
              formik.errors,
              "operatorConfiguration.dbt.dockerImage"
            )}
            label={<FormattedMessage id="form.dockerUrl" />}
          >
            <Input
              {...formik.getFieldProps("operatorConfiguration.dbt.dockerImage")}
            />
          </Label>
          <Label
            {...prepareLabelFields(
              formik.errors,
              "operatorConfiguration.dbt.gitRepoUrl"
            )}
            label={<FormattedMessage id="form.repositoryUrl" />}
          >
            <Input
              {...formik.getFieldProps("operatorConfiguration.dbt.gitRepoUrl")}
              placeholder={formatMessage({
                id: "form.repositoryUrl.placeholder",
              })}
            />
          </Label>
        </Column>

        <Column>
          <Label label={<FormattedMessage id="form.transformationType" />}>
            <DropDown
              options={TransformationTypes}
              value="custom"
              placeholder={formatMessage({ id: "form.selectType" })}
            />
          </Label>
          <Label
            label={<FormattedMessage id="form.entrypoint" />}
            {...prepareLabelFields(
              formik.errors,
              "operatorConfiguration.dbt.dbtArguments"
            )}
            message={
              <a
                href="https://docs.getdbt.com/reference/dbt-commands"
                target="_blanc"
              >
                <FormattedMessage id="form.entrypoint.docs" />
              </a>
            }
          >
            <Input
              {...formik.getFieldProps(
                "operatorConfiguration.dbt.dbtArguments"
              )}
            />
          </Label>
          <Label label={<FormattedMessage id="form.gitBranch" />}>
            <Input
              {...formik.getFieldProps(
                "operatorConfiguration.dbt.gitRepoBranch"
              )}
            />
          </Label>
        </Column>
      </Content>
      <ButtonContainer>
        <SmallButton onClick={onCancel} type="button" secondary>
          <FormattedMessage id="form.cancel" />
        </SmallButton>
        <SmallButton
          onClick={() => formik.handleSubmit()}
          type="button"
          data-testid="done-button"
          isLoading={formik.isSubmitting}
          disabled={!formik.dirty || equal(transformation, formik.values)}
        >
          <FormattedMessage id="form.saveTransformation" />
        </SmallButton>
      </ButtonContainer>
    </>
  );
};

export default TransformationForm;
