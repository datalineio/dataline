import React from "react";
import { Field, FieldProps, Formik, Form } from "formik";
import * as yup from "yup";
import styled from "styled-components";

import LabeledInput from "../../../components/LabeledInput";
import { FormattedMessage, useIntl } from "react-intl";
import { ButtonContainer } from "./FormComponents";
import Button from "../../../components/Button";
import LabeledDropDown from "../../../components/LabeledDropDown";
import { IDataItem } from "../../../components/DropDown/components/ListItem";

type IProps = {
  dropDownData: Array<IDataItem>;
  onSubmit: () => void;
  formType: "source" | "destination";
};

const FormItem = styled.div`
  margin-bottom: 27px;
`;

const SmallLabeledDropDown = styled(LabeledDropDown)`
  max-width: 202px;
`;

const LinkToInstruction = styled.a`
  margin-left: 19px;
  font-weight: 500;
  font-size: 14px;
  line-height: 17px;
  text-decoration: underline;

  color: ${({ theme }) => theme.primaryColor};
`;

const onboardingValidationSchema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
  serviceType: yup.string().required("form.empty.error")
});

const OnboardingForm: React.FC<IProps> = ({
  onSubmit,
  formType,
  dropDownData
}) => {
  const instructionLink = (serviceId: string) => {
    const service = dropDownData.find(item => item.value === serviceId);

    return service ? (
      <LinkToInstruction href="https://dataline.io/" target="_blank">
        <FormattedMessage
          id="onboarding.instructionsLink"
          values={{ name: service.text }}
        />
      </LinkToInstruction>
    ) : null;
  };

  const formatMessage = useIntl().formatMessage;

  return (
    <Formik
      initialValues={{
        name: "",
        serviceType: ""
      }}
      validateOnBlur={true}
      validateOnChange={true}
      validationSchema={onboardingValidationSchema}
      onSubmit={async (_, { setSubmitting }) => {
        setSubmitting(false);
        onSubmit();
      }}
    >
      {({ isSubmitting, setFieldValue, isValid, dirty, values }) => (
        <Form>
          <FormItem>
            <Field name="name">
              {({ field }: FieldProps<string>) => (
                <LabeledInput
                  {...field}
                  label={<FormattedMessage id="form.name" />}
                  placeholder={formatMessage({
                    id: `form.${formType}Name.placeholder`
                  })}
                  type="text"
                  message={formatMessage({
                    id: `form.${formType}Name.message`
                  })}
                />
              )}
            </Field>
          </FormItem>

          <FormItem>
            <Field name="serviceType">
              {({ field }: FieldProps<string>) => (
                <SmallLabeledDropDown
                  {...field}
                  label={formatMessage({
                    id: `form.${formType}Type`
                  })}
                  hasFilter
                  placeholder={formatMessage({
                    id: "form.selectConnector"
                  })}
                  filterPlaceholder={formatMessage({
                    id: "form.searchName"
                  })}
                  data={dropDownData}
                  onSelect={item => setFieldValue("serviceType", item.value)}
                />
              )}
            </Field>
            {values.serviceType && instructionLink(values.serviceType)}
          </FormItem>

          <ButtonContainer>
            <Button type="submit" disabled={isSubmitting || !isValid || !dirty}>
              <FormattedMessage id={`onboarding.${formType}SetUp.buttonText`} />
            </Button>
          </ButtonContainer>
        </Form>
      )}
    </Formik>
  );
};

export default OnboardingForm;
