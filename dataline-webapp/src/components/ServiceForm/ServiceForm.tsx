import React from "react";
import { Formik, Form } from "formik";
import * as yup from "yup";
import styled from "styled-components";

import { IDataItem } from "../DropDown/components/ListItem";
import FormContent from "./components/FormContent";
import BottomBlock from "./components/BottomBlock";
import EditControls from "./components/EditControls";

type IProps = {
  dropDownData: Array<IDataItem>;
  onDropDownSelect?: (id: string) => void;
  onSubmit: (values: {
    name: string;
    serviceType: string;
    frequency?: string;
  }) => void;
  formType: "source" | "destination" | "connection";
  formValues?: { name: string; serviceType: string; frequency?: string };
  hasSuccess?: boolean;
  errorMessage?: React.ReactNode;
};

const FormContainer = styled(Form)`
  padding: 22px 27px 23px 24px;
`;

const onboardingValidationSchema = yup.object().shape({
  name: yup.string().required("form.empty.error"),
  serviceType: yup.string().required("form.empty.error")
});

const ServiceForm: React.FC<IProps> = ({
  onSubmit,
  formType,
  dropDownData,
  formValues,
  onDropDownSelect,
  hasSuccess,
  errorMessage
}) => {
  const isEditMode = !!formValues;
  return (
    <Formik
      initialValues={{
        name: formValues?.name || "",
        serviceType: formValues?.serviceType || "",
        frequency: formValues?.frequency || ""
      }}
      validateOnBlur={true}
      validateOnChange={true}
      validationSchema={onboardingValidationSchema}
      onSubmit={async (values, { setSubmitting }) => {
        await onSubmit(values);
        setSubmitting(false);
      }}
    >
      {({ isSubmitting, setFieldValue, isValid, dirty, values, resetForm }) => (
        <FormContainer>
          <FormContent
            dropDownData={dropDownData}
            formType={formType}
            setFieldValue={setFieldValue}
            values={values}
            isEditMode={isEditMode}
            onDropDownSelect={onDropDownSelect}
          />

          {isEditMode ? (
            <EditControls
              isSubmitting={isSubmitting}
              isValid={isValid}
              dirty={dirty}
              resetForm={resetForm}
            />
          ) : (
            <BottomBlock
              isSubmitting={isSubmitting}
              isValid={isValid}
              dirty={dirty}
              formType={formType}
              hasSuccess={hasSuccess}
              errorMessage={errorMessage}
            />
          )}
        </FormContainer>
      )}
    </Formik>
  );
};

export default ServiceForm;
