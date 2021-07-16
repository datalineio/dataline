import React from "react";
import styled from "styled-components";
import { FormattedMessage } from "react-intl";

import Label from "components/Label";
import LabeledToggle from "components/LabeledToggle";
import FeedbackBlock from "../../../components/FeedbackBlock";

export type NotificationsFormProps = {
  onChange: (data: { news: boolean; securityUpdates: boolean }) => void;
  preferencesValues: {
    news: boolean;
    securityUpdates: boolean;
  };
  successMessage?: React.ReactNode;
  errorMessage?: React.ReactNode;
  isLoading?: boolean;
};

const FormItem = styled.div`
  margin-bottom: 10px;
`;

const Subtitle = styled(Label)`
  padding-bottom: 9px;
`;

const NotificationsForm: React.FC<NotificationsFormProps> = ({
  onChange,
  preferencesValues,
  successMessage,
  errorMessage,
  isLoading,
}) => {
  return (
    <>
      <Subtitle>
        <FormattedMessage id="settings.emailNotifications" />
        <FeedbackBlock
          errorMessage={errorMessage}
          successMessage={successMessage}
          isLoading={isLoading}
        />
      </Subtitle>
      <FormItem>
        <LabeledToggle
          name="securityUpdates"
          checked={preferencesValues.securityUpdates}
          disabled={isLoading}
          label={<FormattedMessage id="settings.securityUpdates" />}
          onChange={(event) => {
            onChange({
              securityUpdates: event.target.checked,
              news: preferencesValues.news,
            });
          }}
        />
      </FormItem>

      <FormItem>
        <LabeledToggle
          name="newsletter"
          checked={preferencesValues.news}
          disabled={isLoading}
          label={<FormattedMessage id="settings.newsletter" />}
          onChange={(event) => {
            onChange({
              news: event.target.checked,
              securityUpdates: preferencesValues.securityUpdates,
            });
          }}
        />
      </FormItem>
    </>
  );
};

export default NotificationsForm;
