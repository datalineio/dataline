import React, { useCallback, useState } from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faRedoAlt } from "@fortawesome/free-solid-svg-icons";

import { Button } from "components";
import ContentCard from "components/ContentCard";
import useConnection, {
  useConnectionLoad,
  ValuesProps,
} from "components/hooks/services/useConnectionHook";
import DeleteBlock from "components/DeleteBlock";
import ConnectionForm from "views/Connection/ConnectionForm";
import ResetDataModal from "components/ResetDataModal";
import { ModalTypes } from "components/ResetDataModal/types";
import LoadingSchema from "components/LoadingSchema";
import { DestinationDefinition } from "core/resources/DestinationDefinition";
import { SourceDefinition } from "core/resources/SourceDefinition";

import { equal } from "utils/objects";
import EnabledControl from "./EnabledControl";
import { ConnectionNamespaceDefinition } from "core/domain/connection";
import { useAsyncFn } from "react-use";

type IProps = {
  onAfterSaveSchema: () => void;
  connectionId: string;
  frequencyText?: string;
  destinationDefinition?: DestinationDefinition;
  sourceDefinition?: SourceDefinition;
};

const Content = styled.div`
  max-width: 1279px;
  overflow-x: hidden;
  margin: 18px auto;
`;

const TitleContainer = styled.div<{ hasButton: boolean }>`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  margin: ${({ hasButton }) => (hasButton ? "-5px 0" : 0)};
`;

const TryArrow = styled(FontAwesomeIcon)`
  margin: 0 10px -1px 0;
  font-size: 14px;
`;

const Title = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const Message = styled.div`
  margin: -5px 0 13px;
  font-weight: 500;
  font-size: 12px;
  line-height: 15px;
  color: ${({ theme }) => theme.greyColor40};
`;

const Note = styled.span`
  color: ${({ theme }) => theme.dangerColor};
`;

const SettingsView: React.FC<IProps> = ({
  onAfterSaveSchema,
  connectionId,
  frequencyText,
  destinationDefinition,
  sourceDefinition,
}) => {
  const [isModalOpen, setIsUpdateModalOpen] = useState(false);
  const [activeUpdatingSchemaMode, setActiveUpdatingSchemaMode] = useState(
    false
  );
  const [saved, setSaved] = useState(false);
  const [currentValues, setCurrentValues] = useState<ValuesProps>({
    namespaceDefinition: ConnectionNamespaceDefinition.Source,
    namespaceFormat: "",
    schedule: null,
    prefix: "",
    syncCatalog: { streams: [] },
  });

  const {
    updateConnection,
    deleteConnection,
    resetConnection,
  } = useConnection();

  const onDelete = useCallback(() => deleteConnection({ connectionId }), [
    deleteConnection,
    connectionId,
  ]);

  const onReset = useCallback(() => resetConnection(connectionId), [
    resetConnection,
    connectionId,
  ]);

  const {
    connection: initialConnection,
    refreshConnectionCatalog,
  } = useConnectionLoad(connectionId);

  const [
    { value: connectionWithRefreshCatalog, loading: isRefreshingCatalog },
    refreshCatalog,
  ] = useAsyncFn(refreshConnectionCatalog, [connectionId]);

  const connection = activeUpdatingSchemaMode
    ? connectionWithRefreshCatalog
    : initialConnection;

  const onSubmit = async (values: ValuesProps) => {
    const initialSyncSchema = connection?.syncCatalog;

    await updateConnection({
      ...values,
      connectionId,
      status: initialConnection.status || "",
      withRefreshedCatalog: activeUpdatingSchemaMode,
    });

    setSaved(true);
    if (!equal(values.syncCatalog, initialSyncSchema)) {
      onAfterSaveSchema();
    }

    if (activeUpdatingSchemaMode) {
      setActiveUpdatingSchemaMode(false);
    }
  };

  const onSubmitResetModal = async () => {
    setIsUpdateModalOpen(false);
    await onSubmit(currentValues);
  };

  const onSubmitForm = async (values: ValuesProps) => {
    if (activeUpdatingSchemaMode) {
      setCurrentValues(values);
      setIsUpdateModalOpen(true);
    } else {
      await onSubmit(values);
    }
  };

  const onEnterRefreshCatalogMode = async () => {
    setActiveUpdatingSchemaMode(true);
    await refreshCatalog();
  };

  const onExitRefreshCatalogMode = () => {
    setActiveUpdatingSchemaMode(false);
  };

  const renderUpdateSchemaButton = () => {
    if (!activeUpdatingSchemaMode) {
      return (
        <Button onClick={onEnterRefreshCatalogMode} type="button">
          <TryArrow icon={faRedoAlt} />
          <FormattedMessage id="connection.updateSchema" />
        </Button>
      );
    }
    return (
      <Message>
        <FormattedMessage id="form.toSaveSchema" />{" "}
        <Note>
          <FormattedMessage id="form.noteStartSync" />
        </Note>
      </Message>
    );
  };

  return (
    <Content>
      <ContentCard
        title={
          <Title>
            <TitleContainer hasButton={!activeUpdatingSchemaMode}>
              <FormattedMessage id="connection.connectionSettings" />{" "}
            </TitleContainer>
            <EnabledControl
              disabled={isRefreshingCatalog}
              connection={initialConnection}
              frequencyText={frequencyText}
            />
          </Title>
        }
      >
        {!isRefreshingCatalog && connection ? (
          <ConnectionForm
            isEditMode
            connection={connection}
            onSubmit={onSubmitForm}
            onReset={onReset}
            successMessage={
              saved && <FormattedMessage id="form.changesSaved" />
            }
            onCancel={onExitRefreshCatalogMode}
            editSchemeMode={activeUpdatingSchemaMode}
            additionalSchemaControl={renderUpdateSchemaButton()}
            destinationIcon={destinationDefinition?.icon}
            sourceIcon={sourceDefinition?.icon}
          />
        ) : (
          <LoadingSchema />
        )}
      </ContentCard>
      <DeleteBlock type="connection" onDelete={onDelete} />
      {isModalOpen ? (
        <ResetDataModal
          onClose={() => setIsUpdateModalOpen(false)}
          onSubmit={onSubmitResetModal}
          modalType={ModalTypes.UPDATE_SCHEMA}
        />
      ) : null}
    </Content>
  );
};

export default SettingsView;
