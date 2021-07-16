import React, { Suspense, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";
import { faRedoAlt } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useResource } from "rest-hooks";

import LoadingSchema from "components/LoadingSchema";
import ContentCard from "components/ContentCard";
import { JobsLogItem } from "components/JobItem";
import ConnectionForm from "views/Connection/ConnectionForm";
import { Button } from "components";
import TryAfterErrorBlock from "./components/TryAfterErrorBlock";

import { AnalyticsService } from "core/analytics/AnalyticsService";
import { Source } from "core/resources/Source";
import { Destination } from "core/resources/Destination";

import useConnection, {
  ValuesProps,
} from "components/hooks/services/useConnectionHook";
import { useDiscoverSchema } from "components/hooks/services/useSchemaHook";
import SourceDefinitionResource from "core/resources/SourceDefinition";
import DestinationDefinitionResource from "core/resources/DestinationDefinition";
import { IDataItem } from "../base/DropDown/components/Option";

const SkipButton = styled.div`
  margin-top: 6px;

  & > button {
    min-width: 239px;
    margin-left: 9px;
  }
`;

const TryArrow = styled(FontAwesomeIcon)`
  margin: 0 10px -1px 0;
  font-size: 14px;
`;

type IProps = {
  additionBottomControls?: React.ReactNode;
  source: Source;
  destination: Destination;
  afterSubmitConnection?: () => void;
};

const CreateConnectionContent: React.FC<IProps> = ({
  source,
  destination,
  afterSubmitConnection,
  additionBottomControls,
}) => {
  const { createConnection } = useConnection();

  const sourceDefinition = useResource(SourceDefinitionResource.detailShape(), {
    sourceDefinitionId: source.sourceDefinitionId,
  });

  const destinationDefinition = useResource(
    DestinationDefinitionResource.detailShape(),
    {
      destinationDefinitionId: destination.destinationDefinitionId,
    }
  );

  const {
    schema,
    isLoading,
    schemaErrorStatus,
    onDiscoverSchema,
  } = useDiscoverSchema(source?.sourceId);

  const connection = useMemo(
    () => ({
      syncCatalog: schema,
      destination,
      source,
    }),
    [schema, destination, source]
  );

  if (isLoading) {
    return (
      <ContentCard title={<FormattedMessage id="onboarding.setConnection" />}>
        <LoadingSchema />
      </ContentCard>
    );
  }

  if (schemaErrorStatus) {
    return (
      <ContentCard title={<FormattedMessage id="onboarding.setConnection" />}>
        <TryAfterErrorBlock
          onClick={onDiscoverSchema}
          additionControl={<SkipButton>{additionBottomControls}</SkipButton>}
        />
        <JobsLogItem jobInfo={schemaErrorStatus?.response} />
      </ContentCard>
    );
  }

  const onSubmitConnectionStep = async (values: ValuesProps) => {
    await createConnection({
      values,
      source: source,
      destination: destination,
      sourceDefinition: {
        name: source?.name ?? "",
        sourceDefinitionId: source?.sourceDefinitionId ?? "",
      },
      destinationDefinition: {
        name: destination?.name ?? "",
        destinationDefinitionId: destination?.destinationDefinitionId ?? "",
      },
    });

    if (afterSubmitConnection) {
      afterSubmitConnection();
    }
  };

  const onSelectFrequency = (item: IDataItem | null) => {
    AnalyticsService.track("New Connection - Action", {
      action: "Select a frequency",
      frequency: item?.label,
      connector_source_definition: source?.sourceName,
      connector_source_definition_id: source?.sourceDefinitionId,
      connector_destination_definition: destination?.destinationName,
      connector_destination_definition_id: destination?.destinationDefinitionId,
    });
  };

  return (
    <ContentCard title={<FormattedMessage id="onboarding.setConnection" />}>
      <Suspense fallback={<LoadingSchema />}>
        <ConnectionForm
          connection={connection}
          additionBottomControls={additionBottomControls}
          onDropDownSelect={onSelectFrequency}
          additionalSchemaControl={
            <Button onClick={onDiscoverSchema} type="button">
              <TryArrow icon={faRedoAlt} />
              <FormattedMessage id="connection.refreshSchema" />
            </Button>
          }
          onSubmit={onSubmitConnectionStep}
          sourceIcon={sourceDefinition?.icon}
          destinationIcon={destinationDefinition?.icon}
        />
      </Suspense>
    </ContentCard>
  );
};

export default CreateConnectionContent;
