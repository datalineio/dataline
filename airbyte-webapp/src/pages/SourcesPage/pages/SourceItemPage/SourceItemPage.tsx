import React, { Suspense, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";
import { useResource } from "rest-hooks";

import config from "config";
import { Routes } from "pages/routes";

import { DropDownRow, ImageBlock } from "components";
import PageTitle from "components/PageTitle";
import useRouter from "components/hooks/useRouterHook";
import ContentCard from "components/ContentCard";
import EmptyResource from "components/EmptyResourceBlock";
import Breadcrumbs from "components/Breadcrumbs";
import {
  ItemTabs,
  StepsTypes,
  TableItemTitle,
} from "components/ConnectorBlocks";
import LoadingPage from "components/LoadingPage";
import MainPageWithScroll from "components/MainPageWithScroll";

import SourceConnectionTable from "./components/SourceConnectionTable";
import SourceSettings from "./components/SourceSettings";

import ConnectionResource from "core/resources/Connection";
import SourceResource from "core/resources/Source";

import DestinationResource from "core/resources/Destination";
import SourceDefinitionResource from "core/resources/SourceDefinition";
import DestinationsDefinitionResource from "core/resources/DestinationDefinition";
import { getIcon } from "utils/imageUtils";
import HeadTitle from "components/HeadTitle";

const Content = styled(ContentCard)`
  margin: 0 32px 0 27px;
`;

const SourceItemPage: React.FC = () => {
  const { query, push } = useRouter<{ id: string }>();

  const [currentStep, setCurrentStep] = useState<string>(StepsTypes.OVERVIEW);
  const onSelectStep = (id: string) => setCurrentStep(id);

  const { destinations } = useResource(DestinationResource.listShape(), {
    workspaceId: config.ui.workspaceId,
  });

  const { destinationDefinitions } = useResource(
    DestinationsDefinitionResource.listShape(),
    {
      workspaceId: config.ui.workspaceId,
    }
  );

  const source = useResource(SourceResource.detailShape(), {
    sourceId: query.id,
  });

  const sourceDefinition = useResource(SourceDefinitionResource.detailShape(), {
    sourceDefinitionId: source.sourceDefinitionId,
  });

  const { connections } = useResource(ConnectionResource.listShape(), {
    workspaceId: config.ui.workspaceId,
  });

  const onClickBack = () => push(Routes.Source);

  const breadcrumbsData = [
    {
      name: <FormattedMessage id="sidebar.sources" />,
      onClick: onClickBack,
    },
    { name: source.name },
  ];

  const connectionsWithSource = connections.filter(
    (connectionItem) => connectionItem.sourceId === source.sourceId
  );

  const destinationsDropDownData = useMemo(
    () =>
      destinations.map((item) => {
        const destinationDef = destinationDefinitions.find(
          (dd) => dd.destinationDefinitionId === item.destinationDefinitionId
        );
        return {
          label: item.name,
          value: item.destinationId,
          img: <ImageBlock img={destinationDef?.icon} />,
        };
      }),
    [destinations, destinationDefinitions]
  );

  const onSelect = (data: DropDownRow.IDataItem) => {
    if (data.value === "create-new-item") {
      push({
        pathname: `${Routes.Source}${Routes.ConnectionNew}`,
        state: { sourceId: source.sourceId },
      });
    } else {
      push({
        pathname: `${Routes.Source}${Routes.ConnectionNew}`,
        state: { destinationId: data.value, sourceId: source.sourceId },
      });
    }
  };

  const renderContent = () => {
    if (currentStep === StepsTypes.SETTINGS) {
      return (
        <SourceSettings
          currentSource={source}
          connectionsWithSource={connectionsWithSource}
        />
      );
    }

    return (
      <>
        <TableItemTitle
          type="destination"
          dropDownData={destinationsDropDownData}
          onSelect={onSelect}
          entity={source.sourceName}
          entityName={source.name}
          entityIcon={sourceDefinition ? getIcon(sourceDefinition.icon) : null}
        />
        {connectionsWithSource.length ? (
          <SourceConnectionTable connections={connectionsWithSource} />
        ) : (
          <Content>
            <EmptyResource
              text={<FormattedMessage id="sources.noDestinations" />}
              description={
                <FormattedMessage id="sources.addDestinationReplicateData" />
              }
            />
          </Content>
        )}
      </>
    );
  };

  return (
    <MainPageWithScroll
      headTitle={
        <HeadTitle titles={[{ id: "admin.sources" }, { title: source.name }]} />
      }
      pageTitle={
        <PageTitle
          title={<Breadcrumbs data={breadcrumbsData} />}
          middleComponent={
            <ItemTabs currentStep={currentStep} setCurrentStep={onSelectStep} />
          }
          withLine
        />
      }
    >
      <Suspense fallback={<LoadingPage />}>{renderContent()}</Suspense>
    </MainPageWithScroll>
  );
};

export default SourceItemPage;
