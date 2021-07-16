import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
// @ts-ignore
import LinesEllipsis from "react-lines-ellipsis";

import styled from "styled-components";

import { DropDownRow } from "components";
import { MainInfoCell } from "./components/MainInfoCell";
import { Cell, LightTextCell } from "components/SimpleTableComponents";
import { ExpandFieldCell } from "./components/ExpandFieldCell";
import { SyncSettingsCell } from "./components/SyncSettingsCell";
import {
  DestinationSyncMode,
  SyncMode,
  SyncSchemaStream,
} from "core/domain/catalog";

const EmptyField = styled.span`
  color: ${({ theme }) => theme.greyColor40};
`;

type SyncSchema = {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
};

interface StreamHeaderProps {
  stream: SyncSchemaStream;
  destName: string;
  destNamespace: string;
  availableSyncModes: {
    value: SyncSchema;
  }[];
  onSelectSyncMode: (selectedMode: DropDownRow.IDataItem | null) => void;
  onSelectStream: () => void;

  pkRequired?: boolean;
  cursorRequired?: boolean;

  isRowExpanded: boolean;
  hasFields: boolean;
  onExpand: () => void;
}

export const StreamHeader: React.FC<StreamHeaderProps> = ({
  stream,
  destName,
  destNamespace,
  onSelectSyncMode,
  onSelectStream,
  availableSyncModes,
  pkRequired,
  cursorRequired,
  isRowExpanded,
  hasFields,
  onExpand,
}) => {
  const { primaryKey, syncMode, destinationSyncMode } = stream.config;
  const pkKeyItems = primaryKey.map((k) => k.join("."));
  const syncSchema = useMemo(
    () => ({
      syncMode: syncMode,
      destinationSyncMode: destinationSyncMode,
    }),
    [syncMode, destinationSyncMode]
  );

  return (
    <>
      <MainInfoCell
        label={stream.stream.name}
        onCheckBoxClick={onSelectStream}
        onExpand={onExpand}
        isItemChecked={stream.config.selected}
        isItemHasChildren={hasFields}
        isItemOpen={isRowExpanded}
      />
      <Cell>
        {stream.stream.namespace || (
          <EmptyField>
            <FormattedMessage id="form.noNamespace" />
          </EmptyField>
        )}
      </Cell>
      <Cell>
        <LinesEllipsis
          // maxLine={2}
          text={destNamespace}
          ellipsis="..."
          trimRight
          basedOn="letters"
          style={{
            "word-break": "break-all",
            "overflow-wrap": "break-all",
            "word-wrap": "break-all",
            width: "50px",
          }}
        />
      </Cell>
      <LightTextCell>
        <LinesEllipsis
          maxLine={2}
          text={destName}
          basedOn="letters"
          style={{
            "word-break": "break-all",
            "overflow-wrap": "break-word",
            "word-wrap": "break-word",
            width: "50px",
          }}
        />
      </LightTextCell>
      <SyncSettingsCell
        value={syncSchema}
        options={availableSyncModes}
        onChange={onSelectSyncMode}
      />
      <Cell>
        {pkRequired && (
          <ExpandFieldCell
            onExpand={onExpand}
            isItemOpen={isRowExpanded}
            tooltipItems={pkKeyItems}
          >
            <FormattedMessage
              id="form.pkSelected"
              values={{ count: pkKeyItems.length, items: pkKeyItems }}
            />
          </ExpandFieldCell>
        )}
      </Cell>
      <Cell>
        {cursorRequired && (
          <ExpandFieldCell onExpand={onExpand} isItemOpen={isRowExpanded}>
            {stream.config.cursorField.join(".")}
          </ExpandFieldCell>
        )}
      </Cell>
    </>
  );
};
