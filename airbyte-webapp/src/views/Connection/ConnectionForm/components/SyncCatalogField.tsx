import React, { useCallback, useMemo, useState } from "react";
import { FieldProps, setIn } from "formik";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import type { DestinationSyncMode } from "core/domain/catalog";
import { SyncSchemaStream } from "core/domain/catalog";

import { CheckBox, LabeledRadioButton } from "components";
import { Header, LightCell } from "components/SimpleTableComponents";
import CatalogTree from "./CatalogTree";
import Search from "./Search";
import SectionTitle from "./SectionTitle";
import { naturalComparatorBy } from "utils/objects";
import { SyncCatalogFilters } from "./SyncCatalogFilters";

const TreeViewContainer = styled.div`
  width: 100%;
  background: ${({ theme }) => theme.greyColor0};
  margin-bottom: 29px;
  border-radius: 4px;
  max-height: 600px;
  overflow-y: overlay;
`;

const SchemaHeader = styled(Header)`
  min-height: 28px;
  margin-bottom: 5px;
`;

const SchemaTitle = styled(SectionTitle)`
  display: inline-block;
  margin: 0 11px 13px 0;
`;

const SelectAll = styled.div`
  margin: 0 9px 0 30px;
`;

const NamespaceTitleCell = styled(LightCell)`
  display: flex;
  flex-direction: row;
  align-items: center;
`;

const SearchContent = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
`;

const RadioButtonControl = styled(LabeledRadioButton)`
  margin: 0 0 0 5px;
`;

type SchemaViewProps = {
  additionalControl?: React.ReactNode;
  destinationSupportedSyncModes: DestinationSyncMode[];
} & FieldProps<SyncSchemaStream[]>;

const SyncCatalogField: React.FC<SchemaViewProps> = ({
  destinationSupportedSyncModes,
  additionalControl,
  field,
  form,
}) => {
  const { value: streams, name: fieldName } = field;

  const [searchString, setSearchString] = useState("");
  const [filterMode, setFilterMode] = useState(SyncCatalogFilters.All);

  const setField = form.setFieldValue;

  const onChangeSchema = useCallback(
    (newValue: SyncSchemaStream[]) => setField(fieldName, newValue),
    [fieldName, setField]
  );

  const onChangeStream = useCallback(
    (newValue: SyncSchemaStream) =>
      onChangeSchema(
        streams.map((str) => (str.id === newValue.id ? newValue : str))
      ),
    [streams, onChangeSchema]
  );

  const sortedSchema = useMemo(
    () =>
      streams.sort(naturalComparatorBy((syncStream) => syncStream.stream.name)),
    [streams]
  );

  const filteredStreams = useMemo(() => {
    const filters: Array<(s: SyncSchemaStream) => boolean> = [
      (_: SyncSchemaStream) => true,
      searchString
        ? (stream: SyncSchemaStream) =>
            stream.stream.name
              .toLowerCase()
              .includes(searchString.toLowerCase())
        : null,
      filterMode !== SyncCatalogFilters.All
        ? (stream: SyncSchemaStream) =>
            (filterMode === SyncCatalogFilters.Selected &&
              stream.config.selected) ||
            (filterMode === SyncCatalogFilters.NotSelected &&
              !stream.config.selected)
        : null,
    ].filter(Boolean) as Array<(s: SyncSchemaStream) => boolean>;

    return sortedSchema.filter((stream) => filters.every((f) => f(stream)));
  }, [searchString, filterMode, sortedSchema]);

  const hasSelectedItem = useMemo(
    () => filteredStreams.some((streamNode) => streamNode.config.selected),
    [filteredStreams]
  );

  const onCheckAll = useCallback(() => {
    const allSelectedValues = !hasSelectedItem;

    const newSchema = filteredStreams.map((streamNode) =>
      setIn(streamNode, "config.selected", allSelectedValues)
    );

    onChangeSchema(newSchema);
  }, [hasSelectedItem, onChangeSchema, filteredStreams]);

  return (
    <>
      <div>
        <SchemaTitle>
          <FormattedMessage id="form.dataSync" />
        </SchemaTitle>
        {additionalControl}
      </div>
      <SearchContent>
        <Search onSearch={setSearchString} />
        <RadioButtonControl
          onChange={(value) => {
            setFilterMode(value.target.value as SyncCatalogFilters);
          }}
          name="FilterAll"
          value={SyncCatalogFilters.All}
          checked={filterMode === SyncCatalogFilters.All}
          label={<FormattedMessage id="form.all" />}
        />
        <RadioButtonControl
          onChange={(value) => {
            setFilterMode(value.target.value as SyncCatalogFilters);
          }}
          name="FilterSelected"
          value={SyncCatalogFilters.Selected}
          checked={filterMode === SyncCatalogFilters.Selected}
          label={<FormattedMessage id="form.selected" />}
        />
        <RadioButtonControl
          onChange={(value) => {
            setFilterMode(value.target.value as SyncCatalogFilters);
          }}
          name="FilterNotSelected"
          value={SyncCatalogFilters.NotSelected}
          checked={filterMode === SyncCatalogFilters.NotSelected}
          label={<FormattedMessage id="form.notSelected" />}
        />
      </SearchContent>
      <SchemaHeader>
        <NamespaceTitleCell flex={1.5}>
          <SelectAll>
            <CheckBox onChange={onCheckAll} checked={hasSelectedItem} />
          </SelectAll>
          <FormattedMessage id="form.sourceNamespace" />
        </NamespaceTitleCell>
        <LightCell>
          <FormattedMessage id="form.sourceStreamName" />
        </LightCell>
        <LightCell>
          <FormattedMessage id="form.destinationNamespace" />
        </LightCell>
        <LightCell>
          <FormattedMessage id="form.destinationStreamName" />
        </LightCell>
        <LightCell flex={1.5}>
          <FormattedMessage id="form.syncMode" />
        </LightCell>
        <LightCell>
          <FormattedMessage id="form.primaryKey" />
        </LightCell>
        <LightCell>
          <FormattedMessage id="form.cursorField" />
        </LightCell>
      </SchemaHeader>
      <TreeViewContainer>
        <CatalogTree
          streams={filteredStreams}
          onChangeStream={onChangeStream}
          destinationSupportedSyncModes={destinationSupportedSyncModes}
        />
      </TreeViewContainer>
    </>
  );
};

export default React.memo(SyncCatalogField);
