import React, { useCallback } from "react";
import { FastFieldProps, Field, setIn } from "formik";

import {
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  SyncSchemaStream,
} from "core/domain/catalog";
import { CatalogSection } from "./CatalogSection";

type IProps = {
  streams: SyncSchemaStream[];
  destinationSupportedSyncModes: DestinationSyncMode[];
  onChangeStream: (stream: SyncSchemaStream) => void;
};

const CatalogTree: React.FC<IProps> = ({
  streams,
  destinationSupportedSyncModes,
  onChangeStream,
}) => {
  const onUpdateStream = useCallback(
    (id: string, newStream: Partial<AirbyteStreamConfiguration>) => {
      const streamNode = streams.find((streamNode) => streamNode.id === id);

      if (streamNode) {
        const newStreamNode = setIn(
          streamNode,
          "config",
          Object.assign({}, streamNode.config, newStream)
        );

        onChangeStream(newStreamNode);
      }
    },
    [streams, onChangeStream]
  );

  return (
    <>
      {streams.map((streamNode) => (
        <Field
          key={`schema.streams[${streamNode.id}].config`}
          name={`schema.streams[${streamNode.id}].config`}
        >
          {({ form }: FastFieldProps) => (
            <CatalogSection
              key={`schema.streams[${streamNode.id}].config`}
              errors={form.errors}
              streamNode={streamNode}
              destinationSupportedSyncModes={destinationSupportedSyncModes}
              updateStream={onUpdateStream}
            />
          )}
        </Field>
      ))}
    </>
  );
};

export default CatalogTree;
